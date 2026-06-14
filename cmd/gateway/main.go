package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"auv-sonar-gateway/internal/beam"
	"auv-sonar-gateway/internal/ins"
	redisx "auv-sonar-gateway/internal/redis"
	"auv-sonar-gateway/internal/model"
	"auv-sonar-gateway/internal/multicast"
	"auv-sonar-gateway/internal/pointcloud"
	"auv-sonar-gateway/internal/tcp"
	"auv-sonar-gateway/internal/voidfill"
	"auv-sonar-gateway/internal/kriging"
)

type GatewayConfig struct {
	SonarIface     string
	SonarGroup     string
	SonarUdpPort   int
	SonarTcpPort   int
	SonarBuffer    int
	SonarEnableTcp bool

	INSAddr        string
	INSPort        int

	RedisAddr      string
	RedisPassword  string
	RedisDB        int

	MaxCacheFrames int
	SSPProfile     *model.SoundSpeedProfile
	DebugLogging   bool
	MetricsInterval time.Duration

	VoidFillEnable       bool
	VoidFillCellSize     float64
	VoidFillEveryNFrames int
	VoidFillSearchRadius int
	VoidFillVariogramNugget    float64
	VoidFillVariogramSill      float64
	VoidFillVariogramRange     float64
	VoidFillAnisoAngle         float64
	VoidFillAnisoRatio         float64
	VoidFillMaxInterpolateTick int
}

type Gateway struct {
	config       GatewayConfig
	sonarRx      *multicast.SonarReceiver
	sonarTcp     *tcp.SonarTCPServer
	insReader    *ins.INSReader
	beamProc     *beam.Processor
	reorganizer  *pointcloud.Reorganizer
	redisHub     *redisx.Hub
	voidFiller   *voidfill.VoidFiller
	metricsDone  chan struct{}
}

func NewGateway(cfg GatewayConfig) *Gateway {
	return &Gateway{config: cfg}
}

func (g *Gateway) Init() error {
	opts := redisx.DefaultOptions()
	opts.Addr = g.config.RedisAddr
	opts.Password = g.config.RedisPassword
	opts.DB = g.config.RedisDB
	g.redisHub = redisx.NewHub(opts)

	if err := g.redisHub.Connect(); err != nil {
		log.Printf("warning: failed to connect to Redis: %v, proceeding without persistence", err)
	}

	insReader, err := ins.NewINSReader(g.config.INSAddr, g.config.INSPort)
	if err != nil {
		return fmt.Errorf("create INS reader: %w", err)
	}
	g.insReader = insReader
	g.insReader.SetDebug(g.config.DebugLogging)

	if g.config.SSPProfile == nil {
		g.config.SSPProfile = buildDefaultSSP()
	}

	sspFromRedis, err := g.redisHub.GetLatestSSP()
	if err != nil {
		log.Printf("warning: failed to fetch latest SSP from Redis: %v", err)
	}
	if sspFromRedis != nil {
		g.config.SSPProfile = sspFromRedis
		log.Printf("loaded SSP from Redis with %d entries", len(g.config.SSPProfile.Entries))
	}

	g.beamProc = beam.NewProcessor(g.insReader, g.config.SSPProfile)

	g.reorganizer = pointcloud.NewReorganizer(g.config.MaxCacheFrames)

	g.reorganizer.SetReadyHandler(func(frames []*model.PointCloudFrame) {
		for _, f := range frames {
			if err := g.redisHub.PublishFrame(f); err != nil {
				log.Printf("warning: failed to publish frame %d: %v", f.PingCounter, err)
			}
		}
	})

	if g.config.VoidFillEnable {
		fillerCfg := voidfill.DefaultFillerConfig()
		if g.config.VoidFillCellSize > 0 {
			fillerCfg.CellSize = g.config.VoidFillCellSize
		}
		if g.config.VoidFillSearchRadius > 0 {
			fillerCfg.SearchRadius = g.config.VoidFillSearchRadius
		}
		if g.config.VoidFillVariogramNugget > 0 {
			fillerCfg.VariogramNugget = g.config.VoidFillVariogramNugget
		}
		if g.config.VoidFillVariogramSill > 0 {
			fillerCfg.VariogramSill = g.config.VoidFillVariogramSill
		}
		if g.config.VoidFillVariogramRange > 0 {
			fillerCfg.VariogramRange = g.config.VoidFillVariogramRange
		}
		fillerCfg.VariogramAnisoAngle = g.config.VoidFillAnisoAngle
		fillerCfg.VariogramAnisoRatio = g.config.VoidFillAnisoRatio
		if g.config.VoidFillMaxInterpolateTick > 0 {
			fillerCfg.MaxInterpolatePerTick = g.config.VoidFillMaxInterpolateTick
		}

		g.voidFiller = voidfill.NewVoidFiller(fillerCfg)
		g.reorganizer.EnableVoidFill(g.voidFiller, g.config.VoidFillEveryNFrames)

		log.Printf("void fill enabled: cell=%.2fm, radius=%d, range=%.2fm, every=%d frames",
			fillerCfg.CellSize, fillerCfg.SearchRadius, fillerCfg.VariogramRange, g.config.VoidFillEveryNFrames)
	}

	g.beamProc.SetFrameHandler(func(frame *model.PointCloudFrame) {
		g.reorganizer.SubmitFrame(frame)
	})

	g.redisHub.SetSSPUpdateHandler(func(ssp *model.SoundSpeedProfile) {
		log.Printf("received SSP update via Redis pub/sub, %d entries", len(ssp.Entries))
		g.beamProc.UpdateProfile(ssp)
	})

	sonarRx, err := multicast.NewSonarReceiver(
		g.config.SonarIface,
		g.config.SonarGroup,
		g.config.SonarUdpPort,
		g.config.SonarBuffer,
	)
	if err != nil {
		return fmt.Errorf("create sonar receiver: %w", err)
	}
	g.sonarRx = sonarRx
	g.sonarRx.SetDebug(g.config.DebugLogging)

	g.sonarRx.SetPingHandler(func(ping *model.SonarPing) {
		if err := g.redisHub.PublishPing(ping); err != nil {
			log.Printf("warning: failed to publish ping: %v", err)
		}
		g.beamProc.ProcessPing(ping)
	})

	if g.config.SonarEnableTcp {
		tcpListenAddr := fmt.Sprintf(":%d", g.config.SonarTcpPort)
		g.sonarTcp = tcp.NewSonarTCPServer(tcpListenAddr, 8, g.config.SonarBuffer)
		g.sonarTcp.SetDebug(g.config.DebugLogging)
		g.sonarTcp.SetPingHandler(func(ping *model.SonarPing) {
			if err := g.redisHub.PublishPing(ping); err != nil {
				log.Printf("warning: failed to publish TCP ping: %v", err)
			}
			g.beamProc.ProcessPing(ping)
		})
	}

	g.insReader.SetDataHandler(func(insData *model.INSData) {
		if err := g.redisHub.PublishINS(insData); err != nil {
			log.Printf("warning: failed to publish INS: %v", err)
		}
	})

	if err := g.redisHub.PublishSSP(g.config.SSPProfile); err != nil {
		log.Printf("warning: failed to publish initial SSP: %v", err)
	}

	g.redisHub.PublishStatsPeriodically(10 * time.Second)

	if g.config.MetricsInterval <= 0 {
		g.config.MetricsInterval = 5 * time.Second
	}
	g.metricsDone = make(chan struct{})

	return nil
}

func (g *Gateway) Start() error {
	if err := g.insReader.Start(); err != nil {
		return fmt.Errorf("start INS reader: %w", err)
	}

	if err := g.sonarRx.Start(); err != nil {
		return fmt.Errorf("start sonar receiver: %w", err)
	}

	if g.sonarTcp != nil {
		if err := g.sonarTcp.Start(); err != nil {
			return fmt.Errorf("start sonar TCP server: %w", err)
		}
	}

	g.startMetricsLogger()

	log.Printf("=== AUV Sonar Gateway started ===")
	log.Printf("  UDP Sonar: %s:%d (%s)", g.config.SonarGroup, g.config.SonarUdpPort, g.config.SonarIface)
	if g.config.SonarEnableTcp {
		log.Printf("  TCP Sonar: :%d", g.config.SonarTcpPort)
	}
	log.Printf("  INS:   %s:%d", g.config.INSAddr, g.config.INSPort)
	log.Printf("  Redis: %s (DB=%d)", g.config.RedisAddr, g.config.RedisDB)
	log.Printf("  Frame cache: %d frames", g.config.MaxCacheFrames)
	log.Printf("  Sliding window: ENABLED (state machine driven)")
	if g.config.VoidFillEnable {
		log.Printf("  Void fill: ENABLED (Kriging, cell=%.2fm)", g.config.VoidFillCellSize)
	} else {
		log.Printf("  Void fill: DISABLED")
	}
	log.Printf("  Debug logging: %v", g.config.DebugLogging)
	log.Printf("  Metrics interval: %v", g.config.MetricsInterval)
	log.Printf("=================================")
	return nil
}

func (g *Gateway) Stop() {
	log.Printf("=== AUV Sonar Gateway shutting down ===")

	close(g.metricsDone)

	if g.sonarTcp != nil {
		g.sonarTcp.Stop()
	}
	g.sonarRx.Stop()
	g.insReader.Stop()

	g.reorganizer.Flush()

	g.redisHub.Disconnect()

	g.printFinalStats()

	log.Printf("=== AUV Sonar Gateway stopped ===")
}

func (g *Gateway) startMetricsLogger() {
	go func() {
		ticker := time.NewTicker(g.config.MetricsInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				g.logCurrentStats()
			case <-g.metricsDone:
				return
			}
		}
	}()
}

func (g *Gateway) logCurrentStats() {
	sonarStats := g.sonarRx.GetStats()
	insStats := g.insReader.Stats()
	beamProcessed, beamDropped := g.beamProc.GetStats()
	redisStats := g.redisHub.GetStats()

	log.Printf("--- Gateway Metrics ---")
	log.Printf("  UDP Sonar: pkts=%d, decode_err=%d, bytes=%d",
		sonarStats.PacketsReceived, sonarStats.DecodeErrors, sonarStats.BytesReceived)

	if sonarStats.AssemblerStats.BytesFed > 0 {
		as := sonarStats.AssemblerStats
		log.Printf("  UDP Assembler: sync=%d, header=%d, frames=%d, resync=%d, bad_len=%d, overrun=%d",
			as.SyncFound, as.HeaderParsed, as.FramesCompleted,
			as.ResyncEvents, as.InvalidLength, as.Overruns)
	}

	if g.sonarTcp != nil {
		ts := g.sonarTcp.Stats()
		log.Printf("  TCP Sonar: conns=%d, bytes=%d, frames=%d, decode_err=%d",
			ts.ConnectionsOpened-ts.ConnectionsClosed,
			ts.BytesReceived, ts.PacketsReceived, ts.DecodeErrors)
		if ts.AssemblerStats.BytesFed > 0 {
			as := ts.AssemblerStats
			log.Printf("  TCP Assembler: sync=%d, header=%d, frames=%d, resync=%d, bad_len=%d, overrun=%d",
				as.SyncFound, as.HeaderParsed, as.FramesCompleted,
				as.ResyncEvents, as.InvalidLength, as.Overruns)
		}
	}

	log.Printf("  INS: frames=%d, resync=%d, bad_ts=%d, overrun=%d",
		insStats.FramesCompleted, insStats.ResyncEvents, insStats.BadTimestamp, insStats.Overruns)
	log.Printf("  Beam: processed=%d, dropped=%d", beamProcessed, beamDropped)
	log.Printf("  Redis: frames=%d, points=%d, ssp=%d, ins=%d",
		redisStats.FramesPublished, redisStats.PointsPublished,
		redisStats.SSPUpdates, redisStats.InspDataUpdates)
	log.Printf("  PointCloud: cache=%d frames", g.reorganizer.Size())

	if g.voidFiller != nil {
		vs := g.voidFiller.GetStats()
		log.Printf("  Void Fill: grids=%d, void_detected=%d, filled=%d, skipped=%d, kriging=%d, idw=%d",
			vs.GridsProcessed, vs.VoidCellsDetected, vs.VoidCellsFilled, vs.VoidCellsSkipped,
			vs.KrigingInterpolations, vs.IDWFallbacks)
		if vs.KrigingInterpolations+vs.IDWFallbacks > 0 {
			log.Printf("  Void Fill Perf: avg=%.1fus, min=%.1fus, max=%.1fus",
				vs.AvgInterpolateUs, vs.MinInterpolateUs, vs.MaxInterpolateUs)
		}

		fillResults := g.reorganizer.GetFillResults()
		if len(fillResults) > 0 {
			last := fillResults[len(fillResults)-1]
			log.Printf("  Last Patch: id=%d, voids=%d, filled=%d, kriging=%d, idw=%d, elapsed=%dus",
				last.PatchID, last.VoidCellsInPatch, last.FilledCells,
				last.KrigingCount, last.IDWCount, last.ElapsedUs)
		}
	}
	log.Printf("---------------------")
}

func (g *Gateway) printFinalStats() {
	log.Printf("--- Final statistics ---")

	sonarStats := g.sonarRx.GetStats()
	insStats := g.insReader.Stats()
	beamProcessed, beamDropped := g.beamProc.GetStats()
	redisStats := g.redisHub.GetStats()

	log.Printf("UDP Sonar:")
	log.Printf("  Packets received:  %d", sonarStats.PacketsReceived)
	log.Printf("  Decode errors: %d", sonarStats.DecodeErrors)
	log.Printf("  Bytes received: %d", sonarStats.BytesReceived)
	if sonarStats.AssemblerStats.BytesFed > 0 {
		as := sonarStats.AssemblerStats
		log.Printf("  Assembler:")
		log.Printf("    Sync found:      %d", as.SyncFound)
		log.Printf("    Header parsed:   %d", as.HeaderParsed)
		log.Printf("    Frames done:   %d", as.FramesCompleted)
		log.Printf("    Resync events: %d", as.ResyncEvents)
		log.Printf("    Invalid lengths: %d", as.InvalidLength)
		log.Printf("    Buffer overruns: %d", as.Overruns)
	}

	if g.sonarTcp != nil {
		ts := g.sonarTcp.Stats()
		log.Printf("TCP Sonar:")
		log.Printf("  Connections opened:  %d", ts.ConnectionsOpened)
		log.Printf("  Connections closed: %d", ts.ConnectionsClosed)
		log.Printf("  Packets received:  %d", ts.PacketsReceived)
		log.Printf("  Decode errors: %d", ts.DecodeErrors)
		log.Printf("  Bytes received: %d", ts.BytesReceived)
		if ts.AssemblerStats.BytesFed > 0 {
			as := ts.AssemblerStats
			log.Printf("  Assembler:")
			log.Printf("    Sync found:      %d", as.SyncFound)
			log.Printf("    Header parsed:   %d", as.HeaderParsed)
			log.Printf("    Frames done:   %d", as.FramesCompleted)
			log.Printf("    Resync events: %d", as.ResyncEvents)
			log.Printf("    Invalid lengths: %d", as.InvalidLength)
			log.Printf("    Buffer overruns: %d", as.Overruns)
		}
	}

	log.Printf("INS Reader:")
	log.Printf("  Bytes fed:       %d", insStats.BytesFed)
	log.Printf("  Frames completed: %d", insStats.FramesCompleted)
	log.Printf("  Resync events:   %d", insStats.ResyncEvents)
	log.Printf("  Bad timestamps:  %d", insStats.BadTimestamp)
	log.Printf("  Buffer overruns:  %d", insStats.Overruns)

	log.Printf("Beam Processor:")
	log.Printf("  Processed: %d", beamProcessed)
	log.Printf("  Dropped:   %d", beamDropped)

	log.Printf("Redis Hub:")
	log.Printf("  Frames published: %d", redisStats.FramesPublished)
	log.Printf("  Points published: %d", redisStats.PointsPublished)
	log.Printf("  SSP updates:    %d", redisStats.SSPUpdates)
	log.Printf("  INS updates:    %d", redisStats.InspDataUpdates)

	if g.voidFiller != nil {
		vs := g.voidFiller.GetStats()
		log.Printf("Void Fill (Kriging):")
		log.Printf("  Grids processed:   %d", vs.GridsProcessed)
		log.Printf("  Void cells detected: %d", vs.VoidCellsDetected)
		log.Printf("  Void cells filled: %d", vs.VoidCellsFilled)
		log.Printf("  Void cells skipped: %d", vs.VoidCellsSkipped)
		log.Printf("  Kriging interpolations: %d", vs.KrigingInterpolations)
		log.Printf("  IDW fallbacks:      %d", vs.IDWFallbacks)
		if vs.KrigingInterpolations+vs.IDWFallbacks > 0 {
			log.Printf("  Interpolation Perf:")
			log.Printf("    Avg: %.1f us", vs.AvgInterpolateUs)
			log.Printf("    Min: %.1f us", vs.MinInterpolateUs)
			log.Printf("    Max: %.1f us", vs.MaxInterpolateUs)
		}

		vg := g.voidFiller.GetVariogram()
		log.Printf("  Variogram:")
		log.Printf("    Model: %s", modelName(vg.Model))
		log.Printf("    Nugget: %.4f", vg.Nugget)
		log.Printf("    Sill:   %.4f", vg.Sill)
		log.Printf("    Range:  %.2f m", vg.Range)
		if vg.AnisoRatio != 1.0 {
			log.Printf("    Aniso:  ratio=%.2f angle=%.1fdeg",
				vg.AnisoRatio, vg.AnisoAngle*kriging.RadToDeg)
		}
	}

	log.Printf("==========================")
}

func modelName(m kriging.VariogramModel) string {
	switch m {
	case kriging.ModelSpherical:
		return "Spherical"
	case kriging.ModelExponential:
		return "Exponential"
	case kriging.ModelGaussian:
		return "Gaussian"
	default:
		return "Unknown"
	}
}

func buildDefaultSSP() *model.SoundSpeedProfile {
	entries := make([]model.SSPEntry, 0, 20)
	depthStep := 100.0
	speedBase := 1530.0

	for i := 0; i < 20; i++ {
		depth := float64(i) * depthStep
		speed := speedBase - 1.5*float64(i) + 0.0005*float64(i*i)
		if speed < 1450 {
			speed = 1450 + 0.1*float64(i)
		}
		entries = append(entries, model.SSPEntry{
			DepthM:       depth,
			SoundSpeedMs: speed,
		})
	}

	return &model.SoundSpeedProfile{
		Entries:     entries,
		TimestampUS: uint64(time.Now().UnixNano() / 1000),
	}
}

func main() {
	cfg := GatewayConfig{
		SonarIface:     "eth0",
		SonarGroup:     "239.100.1.1",
		SonarUdpPort:   56789,
		SonarTcpPort:   56791,
		SonarBuffer:    32 * 1024 * 1024,
		SonarEnableTcp: true,

		INSAddr:        "0.0.0.0",
		INSPort:        56790,

		RedisAddr:      "127.0.0.1:6379",
		RedisPassword:  "",
		RedisDB:        0,

		MaxCacheFrames: 128,
		DebugLogging:   false,
		MetricsInterval: 5 * time.Second,

		VoidFillEnable:       true,
		VoidFillCellSize:     0.5,
		VoidFillEveryNFrames: 16,
		VoidFillSearchRadius: 4,
		VoidFillVariogramNugget:    0.01,
		VoidFillVariogramSill:      1.0,
		VoidFillVariogramRange:     50.0,
		VoidFillAnisoAngle:         0.0,
		VoidFillAnisoRatio:         1.0,
		VoidFillMaxInterpolateTick: 256,
	}

	gw := NewGateway(cfg)
	if err := gw.Init(); err != nil {
		log.Fatalf("failed to initialize gateway: %v", err)
	}

	if err := gw.Start(); err != nil {
		log.Fatalf("failed to start gateway: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	log.Printf("received shutdown signal")

	gw.Stop()
}
