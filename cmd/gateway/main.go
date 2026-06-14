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
)

type GatewayConfig struct {
	SonarIface     string
	SonarGroup     string
	SonarPort      int
	SonarBuffer    int

	INSAddr        string
	INSPort        int

	RedisAddr      string
	RedisPassword  string
	RedisDB        int

	MaxCacheFrames int
	SSPProfile     *model.SoundSpeedProfile
}

type Gateway struct {
	config     GatewayConfig
	sonarRx    *multicast.SonarReceiver
	insReader  *ins.INSReader
	beamProc   *beam.Processor
	reorganizer *pointcloud.Reorganizer
	redisHub   *redisx.Hub
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

	insFromRedis, err := g.redisHub.GetLatestINS()
	if err == nil && insFromRedis != nil {
		g.insReader.SetDataHandler(nil)
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
		g.config.SonarPort,
		g.config.SonarBuffer,
	)
	if err != nil {
		return fmt.Errorf("create sonar receiver: %w", err)
	}
	g.sonarRx = sonarRx

	g.sonarRx.SetPingHandler(func(ping *model.SonarPing) {
		if err := g.redisHub.PublishPing(ping); err != nil {
			log.Printf("warning: failed to publish ping: %v", err)
		}
		g.beamProc.ProcessPing(ping)
	})

	g.insReader.SetDataHandler(func(insData *model.INSData) {
		if err := g.redisHub.PublishINS(insData); err != nil {
			log.Printf("warning: failed to publish INS: %v", err)
		}
	})

	if err := g.redisHub.PublishSSP(g.config.SSPProfile); err != nil {
		log.Printf("warning: failed to publish initial SSP: %v", err)
	}

	g.redisHub.PublishStatsPeriodically(10 * time.Second)

	return nil
}

func (g *Gateway) Start() error {
	if err := g.insReader.Start(); err != nil {
		return fmt.Errorf("start INS reader: %w", err)
	}

	if err := g.sonarRx.Start(); err != nil {
		return fmt.Errorf("start sonar receiver: %w", err)
	}

	log.Printf("=== AUV Sonar Gateway started ===")
	log.Printf("  Sonar: %s:%d (%s)", g.config.SonarGroup, g.config.SonarPort, g.config.SonarIface)
	log.Printf("  INS:   %s:%d", g.config.INSAddr, g.config.INSPort)
	log.Printf("  Redis: %s (DB=%d)", g.config.RedisAddr, g.config.RedisDB)
	log.Printf("  Frame cache: %d frames", g.config.MaxCacheFrames)
	return nil
}

func (g *Gateway) Stop() {
	log.Printf("=== AUV Sonar Gateway shutting down ===")

	g.sonarRx.Stop()
	g.insReader.Stop()

	g.reorganizer.Flush()

	g.redisHub.Disconnect()

	processed, dropped := g.beamProc.GetStats()
	sonarStats := g.sonarRx.GetStats()
	redisStats := g.redisHub.GetStats()

	log.Printf("--- Final statistics ---")
	log.Printf("Beam processor: processed=%d, dropped=%d", processed, dropped)
	log.Printf("Sonar receiver: pkts=%d, decode_errs=%d, bytes=%d",
		sonarStats.PacketsReceived, sonarStats.DecodeErrors, sonarStats.BytesReceived)
	log.Printf("Redis hub: frames=%d, points=%d, ssp=%d, ins=%d",
		redisStats.FramesPublished, redisStats.PointsPublished, redisStats.SSPUpdates, redisStats.InspDataUpdates)

	log.Printf("=== AUV Sonar Gateway stopped ===")
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
			DepthM:        depth,
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
		SonarPort:      56789,
		SonarBuffer:    32 * 1024 * 1024,

		INSAddr:        "0.0.0.0",
		INSPort:        56790,

		RedisAddr:      "127.0.0.1:6379",
		RedisPassword:  "",
		RedisDB:        0,

		MaxCacheFrames: 128,
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
