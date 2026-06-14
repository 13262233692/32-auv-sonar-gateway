package redisx

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"auv-sonar-gateway/internal/model"
)

type Hub struct {
	client       *redis.Client
	ctx          context.Context
	opts         HubOptions
	running      atomic.Bool
	wg           sync.WaitGroup
	statsMutex   sync.Mutex
	stats        HubStats
	onSSPUpdate  func(*model.SoundSpeedProfile)
}

type HubOptions struct {
	Addr         string
	Password     string
	DB           int
	PoolSize     int
	KeyPrefix    string
	SSPChannel   string
	FrameChannel string
	StatsChannel string
}

type HubStats struct {
	PingsPublished   uint64
	FramesPublished  uint64
	PointsPublished  uint64
	SSPUpdates       uint64
	InspDataUpdates  uint64
}

const (
	DefaultAddr         = "127.0.0.1:6379"
	DefaultKeyPrefix    = "auv:sonar:"
	DefaultSSPChannel   = "auv:sonar:ssp"
	DefaultFrameChannel = "auv:sonar:frames"
	DefaultStatsChannel = "auv:sonar:stats"
)

func DefaultOptions() HubOptions {
	return HubOptions{
		Addr:         DefaultAddr,
		Password:     "",
		DB:           0,
		PoolSize:     32,
		KeyPrefix:    DefaultKeyPrefix,
		SSPChannel:   DefaultSSPChannel,
		FrameChannel: DefaultFrameChannel,
		StatsChannel: DefaultStatsChannel,
	}
}

func NewHub(opts HubOptions) *Hub {
	return &Hub{
		opts: opts,
		ctx:  context.Background(),
	}
}

func (h *Hub) SetSSPUpdateHandler(handler func(*model.SoundSpeedProfile)) {
	h.onSSPUpdate = handler
}

func (h *Hub) Connect() error {
	if !h.running.CompareAndSwap(false, true) {
		return fmt.Errorf("redis hub already connected")
	}

	h.client = redis.NewClient(&redis.Options{
		Addr:     h.opts.Addr,
		Password: h.opts.Password,
		DB:       h.opts.DB,
		PoolSize: h.opts.PoolSize,
	})

	ctx, cancel := context.WithTimeout(h.ctx, 5*time.Second)
	defer cancel()

	if err := h.client.Ping(ctx).Err(); err != nil {
		h.running.Store(false)
		return fmt.Errorf("redis ping failed: %w", err)
	}

	log.Printf("connected to Redis at %s", h.opts.Addr)

	h.wg.Add(1)
	go h.listenSSP()

	return nil
}

func (h *Hub) Disconnect() {
	if !h.running.CompareAndSwap(true, false) {
		return
	}
	if h.client != nil {
		h.client.Close()
	}
	h.wg.Wait()
	log.Printf("disconnected from Redis")
}

func (h *Hub) key(name string) string {
	return h.opts.KeyPrefix + name
}

func (h *Hub) PublishPing(ping *model.SonarPing) error {
	if ping == nil {
		return nil
	}
	data, err := json.Marshal(ping)
	if err != nil {
		return fmt.Errorf("marshal ping: %w", err)
	}

	score := float64(ping.TimestampUS) / 1e6
	key := h.key(fmt.Sprintf("pings:%d", time.Now().Unix()/3600))

	pipe := h.client.Pipeline()
	pipe.ZAdd(h.ctx, key, redis.Z{
		Score:  score,
		Member: data,
	})
	pipe.Expire(h.ctx, key, 24*time.Hour)

	_, err = pipe.Exec(h.ctx)
	if err != nil {
		return fmt.Errorf("publish ping pipeline: %w", err)
	}

	h.statsMutex.Lock()
	h.stats.PingsPublished++
	h.statsMutex.Unlock()

	return nil
}

func (h *Hub) PublishFrame(frame *model.PointCloudFrame) error {
	if frame == nil || frame.NumPoints == 0 {
		return nil
	}

	data, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("marshal frame: %w", err)
	}

	score := float64(frame.TimestampUS) / 1e6
	hourKey := time.Now().Unix() / 3600
	zsetKey := h.key(fmt.Sprintf("frames:%d", hourKey))
	hashKey := h.key(fmt.Sprintf("frame:%d", frame.PingCounter))

	pipe := h.client.Pipeline()
	pipe.ZAdd(h.ctx, zsetKey, redis.Z{
		Score:  score,
		Member: frame.PingCounter,
	})
	pipe.Expire(h.ctx, zsetKey, 48*time.Hour)

	pipe.HSet(h.ctx, hashKey,
		"timestamp", frame.TimestampUS,
		"ping_counter", frame.PingCounter,
		"num_points", frame.NumPoints,
		"data", data,
	)
	pipe.Expire(h.ctx, hashKey, 48*time.Hour)

	pipe.Publish(h.ctx, h.opts.FrameChannel, data)

	if _, err := pipe.Exec(h.ctx); err != nil {
		return fmt.Errorf("publish frame pipeline: %w", err)
	}

	h.statsMutex.Lock()
	h.stats.FramesPublished++
	h.stats.PointsPublished += uint64(frame.NumPoints)
	h.statsMutex.Unlock()

	return nil
}

func (h *Hub) PublishINS(ins *model.INSData) error {
	if ins == nil {
		return nil
	}

	data, err := json.Marshal(ins)
	if err != nil {
		return fmt.Errorf("marshal INS: %w", err)
	}

	hashKey := h.key("ins:latest")
	pipe := h.client.Pipeline()
	pipe.HSet(h.ctx, hashKey,
		"timestamp", ins.TimestampUS,
		"lat", ins.LatDeg7,
		"lon", ins.LonDeg7,
		"depth", ins.DepthMM,
		"roll", ins.RollCentidg,
		"pitch", ins.PitchCentidg,
		"heading", ins.HeadingCentidg,
		"heave", ins.HeaveMM,
		"data", data,
	)

	score := float64(ins.TimestampUS) / 1e6
	histKey := h.key(fmt.Sprintf("ins:hist:%d", time.Now().Unix()/3600))
	pipe.ZAdd(h.ctx, histKey, redis.Z{
		Score:  score,
		Member: data,
	})
	pipe.Expire(h.ctx, histKey, 24*time.Hour)

	if _, err := pipe.Exec(h.ctx); err != nil {
		return fmt.Errorf("publish INS pipeline: %w", err)
	}

	h.statsMutex.Lock()
	h.stats.InspDataUpdates++
	h.statsMutex.Unlock()

	return nil
}

func (h *Hub) PublishSSP(ssp *model.SoundSpeedProfile) error {
	if ssp == nil {
		return nil
	}
	data, err := json.Marshal(ssp)
	if err != nil {
		return fmt.Errorf("marshal SSP: %w", err)
	}

	hashKey := h.key("ssp:latest")
	pipe := h.client.Pipeline()
	pipe.HSet(h.ctx, hashKey,
		"timestamp", ssp.TimestampUS,
		"num_entries", len(ssp.Entries),
		"data", data,
	)
	pipe.Publish(h.ctx, h.opts.SSPChannel, data)

	if _, err := pipe.Exec(h.ctx); err != nil {
		return fmt.Errorf("publish SSP pipeline: %w", err)
	}

	h.statsMutex.Lock()
	h.stats.SSPUpdates++
	h.statsMutex.Unlock()

	return nil
}

func (h *Hub) GetLatestINS() (*model.INSData, error) {
	hashKey := h.key("ins:latest")
	data, err := h.client.HGet(h.ctx, hashKey, "data").Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get latest INS: %w", err)
	}
	var ins model.INSData
	if err := json.Unmarshal([]byte(data), &ins); err != nil {
		return nil, fmt.Errorf("unmarshal INS: %w", err)
	}
	return &ins, nil
}

func (h *Hub) GetLatestSSP() (*model.SoundSpeedProfile, error) {
	hashKey := h.key("ssp:latest")
	data, err := h.client.HGet(h.ctx, hashKey, "data").Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get latest SSP: %w", err)
	}
	var ssp model.SoundSpeedProfile
	if err := json.Unmarshal([]byte(data), &ssp); err != nil {
		return nil, fmt.Errorf("unmarshal SSP: %w", err)
	}
	return &ssp, nil
}

func (h *Hub) GetFramesInRange(startTimeUS, endTimeUS uint64) ([]*model.PointCloudFrame, error) {
	startScore := float64(startTimeUS) / 1e6
	endScore := float64(endTimeUS) / 1e6
	startHour := int64(startScore) / 3600
	endHour := int64(endScore) / 3600

	var allPings []uint16
	for h2 := startHour; h2 <= endHour; h2++ {
		zsetKey := h.key(fmt.Sprintf("frames:%d", h2))
		result, err := h.client.ZRangeByScore(h.ctx, zsetKey, &redis.ZRangeBy{
			Min: fmt.Sprintf("%f", startScore),
			Max: fmt.Sprintf("%f", endScore),
		}).Result()
		if err != nil {
			log.Printf("warning: zrange failed for %s: %v", zsetKey, err)
			continue
		}
		for _, s := range result {
			var v uint16
			fmt.Sscanf(s, "%d", &v)
			allPings = append(allPings, v)
		}
	}

	frames := make([]*model.PointCloudFrame, 0, len(allPings))
	for _, pingCnt := range allPings {
		hashKey := h.key(fmt.Sprintf("frame:%d", pingCnt))
		data, err := h.client.HGet(h.ctx, hashKey, "data").Result()
		if err != nil {
			log.Printf("warning: hget failed for frame %d: %v", pingCnt, err)
			continue
		}
		var f model.PointCloudFrame
		if err := json.Unmarshal([]byte(data), &f); err != nil {
			log.Printf("warning: unmarshal frame %d failed: %v", pingCnt, err)
			continue
		}
		frames = append(frames, &f)
	}

	return frames, nil
}

func (h *Hub) GetStats() HubStats {
	h.statsMutex.Lock()
	defer h.statsMutex.Unlock()
	return h.stats
}

func (h *Hub) listenSSP() {
	defer h.wg.Done()

	for h.running.Load() {
		pubsub := h.client.Subscribe(h.ctx, h.opts.SSPChannel)
		ch := pubsub.Channel()

		func() {
			defer pubsub.Close()
			for msg := range ch {
				var ssp model.SoundSpeedProfile
				if err := json.Unmarshal([]byte(msg.Payload), &ssp); err != nil {
					log.Printf("warning: invalid SSP payload: %v", err)
					continue
				}
				if h.onSSPUpdate != nil {
					h.onSSPUpdate(&ssp)
				}
			}
		}()

		if h.running.Load() {
			time.Sleep(time.Second)
		}
	}
}

func (h *Hub) PublishStatsPeriodically(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for h.running.Load() {
			<-ticker.C
			stats := h.GetStats()
			data, err := json.Marshal(stats)
			if err != nil {
				continue
			}
			h.client.Publish(h.ctx, h.opts.StatsChannel, data)
		}
	}()
}
