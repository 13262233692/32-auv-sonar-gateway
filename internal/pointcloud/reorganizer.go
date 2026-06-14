package pointcloud

import (
	"container/heap"
	"sort"
	"sync"

	"auv-sonar-gateway/internal/model"
	"auv-sonar-gateway/internal/voidfill"
)

type FrameHeap []*model.PointCloudFrame

func (h FrameHeap) Len() int           { return len(h) }
func (h FrameHeap) Less(i, j int) bool { return h[i].TimestampUS < h[j].TimestampUS }
func (h FrameHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *FrameHeap) Push(x interface{}) {
	*h = append(*h, x.(*model.PointCloudFrame))
}

func (h *FrameHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return item
}

type VoidFillCallback func(frames []*model.PointCloudFrame) ([]model.Point3D, []voidfill.PatchFillResult, error)

type Reorganizer struct {
	maxCacheFrames    int
	frameHeap         FrameHeap
	cache             map[uint16]*model.PointCloudFrame
	mu                sync.Mutex
	onReadyFrames     func([]*model.PointCloudFrame)
	frameOrder        []uint16
	maxOrder          int

	voidFiller        *voidfill.VoidFiller
	onVoidFill        VoidFillCallback
	fillEveryNFrames  int
	framesSinceFill   int
	autoFillEnabled   bool
	filledPoints      []model.Point3D
	fillResults       []voidfill.PatchFillResult
	fillMu            sync.Mutex
}

func NewReorganizer(maxCacheFrames int) *Reorganizer {
	r := &Reorganizer{
		maxCacheFrames:   maxCacheFrames,
		cache:            make(map[uint16]*model.PointCloudFrame),
		frameOrder:       make([]uint16, 0, maxCacheFrames),
		fillEveryNFrames: 16,
	}
	heap.Init(&r.frameHeap)
	return r
}

func (r *Reorganizer) EnableVoidFill(filler *voidfill.VoidFiller, everyNFrames int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.voidFiller = filler
	r.autoFillEnabled = filler != nil
	if everyNFrames > 0 {
		r.fillEveryNFrames = everyNFrames
	}
	r.onVoidFill = func(frames []*model.PointCloudFrame) ([]model.Point3D, []voidfill.PatchFillResult, error) {
		return r.voidFiller.FillFrames(frames)
	}
}

func (r *Reorganizer) SetReadyHandler(handler func([]*model.PointCloudFrame)) {
	r.onReadyFrames = handler
}

func (r *Reorganizer) SetVoidFillCallback(cb VoidFillCallback) {
	r.onVoidFill = cb
}

func (r *Reorganizer) SubmitFrame(frame *model.PointCloudFrame) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if frame == nil || frame.NumPoints == 0 {
		return
	}

	r.cache[frame.PingCounter] = frame
	heap.Push(&r.frameHeap, frame)
	r.frameOrder = append(r.frameOrder, frame.PingCounter)

	for len(r.cache) > r.maxCacheFrames {
		r.evictOldestLocked()
	}

	if r.autoFillEnabled && r.onVoidFill != nil {
		r.framesSinceFill++
		if r.framesSinceFill >= r.fillEveryNFrames {
			r.framesSinceFill = 0
			r.triggerFillLocked()
		}
	}
}

func (r *Reorganizer) triggerFillLocked() {
	if !r.autoFillEnabled || r.onVoidFill == nil {
		return
	}

	frames := make([]*model.PointCloudFrame, 0, len(r.cache))
	tmpHeap := make(FrameHeap, len(r.frameHeap))
	copy(tmpHeap, r.frameHeap)

	for tmpHeap.Len() > 0 {
		frames = append(frames, heap.Pop(&tmpHeap).(*model.PointCloudFrame))
	}

	if len(frames) < 2 {
		return
	}

	go func() {
		filled, results, err := r.onVoidFill(frames)
		if err != nil {
			return
		}
		r.fillMu.Lock()
		r.filledPoints = filled
		r.fillResults = results
		r.fillMu.Unlock()

		if r.onReadyFrames != nil {
			filledFrame := &model.PointCloudFrame{
				TimestampUS: frames[len(frames)-1].TimestampUS,
				PingCounter: frames[len(frames)-1].PingCounter,
				Points:      filled,
				NumPoints:   len(filled),
			}
			r.onReadyFrames([]*model.PointCloudFrame{filledFrame})
		}
	}()
}

func (r *Reorganizer) GetFilledPoints() []model.Point3D {
	r.fillMu.Lock()
	defer r.fillMu.Unlock()
	return r.filledPoints
}

func (r *Reorganizer) GetFillResults() []voidfill.PatchFillResult {
	r.fillMu.Lock()
	defer r.fillMu.Unlock()
	return r.fillResults
}

func (r *Reorganizer) ForceFill() ([]model.Point3D, []voidfill.PatchFillResult, error) {
	r.mu.Lock()
	if !r.autoFillEnabled || r.onVoidFill == nil {
		r.mu.Unlock()
		return nil, nil, nil
	}

	frames := make([]*model.PointCloudFrame, 0, len(r.cache))
	tmpHeap := make(FrameHeap, len(r.frameHeap))
	copy(tmpHeap, r.frameHeap)

	for tmpHeap.Len() > 0 {
		frames = append(frames, heap.Pop(&tmpHeap).(*model.PointCloudFrame))
	}
	r.mu.Unlock()

	if len(frames) == 0 {
		return nil, nil, nil
	}

	filled, results, err := r.onVoidFill(frames)
	if err != nil {
		return nil, nil, err
	}

	r.fillMu.Lock()
	r.filledPoints = filled
	r.fillResults = results
	r.fillMu.Unlock()

	return filled, results, nil
}

func (r *Reorganizer) evictOldestLocked() {
	if len(r.frameOrder) == 0 {
		return
	}
	oldestPing := r.frameOrder[0]
	r.frameOrder = r.frameOrder[1:]
	if f, ok := r.cache[oldestPing]; ok {
		delete(r.cache, oldestPing)
		for i, hf := range r.frameHeap {
			if hf == f {
				heap.Remove(&r.frameHeap, i)
				break
			}
		}
	}
}

func (r *Reorganizer) GetFrame(pingCounter uint16) *model.PointCloudFrame {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cache[pingCounter]
}

func (r *Reorganizer) GetOrderedFrames() []*model.PointCloudFrame {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := make([]*model.PointCloudFrame, 0, len(r.cache))
	tmpHeap := make(FrameHeap, len(r.frameHeap))
	copy(tmpHeap, r.frameHeap)

	for tmpHeap.Len() > 0 {
		result = append(result, heap.Pop(&tmpHeap).(*model.PointCloudFrame))
	}
	return result
}

func (r *Reorganizer) Flush() []*model.PointCloudFrame {
	r.mu.Lock()
	defer r.mu.Unlock()

	result := make([]*model.PointCloudFrame, 0, len(r.cache))
	tmpHeap := make(FrameHeap, len(r.frameHeap))
	copy(tmpHeap, r.frameHeap)

	for tmpHeap.Len() > 0 {
		result = append(result, heap.Pop(&tmpHeap).(*model.PointCloudFrame))
	}

	r.cache = make(map[uint16]*model.PointCloudFrame)
	r.frameHeap = r.frameHeap[:0]
	r.frameOrder = r.frameOrder[:0]
	heap.Init(&r.frameHeap)

	if r.autoFillEnabled && r.onVoidFill != nil && len(result) >= 2 {
		filled, results, err := r.onVoidFill(result)
		if err == nil && len(filled) > 0 {
			filledFrame := &model.PointCloudFrame{
				TimestampUS: result[len(result)-1].TimestampUS,
				PingCounter: result[len(result)-1].PingCounter,
				Points:      filled,
				NumPoints:   len(filled),
			}
			result = append(result, filledFrame)

			r.fillMu.Lock()
			r.filledPoints = filled
			r.fillResults = results
			r.fillMu.Unlock()
		}
	}

	if r.onReadyFrames != nil && len(result) > 0 {
		r.onReadyFrames(result)
	}

	return result
}

func (r *Reorganizer) SpatialVoxelGrid(frames []*model.PointCloudFrame, voxelSize float64) []model.Point3D {
	type VoxelKey struct {
		x, y, z int64
	}

	voxelMap := make(map[VoxelKey]model.Point3D)
	invSize := 1.0 / voxelSize

	for _, f := range frames {
		for i := range f.Points {
			p := f.Points[i]
			kx := int64(p.X * invSize)
			ky := int64(p.Y * invSize)
			kz := int64(p.Z * invSize)
			key := VoxelKey{kx, ky, kz}

			if existing, ok := voxelMap[key]; ok {
				if p.Intensity > existing.Intensity {
					voxelMap[key] = p
				}
			} else {
				voxelMap[key] = p
			}
		}
	}

	result := make([]model.Point3D, 0, len(voxelMap))
	for _, p := range voxelMap {
		result = append(result, p)
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Latitude != result[j].Latitude {
			return result[i].Latitude < result[j].Latitude
		}
		if result[i].Longitude != result[j].Longitude {
			return result[i].Longitude < result[j].Longitude
		}
		return result[i].Depth < result[j].Depth
	})

	return result
}

func (r *Reorganizer) Size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.cache)
}
