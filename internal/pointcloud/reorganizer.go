package pointcloud

import (
	"container/heap"
	"sort"
	"sync"

	"auv-sonar-gateway/internal/model"
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

type Reorganizer struct {
	maxCacheFrames int
	frameHeap      FrameHeap
	cache          map[uint16]*model.PointCloudFrame
	mu             sync.Mutex
	onReadyFrames  func([]*model.PointCloudFrame)
	frameOrder     []uint16
	maxOrder       int
}

func NewReorganizer(maxCacheFrames int) *Reorganizer {
	r := &Reorganizer{
		maxCacheFrames: maxCacheFrames,
		cache:          make(map[uint16]*model.PointCloudFrame),
		frameOrder:     make([]uint16, 0, maxCacheFrames),
	}
	heap.Init(&r.frameHeap)
	return r
}

func (r *Reorganizer) SetReadyHandler(handler func([]*model.PointCloudFrame)) {
	r.onReadyFrames = handler
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
