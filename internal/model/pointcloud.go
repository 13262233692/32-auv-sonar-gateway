package model

type Vec3 struct {
	X float64
	Y float64
	Z float64
}

type Point3D struct {
	Latitude  float64
	Longitude float64
	Depth     float64
	X         float64
	Y         float64
	Z         float64
	Intensity uint8
	TimestampUS uint64
	PingIndex   uint16
	BeamIndex   uint16
}

type PointCloudFrame struct {
	TimestampUS uint64
	PingCounter uint16
	Points      []Point3D
	NumPoints   int
}

func NewPointCloudFrame(timestampUS uint64, pingCounter uint16, capacity int) *PointCloudFrame {
	return &PointCloudFrame{
		TimestampUS: timestampUS,
		PingCounter: pingCounter,
		Points:      make([]Point3D, 0, capacity),
		NumPoints:   0,
	}
}

func (f *PointCloudFrame) AddPoint(p Point3D) {
	f.Points = append(f.Points, p)
	f.NumPoints++
}
