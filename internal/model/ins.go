package model

type INSData struct {
	TimestampUS uint64
	LatDeg7     int64
	LonDeg7     int64
	DepthMM     int32
	RollCentidg int16
	PitchCentidg int16
	HeadingCentidg int16
	HeaveMM     int16
	VelXMM      int16
	VelYMM      int16
	VelZMM      int16
}

func (d *INSData) Latitude() float64 {
	return float64(d.LatDeg7) / 1e7
}

func (d *INSData) Longitude() float64 {
	return float64(d.LonDeg7) / 1e7
}

func (d *INSData) DepthM() float64 {
	return float64(d.DepthMM) / 1000.0
}

func (d *INSData) RollRad() float64 {
	return float64(d.RollCentidg) / 100.0 * (3.14159265358979323846 / 180.0)
}

func (d *INSData) PitchRad() float64 {
	return float64(d.PitchCentidg) / 100.0 * (3.14159265358979323846 / 180.0)
}

func (d *INSData) HeadingRad() float64 {
	return float64(d.HeadingCentidg) / 100.0 * (3.14159265358979323846 / 180.0)
}

func (d *INSData) HeaveM() float64 {
	return float64(d.HeaveMM) / 1000.0
}
