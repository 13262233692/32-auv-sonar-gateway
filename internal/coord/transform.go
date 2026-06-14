package coord

import (
	"math"

	"auv-sonar-gateway/internal/model"
)

const (
	WGS84A     = 6378137.0
	WGS84F     = 1.0 / 298.257223563
	WGS84B     = WGS84A * (1 - WGS84F)
	WGS84E2    = 2*WGS84F - WGS84F*WGS84F
	DegToRad   = math.Pi / 180.0
	RadToDeg   = 180.0 / math.Pi
	MercatorK0 = 0.9996
)

type TransformEngine struct{}

func NewTransformEngine() *TransformEngine {
	return &TransformEngine{}
}

func (t *TransformEngine) RotationMatrixRPY(roll, pitch, heading float64) [3][3]float64 {
	cr, sr := math.Cos(roll), math.Sin(roll)
	cp, sp := math.Cos(pitch), math.Sin(pitch)
	ch, sh := math.Cos(heading), math.Sin(heading)

	var R [3][3]float64

	R[0][0] = ch*cp
	R[0][1] = -sh*cr + ch*sp*sr
	R[0][2] = sh*sr + ch*sp*cr

	R[1][0] = sh*cp
	R[1][1] = ch*cr + sh*sp*sr
	R[1][2] = -ch*sr + sh*sp*cr

	R[2][0] = -sp
	R[2][1] = cp*sr
	R[2][2] = cp*cr

	return R
}

func (t *TransformEngine) ApplyRotation(R [3][3]float64, v model.Vec3) model.Vec3 {
	return model.Vec3{
		X: R[0][0]*v.X + R[0][1]*v.Y + R[0][2]*v.Z,
		Y: R[1][0]*v.X + R[1][1]*v.Y + R[1][2]*v.Z,
		Z: R[2][0]*v.X + R[2][1]*v.Y + R[2][2]*v.Z,
	}
}

func (t *TransformEngine) AcousticToBody(horizOffset, depthOffset, tiltAlong, tiltAcross float64) model.Vec3 {
	ctA, stA := math.Cos(tiltAlong), math.Sin(tiltAlong)
	ctC, stC := math.Cos(tiltAcross), math.Sin(tiltAcross)

	x := horizOffset*ctA - depthOffset*stA
	y := horizOffset*stC*stA + depthOffset*stC*ctA
	z := -horizOffset*ctC*stA - depthOffset*ctC*ctA

	return model.Vec3{X: x, Y: y, Z: z}
}

func (t *TransformEngine) BodyToNav(body model.Vec3, roll, pitch, heading float64) model.Vec3 {
	R := t.RotationMatrixRPY(roll, pitch, heading)
	return t.ApplyRotation(R, body)
}

func (t *TransformEngine) NavToGeo(nav model.Vec3, ins *model.INSData) model.Point3D {
	lat := ins.Latitude() * DegToRad
	_ = ins.Longitude() * DegToRad

	Rn := WGS84A / math.Sqrt(1-WGS84E2*math.Sin(lat)*math.Sin(lat))
	Rm := Rn * (1 - WGS84E2) / (1 - WGS84E2*math.Sin(lat)*math.Sin(lat))

	dLat := nav.Z / Rm * RadToDeg
	dLon := nav.X / (Rn * math.Cos(lat)) * RadToDeg

	return model.Point3D{
		Latitude:  ins.Latitude() + dLat,
		Longitude: ins.Longitude() + dLon,
		Depth:     ins.DepthM() - nav.Y,
		X:         nav.X,
		Y:         nav.Y,
		Z:         nav.Z,
	}
}

func (t *TransformEngine) FullTransform(horizOffset, depthOffset float64, tiltAlong, tiltAcross float64, ins *model.INSData) model.Point3D {
	body := t.AcousticToBody(horizOffset, depthOffset, tiltAlong, tiltAcross)

	nav := t.BodyToNav(body, ins.RollRad(), ins.PitchRad(), ins.HeadingRad())

	geo := t.NavToGeo(nav, ins)
	return geo
}

func LocalENUOffset(lat1, lon1, lat2, lon2 float64) (east, north float64) {
	lat1R := lat1 * DegToRad
	lat2R := lat2 * DegToRad
	midLat := (lat1R + lat2R) / 2.0

	dLon := (lon2 - lon1) * DegToRad

	Rn := WGS84A / math.Sqrt(1-WGS84E2*math.Sin(midLat)*math.Sin(midLat))

	east = Rn * math.Cos(midLat) * dLon
	north = Rn * (lat2R - lat1R)
	return
}
