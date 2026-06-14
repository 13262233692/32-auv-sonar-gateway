package ssp

import (
	"math"

	"auv-sonar-gateway/internal/model"
)

type RayTraceResult struct {
	SlantRange    float64
	HorizontalOffset float64
	DepthOffset   float64
	TravelTime    float64
}

type RayTracer struct {
	profile *model.SoundSpeedProfile
	maxIter int
	tol     float64
}

func NewRayTracer(profile *model.SoundSpeedProfile) *RayTracer {
	return &RayTracer{
		profile: profile,
		maxIter: 50,
		tol:     1e-6,
	}
}

func (rt *RayTracer) Trace(beamAngleRad float64, travelTimeSec float64) RayTraceResult {
	if rt.profile == nil || len(rt.profile.Entries) == 0 {
		slant := 1500.0 * travelTimeSec
		horiz := slant * math.Sin(beamAngleRad)
		depth := slant * math.Cos(beamAngleRad)
		return RayTraceResult{
			SlantRange:       slant,
			HorizontalOffset: horiz,
			DepthOffset:      depth,
			TravelTime:       travelTimeSec,
		}
	}

	return rt.traceLayered(beamAngleRad, travelTimeSec)
}

func (rt *RayTracer) traceLayered(beamAngleRad float64, travelTimeSec float64) RayTraceResult {
	entries := rt.profile.Entries
	nLayers := len(entries) - 1
	if nLayers < 1 {
		slant := rt.profile.MeanSpeed() * travelTimeSec
		return RayTraceResult{
			SlantRange:       slant,
			HorizontalOffset: slant * math.Sin(beamAngleRad),
			DepthOffset:      slant * math.Cos(beamAngleRad),
			TravelTime:       travelTimeSec,
		}
	}

	theta := beamAngleRad
	if theta < 0 {
		theta = -theta
	}

	c0 := entries[0].SoundSpeedMs
	sinTheta0 := math.Sin(theta)
	p := sinTheta0 / c0

	var totalHoriz float64
	var totalDepth float64
	var totalTime float64
	remainingTime := travelTimeSec

	for i := 0; i < nLayers && remainingTime > 1e-12; i++ {
		c1 := entries[i].SoundSpeedMs
		c2 := entries[i+1].SoundSpeedMs
		layerDepth := entries[i+1].DepthM - entries[i].DepthM

		sinTheta1 := p * c1
		if sinTheta1 >= 1.0 {
			break
		}
		cosTheta1 := math.Sqrt(1.0 - sinTheta1*sinTheta1)

		sinTheta2 := p * c2
		if sinTheta2 >= 1.0 {
			break
		}
		cosTheta2 := math.Sqrt(1.0 - sinTheta2*sinTheta2)

		avgCosTheta := (cosTheta1 + cosTheta2) / 2.0
		layerTime := layerDepth / (c1 * avgCosTheta)

		if layerTime > remainingTime {
			fraction := remainingTime / layerTime
			actualDepth := layerDepth * fraction
			horizInLayer := actualDepth * math.Tan(math.Asin(sinTheta1))
			totalHoriz += horizInLayer
			totalDepth += actualDepth
			totalTime += remainingTime
			break
		}

		horizInLayer := layerDepth * math.Tan(math.Asin(sinTheta1))
		totalHoriz += horizInLayer
		totalDepth += layerDepth
		totalTime += layerTime
		remainingTime -= layerTime
	}

	slantRange := math.Sqrt(totalHoriz*totalHoriz + totalDepth*totalDepth)

	return RayTraceResult{
		SlantRange:       slantRange,
		HorizontalOffset: totalHoriz,
		DepthOffset:      totalDepth,
		TravelTime:       travelTimeSec,
	}
}

func (rt *RayTracer) UpdateProfile(profile *model.SoundSpeedProfile) {
	rt.profile = profile
}

func EquivalentBeamAngle(slantRange float64, beamAngleRad float64) float64 {
	cosA := math.Cos(beamAngleRad)
	if math.Abs(cosA) < 1e-12 {
		return math.Pi / 2.0
	}
	return math.Atan2(slantRange*math.Sin(beamAngleRad), slantRange*math.Cos(beamAngleRad))
}
