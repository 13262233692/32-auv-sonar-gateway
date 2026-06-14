package kriging

import "math"

type VariogramModel int

const (
	RadToDeg float64 = 180.0 / 3.14159265358979323846

	ModelSpherical VariogramModel = iota
	ModelExponential
	ModelGaussian
)

type VariogramParams struct {
	Model     VariogramModel
	Nugget    float64
	Sill      float64
	Range     float64
	AnisoAngle float64
	AnisoRatio float64
}

func DefaultVariogramParams() VariogramParams {
	return VariogramParams{
		Model:      ModelSpherical,
		Nugget:     0.01,
		Sill:       1.0,
		Range:      50.0,
		AnisoAngle: 0.0,
		AnisoRatio: 1.0,
	}
}

func (v *VariogramParams) Gamma(h float64) float64 {
	if h <= 0 {
		return 0.0
	}

	switch v.Model {
	case ModelSpherical:
		if h >= v.Range {
			return v.Nugget + v.Sill
		}
		hr := h / v.Range
		return v.Nugget + v.Sill * (1.5*hr - 0.5*hr*hr*hr)

	case ModelExponential:
		hr := h / v.Range
		return v.Nugget + v.Sill * (1.0 - math.Exp(-3.0*hr))

	case ModelGaussian:
		hr := h / v.Range
		return v.Nugget + v.Sill * (1.0 - math.Exp(-3.0*hr*hr))

	default:
		return v.Nugget + v.Sill
	}
}

func (v *VariogramParams) AnisotropicDistance(dx, dy float64) float64 {
	if v.AnisoRatio == 1.0 {
		return math.Sqrt(dx*dx + dy*dy)
	}

	cosA := math.Cos(v.AnisoAngle)
	sinA := math.Sin(v.AnisoAngle)

	dxRot := dx*cosA + dy*sinA
	dyRot := -dx*sinA + dy*cosA

	dxScaled := dxRot
	dyScaled := dyRot / v.AnisoRatio

	return math.Sqrt(dxScaled*dxScaled + dyScaled*dyScaled)
}

type EmpiricalVariogram struct {
	Lags     []float64
	Gamma    []float64
	Counts   []int
	NumLags  int
	LagWidth float64
	MaxDist  float64
}

func NewEmpiricalVariogram(numLags int, maxDist float64) *EmpiricalVariogram {
	ev := &EmpiricalVariogram{
		NumLags:  numLags,
		LagWidth: maxDist / float64(numLags),
		MaxDist:  maxDist,
		Lags:     make([]float64, numLags),
		Gamma:    make([]float64, numLags),
		Counts:   make([]int, numLags),
	}
	for i := 0; i < numLags; i++ {
		ev.Lags[i] = (float64(i) + 0.5) * ev.LagWidth
	}
	return ev
}

func (ev *EmpiricalVariogram) AddPair(dist, valueDiffSq float64) {
	if dist <= 0 || dist >= ev.MaxDist {
		return
	}
	lagIdx := int(dist / ev.LagWidth)
	if lagIdx >= ev.NumLags {
		return
	}
	ev.Gamma[lagIdx] += valueDiffSq
	ev.Counts[lagIdx]++
}

func (ev *EmpiricalVariogram) Finalize() {
	for i := 0; i < ev.NumLags; i++ {
		if ev.Counts[i] > 0 {
			ev.Gamma[i] /= (2.0 * float64(ev.Counts[i]))
		}
	}
}

type SamplePoint struct {
	X     float64
	Y     float64
	Value float64
}

func FitVariogram(samples []SamplePoint, model VariogramModel, maxDist float64, numLags int) VariogramParams {
	ev := NewEmpiricalVariogram(numLags, maxDist)

	n := len(samples)
	maxPairs := n * (n - 1) / 2
	if maxPairs > 5000 {
		return fitVariogramSampled(samples, model, maxDist, numLags)
	}

	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			dx := samples[i].X - samples[j].X
			dy := samples[i].Y - samples[j].Y
			dist := math.Sqrt(dx*dx + dy*dy)
			diffSq := (samples[i].Value - samples[j].Value) * (samples[i].Value - samples[j].Value)
			ev.AddPair(dist, diffSq)
		}
	}
	ev.Finalize()

	return fitFromEmpirical(ev, model)
}

func fitVariogramSampled(samples []SamplePoint, model VariogramModel, maxDist float64, numLags int) VariogramParams {
	ev := NewEmpiricalVariogram(numLags, maxDist)
	n := len(samples)
	step := 1
	if n > 200 {
		step = n / 200
	}

	for i := 0; i < n; i += step {
		for j := i + 1; j < n; j += step {
			dx := samples[i].X - samples[j].X
			dy := samples[i].Y - samples[j].Y
			dist := math.Sqrt(dx*dx + dy*dy)
			diffSq := (samples[i].Value - samples[j].Value) * (samples[i].Value - samples[j].Value)
			ev.AddPair(dist, diffSq)
		}
	}
	ev.Finalize()

	return fitFromEmpirical(ev, model)
}

func fitFromEmpirical(ev *EmpiricalVariogram, model VariogramModel) VariogramParams {
	var maxGamma float64
	var validLags int
	for i := 0; i < ev.NumLags; i++ {
		if ev.Counts[i] > 0 {
			if ev.Gamma[i] > maxGamma {
				maxGamma = ev.Gamma[i]
			}
			validLags++
		}
	}

	if validLags < 3 || maxGamma <= 0 {
		return VariogramParams{
			Model:   model,
			Nugget:  0.01,
			Sill:    1.0,
			Range:   ev.MaxDist / 3.0,
		}
	}

	nugget := estimateNugget(ev)
	sill := maxGamma * 1.1
	if sill <= nugget {
		sill = nugget + 0.1
	}

	practicalRange := estimateRange(ev, nugget, sill)

	return VariogramParams{
		Model:  model,
		Nugget: nugget,
		Sill:   sill - nugget,
		Range:  practicalRange,
	}
}

func estimateNugget(ev *EmpiricalVariogram) float64 {
	for i := 0; i < ev.NumLags; i++ {
		if ev.Counts[i] > 0 {
			return ev.Gamma[i] * 0.5
		}
	}
	return 0.01
}

func estimateRange(ev *EmpiricalVariogram, nugget, sill float64) float64 {
	target := nugget + 0.95*(sill-nugget)
	for i := 0; i < ev.NumLags; i++ {
		if ev.Counts[i] > 0 && ev.Gamma[i] >= target {
			return ev.Lags[i]
		}
	}
	return ev.MaxDist / 3.0
}
