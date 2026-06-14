package kriging

import (
	"fmt"
	"math"
)

const (
	MaxKrigingNeighbors = 24
	MinKrigingNeighbors = 4
	MatrixTolerance     = 1e-12
)

type KrigingResult struct {
	Value        float64
	Variance     float64
	Weights      []float64
	NumNeighbors int
}

type OrdinaryKriging struct {
	Params VariogramParams
	Cache  *DistanceCache
}

type DistanceCache struct {
	distMatrix []float64
	n          int
}

func NewDistanceCache(n int) *DistanceCache {
	return &DistanceCache{
		distMatrix: make([]float64, n*n),
		n:          n,
	}
}

func (dc *DistanceCache) Set(i, j int, dist float64) {
	dc.distMatrix[i*dc.n+j] = dist
	dc.distMatrix[j*dc.n+i] = dist
}

func (dc *DistanceCache) Get(i, j int) float64 {
	return dc.distMatrix[i*dc.n+j]
}

func NewOrdinaryKriging(params VariogramParams) *OrdinaryKriging {
	return &OrdinaryKriging{
		Params: params,
	}
}

func (ok *OrdinaryKriging) Interpolate(targetX, targetY float64, neighbors []SamplePoint) (KrigingResult, error) {
	n := len(neighbors)
	if n < MinKrigingNeighbors {
		return KrigingResult{}, fmt.Errorf("insufficient neighbors: %d < %d", n, MinKrigingNeighbors)
	}
	if n > MaxKrigingNeighbors {
		neighbors = ok.selectNeighbors(targetX, targetY, neighbors, MaxKrigingNeighbors)
		n = len(neighbors)
	}

	size := n + 1
	A := make([]float64, size*size)
	b := make([]float64, size)

	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				A[i*size+j] = 0.0
			} else {
				dx := neighbors[i].X - neighbors[j].X
				dy := neighbors[i].Y - neighbors[j].Y
				h := ok.Params.AnisotropicDistance(dx, dy)
				A[i*size+j] = ok.Params.Gamma(h)
			}
		}
		A[i*size+n] = 1.0
		A[n*size+i] = 1.0
	}
	A[n*size+n] = 0.0

	for i := 0; i < n; i++ {
		dx := neighbors[i].X - targetX
		dy := neighbors[i].Y - targetY
		h := ok.Params.AnisotropicDistance(dx, dy)
		b[i] = ok.Params.Gamma(h)
	}
	b[n] = 1.0

	w, err := solveLinearSystem(A, b, size)
	if err != nil {
		return ok.fallbackIDW(targetX, targetY, neighbors)
	}

	var value float64
	for i := 0; i < n; i++ {
		value += w[i] * neighbors[i].Value
	}

	var krigingVar float64
	for i := 0; i < n; i++ {
		krigingVar += w[i] * b[i]
	}
	krigingVar += w[n]

	if krigingVar < 0 {
		krigingVar = 0
	}

	weights := make([]float64, n)
	copy(weights, w[:n])

	return KrigingResult{
		Value:        value,
		Variance:     krigingVar,
		Weights:      weights,
		NumNeighbors: n,
	}, nil
}

func (ok *OrdinaryKriging) fallbackIDW(targetX, targetY float64, neighbors []SamplePoint) (KrigingResult, error) {
	n := len(neighbors)
	var wSum, wVal float64
	weights := make([]float64, n)

	for i := 0; i < n; i++ {
		dx := neighbors[i].X - targetX
		dy := neighbors[i].Y - targetY
		dist := math.Sqrt(dx*dx + dy*dy)
		if dist < 1e-12 {
			return KrigingResult{
				Value:        neighbors[i].Value,
				Variance:     0,
				Weights:      []float64{1.0},
				NumNeighbors: 1,
			}, nil
		}
		w := 1.0 / (dist * dist)
		wSum += w
		wVal += w * neighbors[i].Value
		weights[i] = w
	}

	for i := range weights {
		weights[i] /= wSum
	}

	return KrigingResult{
		Value:        wVal / wSum,
		Variance:     math.NaN(),
		Weights:      weights,
		NumNeighbors: n,
	}, nil
}

func (ok *OrdinaryKriging) selectNeighbors(targetX, targetY float64, candidates []SamplePoint, maxN int) []SamplePoint {
	items := make([]distItem, len(candidates))
	for i, c := range candidates {
		dx := c.X - targetX
		dy := c.Y - targetY
		items[i] = distItem{pt: c, dist: math.Sqrt(dx*dx + dy*dy)}
	}

	quickSelectItems(items, 0, len(items)-1, maxN)

	result := make([]SamplePoint, maxN)
	for i := 0; i < maxN; i++ {
		result[i] = items[i].pt
	}
	return result
}

type distItem struct {
	pt   SamplePoint
	dist float64
}

func quickSelectItems(items []distItem, lo, hi, k int) {
	for lo < hi {
		pivot := partitionItems(items, lo, hi)
		if pivot == k {
			return
		} else if pivot < k {
			lo = pivot + 1
		} else {
			hi = pivot - 1
		}
	}
}

func partitionItems(items []distItem, lo, hi int) int {
	pivot := items[hi].dist
	i := lo
	for j := lo; j < hi; j++ {
		if items[j].dist <= pivot {
			items[i], items[j] = items[j], items[i]
			i++
		}
	}
	items[i], items[hi] = items[hi], items[i]
	return i
}

func solveLinearSystem(A []float64, b []float64, n int) ([]float64, error) {
	aug := make([]float64, n*(n+1))
	for i := 0; i < n; i++ {
		copy(aug[i*(n+1):i*(n+1)+n], A[i*n:i*n+n])
		aug[i*(n+1)+n] = b[i]
	}

	for col := 0; col < n; col++ {
		maxRow := col
		maxVal := math.Abs(aug[col*(n+1)+col])
		for row := col + 1; row < n; row++ {
			val := math.Abs(aug[row*(n+1)+col])
			if val > maxVal {
				maxVal = val
				maxRow = row
			}
		}

		if maxVal < MatrixTolerance {
			return nil, fmt.Errorf("singular matrix at column %d (pivot=%e)", col, maxVal)
		}

		if maxRow != col {
			for j := 0; j <= n; j++ {
				aug[col*(n+1)+j], aug[maxRow*(n+1)+j] = aug[maxRow*(n+1)+j], aug[col*(n+1)+j]
			}
		}

		pivot := aug[col*(n+1)+col]
		for j := col; j <= n; j++ {
			aug[col*(n+1)+j] /= pivot
		}

		for row := 0; row < n; row++ {
			if row == col {
				continue
			}
			factor := aug[row*(n+1)+col]
			for j := col; j <= n; j++ {
				aug[row*(n+1)+j] -= factor * aug[col*(n+1)+j]
			}
		}
	}

	x := make([]float64, n)
	for i := 0; i < n; i++ {
		x[i] = aug[i*(n+1)+n]
	}

	return x, nil
}

func (ok *OrdinaryKriging) BatchInterpolate(targets []SamplePoint, neighbors []SamplePoint) ([]KrigingResult, error) {
	results := make([]KrigingResult, len(targets))
	for i, t := range targets {
		r, err := ok.Interpolate(t.X, t.Y, neighbors)
		if err != nil {
			results[i] = KrigingResult{Value: math.NaN(), Variance: math.NaN()}
		} else {
			results[i] = r
		}
	}
	return results, nil
}
