package voidfill

import (
	"fmt"
	"log"
	"sync"
	"time"

	"auv-sonar-gateway/internal/kriging"
	"auv-sonar-gateway/internal/model"
)

type FillerConfig struct {
	CellSize             float64
	SearchRadius         int
	MaxKrigingNeighbors  int
	MaxVoidCellsPerPatch int
	MinPointsForKriging  int
	VariogramNugget      float64
	VariogramSill        float64
	VariogramRange       float64
	VariogramAnisoAngle  float64
	VariogramAnisoRatio  float64
	EnableFallbackIDW    bool
	AutoFitVariogram     bool
	MaxInterpolatePerTick int
}

func DefaultFillerConfig() FillerConfig {
	return FillerConfig{
		CellSize:             0.5,
		SearchRadius:         4,
		MaxKrigingNeighbors:  kriging.MaxKrigingNeighbors,
		MaxVoidCellsPerPatch: 2048,
		MinPointsForKriging:  kriging.MinKrigingNeighbors,
		VariogramNugget:      0.01,
		VariogramSill:        1.0,
		VariogramRange:       50.0,
		VariogramAnisoAngle:  0.0,
		VariogramAnisoRatio:  1.0,
		EnableFallbackIDW:    true,
		AutoFitVariogram:     true,
		MaxInterpolatePerTick: 256,
	}
}

type VoidFiller struct {
	config    FillerConfig
	kriging   *kriging.OrdinaryKriging
	variogram kriging.VariogramParams
	grid      *SpatialGrid
	mu        sync.RWMutex

	stats      FillerStats
	lastTick    time.Time
	lastPatchID uint64
}

type FillerStats struct {
	GridsProcessed       uint64
	VoidCellsDetected    uint64
	VoidCellsFilled      uint64
	VoidCellsSkipped     uint64
	KrigingInterpolations uint64
	IDWFallbacks         uint64
	GridCellsTotal       uint64
	GridCellsWithData    uint64
	AvgInterpolateUs     float64
	MinInterpolateUs     float64
	MaxInterpolateUs     float64
}

type PatchFillResult struct {
	PatchID         uint64
	VoidCellsInPatch int
	FilledCells      int
	SkippedCells     int
	KrigingCount     int
	IDWCount         int
	ElapsedUs        int64
	FilledPoints     []model.Point3D
}

func NewVoidFiller(cfg FillerConfig) *VoidFiller {
	params := kriging.VariogramParams{
		Model:      kriging.ModelSpherical,
		Nugget:     cfg.VariogramNugget,
		Sill:       cfg.VariogramSill,
		Range:      cfg.VariogramRange,
		AnisoAngle: cfg.VariogramAnisoAngle,
		AnisoRatio: cfg.VariogramAnisoRatio,
	}

	return &VoidFiller{
		config:    cfg,
		kriging:   kriging.NewOrdinaryKriging(params),
		variogram: params,
		lastTick:  time.Now(),
	}
}

func (vf *VoidFiller) SetGrid(g *SpatialGrid) {
	vf.mu.Lock()
	defer vf.mu.Unlock()
	vf.grid = g
}

func (vf *VoidFiller) SetVariogram(params kriging.VariogramParams) {
	vf.mu.Lock()
	defer vf.mu.Unlock()
	vf.variogram = params
	vf.kriging = kriging.NewOrdinaryKriging(params)
}

func (vf *VoidFiller) UpdateVariogramFromGrid() bool {
	vf.mu.Lock()
	defer vf.mu.Unlock()

	if vf.grid == nil {
		return false
	}

	edgePoints := vf.grid.GetEdgePoints(vf.config.SearchRadius)
	if len(edgePoints) < 30 {
		return false
	}

	sampleSize := len(edgePoints)
	if sampleSize > 200 {
		sampleSize = 200
	}

	samples := make([]kriging.SamplePoint, sampleSize)
	step := len(edgePoints) / sampleSize
	for i := 0; i < sampleSize; i++ {
		samples[i] = edgePoints[i*step]
	}

	params := kriging.FitVariogram(samples, kriging.ModelSpherical,
		vf.config.VariogramRange*2.0, 20)

	params.AnisoAngle = vf.variogram.AnisoAngle
	params.AnisoRatio = vf.variogram.AnisoRatio

	vf.variogram = params
	vf.kriging = kriging.NewOrdinaryKriging(params)

	return true
}

func (vf *VoidFiller) GetStats() FillerStats {
	vf.mu.RLock()
	defer vf.mu.RUnlock()
	return vf.stats
}

func (vf *VoidFiller) GetVariogram() kriging.VariogramParams {
	vf.mu.RLock()
	defer vf.mu.RUnlock()
	return vf.variogram
}

func (vf *VoidFiller) ProcessAndFill(points []model.Point3D,
	computeBounds func([]model.Point3D) (minX, minY, maxX, maxY float64)) (*PatchFillResult, []model.Point3D, error) {

	vf.mu.Lock()
	if len(points) == 0 {
		vf.mu.Unlock()
		return nil, nil, fmt.Errorf("no points to process")
	}

	if computeBounds == nil {
		computeBounds = defaultBounds
	}

	minX, minY, maxX, maxY := computeBounds(points)

	if vf.grid == nil || outOfBounds(vf.grid, minX, minY, maxX, maxY) {
		padding := vf.config.CellSize * float64(vf.config.SearchRadius+1)
		cfg := vf.gridConfig(minX-padding, minY-padding, maxX+padding, maxY+padding)
		vf.grid = NewSpatialGrid(cfg)
	} else {
		vf.grid.Reset()
	}

	vf.grid.AddPoints(points)
	vf.grid.ComputeStatistics()

	voidInfo := vf.grid.AnalyzeVoids()

	vf.stats.GridsProcessed++
	vf.stats.GridCellsTotal += uint64(voidInfo.TotalCells)
	vf.stats.GridCellsWithData += uint64(voidInfo.DataCells)
	vf.stats.VoidCellsDetected += uint64(voidInfo.VoidCells)

	if voidInfo.VoidCells == 0 {
		result := vf.grid.InterpolatedPoints()
		vf.mu.Unlock()
		return &PatchFillResult{VoidCellsInPatch: 0, FilledPoints: result}, result, nil
	}

	if vf.config.AutoFitVariogram {
		vf.UpdateVariogramFromGrid()
	}

	result, err := vf.fillVoidsLocked(voidInfo)
	vf.mu.Unlock()

	if err != nil {
		log.Printf("void fill warning: %v", err)
	}

	allPoints := vf.grid.InterpolatedPoints()
	return result, allPoints, err
}

func (vf *VoidFiller) fillVoidsLocked(info VoidInfo) (*PatchFillResult, error) {
	start := time.Now()

	patchResult := &PatchFillResult{
		PatchID:         vf.lastPatchID + 1,
		VoidCellsInPatch: info.VoidCells,
	}
	vf.lastPatchID++

	voidCells := vf.grid.GetVoidCells()
	colsX, rowsY := vf.grid.Dimensions()

	totalFilled := 0
	totalInterpolated := 0
	maxInterpolations := vf.config.MaxInterpolatePerTick
	if maxInterpolations <= 0 {
		maxInterpolations = len(voidCells)
	}

	minX, minY, _, _ := vf.grid.Bounds()
	invCellSize := 1.0 / vf.config.CellSize

	for _, voidCell := range voidCells {
		if totalInterpolated >= maxInterpolations {
			patchResult.SkippedCells = patchResult.VoidCellsInPatch - totalFilled
			vf.stats.VoidCellsSkipped += uint64(patchResult.SkippedCells)
			break
		}

		cx := int((voidCell.CenterX - minX) * invCellSize)
		cy := int((voidCell.CenterY - minY) * invCellSize)
		if cx < 0 {
			cx = 0
		}
		if cy < 0 {
			cy = 0
		}
		if cx >= colsX {
			cx = colsX - 1
		}
		if cy >= rowsY {
			cy = rowsY - 1
		}

		neighbors := vf.grid.GetNeighbors(cx, cy, vf.config.SearchRadius)
		if len(neighbors) < vf.config.MinPointsForKriging {
			patchResult.SkippedCells++
			continue
		}

		interpStart := time.Now()

		result, err := vf.kriging.Interpolate(voidCell.CenterX, voidCell.CenterY, neighbors)

		elapsedUs := time.Since(interpStart).Microseconds()
		totalInterpolated++

		if err != nil {
			if vf.config.EnableFallbackIDW {
				idwResult, idwErr := vf.fallbackIDW(voidCell.CenterX, voidCell.CenterY, neighbors)
				if idwErr != nil {
					patchResult.SkippedCells++
					vf.stats.VoidCellsSkipped++
					continue
				}
				result = idwResult
				patchResult.IDWCount++
				vf.stats.IDWFallbacks++
			} else {
				patchResult.SkippedCells++
				vf.stats.VoidCellsSkipped++
				continue
			}
		} else {
			patchResult.KrigingCount++
			vf.stats.KrigingInterpolations++
		}

		if vf.stats.MinInterpolateUs == 0 || float64(elapsedUs) < vf.stats.MinInterpolateUs {
			vf.stats.MinInterpolateUs = float64(elapsedUs)
		}
		if float64(elapsedUs) > vf.stats.MaxInterpolateUs {
			vf.stats.MaxInterpolateUs = float64(elapsedUs)
		}
		vf.stats.AvgInterpolateUs = (vf.stats.AvgInterpolateUs*float64(totalInterpolated-1) + float64(elapsedUs)) / float64(totalInterpolated)

		filledPoint := model.Point3D{
			X:         voidCell.CenterX,
			Y:         voidCell.CenterY,
			Depth:     result.Value,
			Intensity: 128,
			TimestampUS: uint64(time.Now().UnixNano() / 1000),
		}

		if len(neighbors) > 0 {
			filledPoint.Latitude = neighbors[0].X
			filledPoint.Longitude = neighbors[0].Y
		}

		vf.grid.SetCellFilled(cx, cy, filledPoint)
		patchResult.FilledPoints = append(patchResult.FilledPoints, filledPoint)
		patchResult.FilledCells++
		totalFilled++
	}

	patchResult.ElapsedUs = time.Since(start).Microseconds()
	vf.stats.VoidCellsFilled += uint64(totalFilled)

	return patchResult, nil
}

func (vf *VoidFiller) fallbackIDW(x, y float64, neighbors []kriging.SamplePoint) (kriging.KrigingResult, error) {
	n := len(neighbors)
	if n == 0 {
		return kriging.KrigingResult{}, fmt.Errorf("no neighbors for IDW")
	}

	var wSum, wVal float64
	for i := 0; i < n; i++ {
		dx := neighbors[i].X - x
		dy := neighbors[i].Y - y
		dist := dx*dx + dy*dy
		if dist < 1e-12 {
			return kriging.KrigingResult{
				Value:        neighbors[i].Value,
				Variance:     0,
				NumNeighbors: 1,
			}, nil
		}
		w := 1.0 / (dist * dist)
		wSum += w
		wVal += w * neighbors[i].Value
	}

	if wSum < 1e-12 {
		return kriging.KrigingResult{}, fmt.Errorf("IDW weight sum zero")
	}

	return kriging.KrigingResult{
		Value:        wVal / wSum,
		Variance:     0,
		NumNeighbors: n,
	}, nil
}

func (vf *VoidFiller) gridConfig(minX, minY, maxX, maxY float64) GridConfig {
	return GridConfig{
		CellSize:         vf.config.CellSize,
		MinX:             minX,
		MinY:             minY,
		MaxX:             maxX,
		MaxY:             maxY,
		DepthThreshold:   0.1,
		EdgeDepthDiff:    1.0,
		MinPointsPerCell: 1,
	}
}

func defaultBounds(points []model.Point3D) (minX, minY, maxX, maxY float64) {
	minX = points[0].X
	minY = points[0].Y
	maxX = points[0].X
	maxY = points[0].Y

	for i := 1; i < len(points); i++ {
		if points[i].X < minX {
			minX = points[i].X
		}
		if points[i].Y < minY {
			minY = points[i].Y
		}
		if points[i].X > maxX {
			maxX = points[i].X
		}
		if points[i].Y > maxY {
			maxY = points[i].Y
		}
	}
	return
}

func outOfBounds(g *SpatialGrid, minX, minY, maxX, maxY float64) bool {
	gMinX, gMinY, gMaxX, gMaxY := g.Bounds()
	return minX < gMinX || minY < gMinY || maxX > gMaxX || maxY > gMaxY
}

func (vf *VoidFiller) FillFrames(frames []*model.PointCloudFrame) ([]model.Point3D, []PatchFillResult, error) {
	allPoints := make([]model.Point3D, 0)
	for _, f := range frames {
		allPoints = append(allPoints, f.Points...)
	}

	if len(allPoints) == 0 {
		return nil, nil, fmt.Errorf("no points in frames")
	}

	_, filled, err := vf.ProcessAndFill(allPoints, nil)
	if err != nil {
		return nil, nil, err
	}

	results := make([]PatchFillResult, 1)
	voidInfo := vf.grid.AnalyzeVoids()
	results[0].VoidCellsInPatch = voidInfo.VoidCells
	results[0].FilledCells = len(filled) - len(allPoints)
	results[0].FilledPoints = filled

	return filled, results, nil
}
