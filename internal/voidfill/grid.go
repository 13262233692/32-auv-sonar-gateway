package voidfill

import (
	"math"
	"sync"

	"auv-sonar-gateway/internal/kriging"
	"auv-sonar-gateway/internal/model"
)

type GridCell struct {
	MinX, MinY, MaxX, MaxY float64
	Points                []model.Point3D
	CenterX, CenterY       float64
	MinDepth, MaxDepth     float64
	AvgDepth               float64
	HasData                bool
	IsEdge                 bool
	IsVoid                 bool
}

type GridConfig struct {
	CellSize      float64
	MinX, MinY    float64
	MaxX, MaxY    float64
	DepthThreshold float64
	EdgeDepthDiff  float64
	MinPointsPerCell int
}

func DefaultGridConfig(cellSize float64) GridConfig {
	return GridConfig{
		CellSize:         cellSize,
		DepthThreshold:   0.1,
		EdgeDepthDiff:    1.0,
		MinPointsPerCell: 1,
	}
}

type SpatialGrid struct {
	config    GridConfig
	numColsX  int
	numRowsY  int
	cells     []*GridCell
	mu        sync.RWMutex
}

func NewSpatialGrid(cfg GridConfig) *SpatialGrid {
	rangeX := cfg.MaxX - cfg.MinX
	rangeY := cfg.MaxY - cfg.MinY
	if rangeX <= 0 {
		rangeX = cfg.CellSize
	}
	if rangeY <= 0 {
		rangeY = cfg.CellSize
	}

	numColsX := int(math.Ceil(rangeX/cfg.CellSize)) + 1
	numRowsY := int(math.Ceil(rangeY/cfg.CellSize)) + 1

	cells := make([]*GridCell, numColsX*numRowsY)
	for i := 0; i < numColsX*numRowsY; i++ {
		cx := i / numRowsY
		cy := i % numRowsY
		cells[i] = &GridCell{
			MinX: cfg.MinX + float64(cx)*cfg.CellSize,
			MinY: cfg.MinY + float64(cy)*cfg.CellSize,
			MaxX: cfg.MinX + float64(cx+1)*cfg.CellSize,
			MaxY: cfg.MinY + float64(cy+1)*cfg.CellSize,
			MinDepth: math.Inf(1),
			MaxDepth: math.Inf(-1),
		}
		cells[i].CenterX = (cells[i].MinX + cells[i].MaxX) / 2.0
		cells[i].CenterY = (cells[i].MinY + cells[i].MaxY) / 2.0
	}

	return &SpatialGrid{
		config:   cfg,
		numColsX: numColsX,
		numRowsY: numRowsY,
		cells:    cells,
	}
}

func (g *SpatialGrid) Reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, c := range g.cells {
		c.Points = c.Points[:0]
		c.HasData = false
		c.IsEdge = false
		c.IsVoid = false
		c.MinDepth = math.Inf(1)
		c.MaxDepth = math.Inf(-1)
		c.AvgDepth = 0
	}
}

func (g *SpatialGrid) cellIndex(x, y float64) (int, bool) {
	if x < g.config.MinX || x >= g.config.MaxX || y < g.config.MinY || y >= g.config.MaxY {
		return -1, false
	}
	cx := int((x - g.config.MinX) / g.config.CellSize)
	cy := int((y - g.config.MinY) / g.config.CellSize)
	if cx < 0 || cx >= g.numColsX || cy < 0 || cy >= g.numRowsY {
		return -1, false
	}
	return cx*g.numRowsY + cy, true
}

func (g *SpatialGrid) AddPoint(p model.Point3D) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	idx, ok := g.cellIndex(p.X, p.Y)
	if !ok {
		return false
	}

	cell := g.cells[idx]
	cell.Points = append(cell.Points, p)
	cell.HasData = true

	if p.Depth < cell.MinDepth {
		cell.MinDepth = p.Depth
	}
	if p.Depth > cell.MaxDepth {
		cell.MaxDepth = p.Depth
	}
	return true
}

func (g *SpatialGrid) AddPoints(points []model.Point3D) int {
	count := 0
	for i := range points {
		if g.AddPoint(points[i]) {
			count++
		}
	}
	return count
}

func (g *SpatialGrid) ComputeStatistics() {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, c := range g.cells {
		if len(c.Points) == 0 {
			c.HasData = false
			c.IsVoid = true
			continue
		}

		var sum float64
		for i := range c.Points {
			sum += c.Points[i].Depth
		}
		c.AvgDepth = sum / float64(len(c.Points))
		c.HasData = len(c.Points) >= g.config.MinPointsPerCell
		c.IsVoid = !c.HasData
	}

	for cx := 0; cx < g.numColsX; cx++ {
		for cy := 0; cy < g.numRowsY; cy++ {
			idx := cx*g.numRowsY + cy
			cell := g.cells[idx]
			if !cell.IsVoid {
				continue
			}
			hasNeighborData := false
			for dx := -1; dx <= 1; dx++ {
				for dy := -1; dy <= 1; dy++ {
					if dx == 0 && dy == 0 {
						continue
					}
					nx, ny := cx+dx, cy+dy
					if nx >= 0 && nx < g.numColsX && ny >= 0 && ny < g.numRowsY {
						nidx := nx*g.numRowsY + ny
						if g.cells[nidx].HasData {
							hasNeighborData = true
							goto found
						}
					}
				}
			}
		found:
			if hasNeighborData {
				cell.IsVoid = true
			}
		}
	}

	for cx := 0; cx < g.numColsX; cx++ {
		for cy := 0; cy < g.numRowsY; cy++ {
			idx := cx*g.numRowsY + cy
			cell := g.cells[idx]
			if !cell.HasData {
				continue
			}
			hasVoidNeighbor := false
			for dx := -1; dx <= 1; dx++ {
				for dy := -1; dy <= 1; dy++ {
					if dx == 0 && dy == 0 {
						continue
					}
					nx, ny := cx+dx, cy+dy
					if nx >= 0 && nx < g.numColsX && ny >= 0 && ny < g.numRowsY {
						nidx := nx*g.numRowsY + ny
						if g.cells[nidx].IsVoid {
							hasVoidNeighbor = true
							goto edge
						}
					}
				}
			}
		edge:
			cell.IsEdge = hasVoidNeighbor
		}
	}
}

type VoidInfo struct {
	TotalCells       int
	DataCells        int
	VoidCells        int
	EdgeCells        int
	VoidCoveragePct  float64
	EdgeNeighborDist float64
}

func (g *SpatialGrid) AnalyzeVoids() VoidInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()

	info := VoidInfo{TotalCells: len(g.cells)}
	for _, c := range g.cells {
		if c.HasData {
			info.DataCells++
		}
		if c.IsVoid {
			info.VoidCells++
		}
		if c.IsEdge {
			info.EdgeCells++
		}
	}
	if info.TotalCells > 0 {
		info.VoidCoveragePct = float64(info.VoidCells) / float64(info.TotalCells) * 100.0
	}
	return info
}

func (g *SpatialGrid) GetVoidCells() []*GridCell {
	g.mu.RLock()
	defer g.mu.RUnlock()

	voids := make([]*GridCell, 0)
	for _, c := range g.cells {
		if c.IsVoid {
			voids = append(voids, c)
		}
	}
	return voids
}

func (g *SpatialGrid) GetEdgePoints(searchRadius int) []kriging.SamplePoint {
	g.mu.RLock()
	defer g.mu.RUnlock()

	points := make([]kriging.SamplePoint, 0)
	for _, c := range g.cells {
		if c.IsEdge && c.HasData {
			for i := range c.Points {
				points = append(points, kriging.SamplePoint{
					X:     c.Points[i].X,
					Y:     c.Points[i].Y,
					Value: c.Points[i].Depth,
				})
			}
		}
	}
	return points
}

func (g *SpatialGrid) GetNeighbors(cx, cy, radius int) []kriging.SamplePoint {
	g.mu.RLock()
	defer g.mu.RUnlock()

	points := make([]kriging.SamplePoint, 0)
	for dx := -radius; dx <= radius; dx++ {
		for dy := -radius; dy <= radius; dy++ {
			nx, ny := cx+dx, cy+dy
			if nx >= 0 && nx < g.numColsX && ny >= 0 && ny < g.numRowsY {
				idx := nx*g.numRowsY + ny
				cell := g.cells[idx]
				if cell.HasData {
					for i := range cell.Points {
						points = append(points, kriging.SamplePoint{
							X:     cell.Points[i].X,
							Y:     cell.Points[i].Y,
							Value: cell.Points[i].Depth,
						})
					}
				}
			}
		}
	}
	return points
}

func (g *SpatialGrid) CellCoord(x, y float64) (cx, cy int, ok bool) {
	if x < g.config.MinX || x >= g.config.MaxX || y < g.config.MinY || y >= g.config.MaxY {
		return 0, 0, false
	}
	cx = int((x - g.config.MinX) / g.config.CellSize)
	cy = int((y - g.config.MinY) / g.config.CellSize)
	ok = cx >= 0 && cx < g.numColsX && cy >= 0 && cy < g.numRowsY
	return
}

func (g *SpatialGrid) GetCell(cx, cy int) (*GridCell, bool) {
	if cx < 0 || cx >= g.numColsX || cy < 0 || cy >= g.numRowsY {
		return nil, false
	}
	return g.cells[cx*g.numRowsY+cy], true
}

func (g *SpatialGrid) SetCellFilled(cx, cy int, point model.Point3D) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	cell, ok := g.GetCell(cx, cy)
	if !ok {
		return false
	}
	cell.Points = append(cell.Points, point)
	cell.HasData = true
	cell.IsVoid = false
	cell.AvgDepth = point.Depth
	cell.MinDepth = point.Depth
	cell.MaxDepth = point.Depth
	return true
}

func (g *SpatialGrid) Bounds() (minX, minY, maxX, maxY float64) {
	return g.config.MinX, g.config.MinY, g.config.MaxX, g.config.MaxY
}

func (g *SpatialGrid) Dimensions() (colsX, rowsY int) {
	return g.numColsX, g.numRowsY
}

func (g *SpatialGrid) AllPoints() []model.Point3D {
	g.mu.RLock()
	defer g.mu.RUnlock()

	result := make([]model.Point3D, 0)
	for _, c := range g.cells {
		result = append(result, c.Points...)
	}
	return result
}

func (g *SpatialGrid) InterpolatedPoints() []model.Point3D {
	g.mu.RLock()
	defer g.mu.RUnlock()

	result := make([]model.Point3D, 0)
	for _, c := range g.cells {
		if c.HasData && len(c.Points) > 0 {
			p := model.Point3D{
				X:         c.CenterX,
				Y:         c.CenterY,
				Depth:     c.AvgDepth,
				Latitude:  c.Points[0].Latitude,
				Longitude: c.Points[0].Longitude,
				Intensity: c.Points[0].Intensity,
			}
			result = append(result, p)
		}
	}
	return result
}
