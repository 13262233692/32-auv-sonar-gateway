package model

type SSPEntry struct {
	DepthM    float64
	SoundSpeedMs float64
}

type SoundSpeedProfile struct {
	Entries    []SSPEntry
	TimestampUS uint64
}

func (s *SoundSpeedProfile) Interpolate(depth float64) float64 {
	n := len(s.Entries)
	if n == 0 {
		return 1500.0
	}
	if depth <= s.Entries[0].DepthM {
		return s.Entries[0].SoundSpeedMs
	}
	if depth >= s.Entries[n-1].DepthM {
		return s.Entries[n-1].SoundSpeedMs
	}
	for i := 0; i < n-1; i++ {
		if depth >= s.Entries[i].DepthM && depth < s.Entries[i+1].DepthM {
			ratio := (depth - s.Entries[i].DepthM) / (s.Entries[i+1].DepthM - s.Entries[i].DepthM)
			return s.Entries[i].SoundSpeedMs + ratio*(s.Entries[i+1].SoundSpeedMs-s.Entries[i].SoundSpeedMs)
		}
	}
	return 1500.0
}

func (s *SoundSpeedProfile) MeanSpeed() float64 {
	if len(s.Entries) == 0 {
		return 1500.0
	}
	var sum float64
	for _, e := range s.Entries {
		sum += e.SoundSpeedMs
	}
	return sum / float64(len(s.Entries))
}
