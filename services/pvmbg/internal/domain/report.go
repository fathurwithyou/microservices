package domain

type VolcanicReport map[string]any

func (r VolcanicReport) Clone() VolcanicReport {
	copy := make(VolcanicReport, len(r)+1)
	for key, value := range r {
		copy[key] = value
	}
	return copy
}
