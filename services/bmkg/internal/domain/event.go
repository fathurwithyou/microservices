package domain

type SeismicEvent struct {
	EventID          string  `json:"event_id"`
	Magnitude        float64 `json:"magnitude"`
	DepthKM          float64 `json:"depth_km"`
	EpicenterLat     float64 `json:"epicenter_lat"`
	EpicenterLon     float64 `json:"epicenter_lon"`
	RegionName       string  `json:"region_name"`
	OccurredAt       string  `json:"occurred_at"`
	PotentialTsunami bool    `json:"potential_tsunami"`
}
