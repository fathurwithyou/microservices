package domain

import "math"

type province struct {
	Name      string
	Latitude  float64
	Longitude float64
}

// A compact BNPB reference set for the provinces most exposed to wildfire.
// Replace with the full static table when the official reference is available.
var provinces = []province{
	{"Aceh", 4.695, 96.749},
	{"Sumatera Utara", 2.115, 99.545},
	{"Riau", 0.293, 101.706},
	{"Jambi", -1.610, 103.613},
	{"Sumatera Selatan", -3.319, 103.914},
	{"Kalimantan Barat", -0.278, 111.475},
	{"Kalimantan Tengah", -1.681, 113.382},
	{"Kalimantan Selatan", -3.092, 115.283},
	{"Kalimantan Timur", 1.681, 116.419},
	{"Kalimantan Utara", 3.073, 116.041},
	{"Papua", -4.269, 138.080},
}

// NearestProvince returns the province centroid closest to a fire centre.
func NearestProvince(latitude, longitude float64) string {
	best := ""
	bestDistance := math.MaxFloat64
	for _, item := range provinces {
		distance := distanceKm(latitude, longitude, item.Latitude, item.Longitude)
		if distance < bestDistance {
			bestDistance = distance
			best = item.Name
		}
	}
	return best
}

func distanceKm(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadiusKm = 6371.0088
	const rad = math.Pi / 180
	dLat := (lat2 - lat1) * rad
	dLon := (lon2 - lon1) * rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusKm * math.Asin(math.Sqrt(a))
}
