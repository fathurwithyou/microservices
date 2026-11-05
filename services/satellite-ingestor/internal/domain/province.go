package domain

import "math"

type province struct {
	Name      string
	Latitude  float64
	Longitude float64
}

// Province centroids cover all 38 provinces. Replace with the official BNPB
// reference table when it is distributed; the mapping only needs the nearest
// centroid.
var provinces = []province{
	{"Aceh", 4.30, 96.90},
	{"Sumatera Utara", 2.00, 99.00},
	{"Sumatera Barat", -0.70, 100.50},
	{"Riau", 0.50, 101.50},
	{"Kepulauan Riau", 3.00, 107.00},
	{"Jambi", -1.60, 103.00},
	{"Bengkulu", -3.50, 102.30},
	{"Sumatera Selatan", -3.30, 104.00},
	{"Kepulauan Bangka Belitung", -2.10, 106.10},
	{"Lampung", -4.80, 105.00},
	{"Banten", -6.50, 106.20},
	{"DKI Jakarta", -6.20, 106.85},
	{"Jawa Barat", -6.90, 107.60},
	{"Jawa Tengah", -7.30, 110.00},
	{"DI Yogyakarta", -7.80, 110.40},
	{"Jawa Timur", -7.50, 112.50},
	{"Bali", -8.40, 115.20},
	{"Nusa Tenggara Barat", -8.60, 117.50},
	{"Nusa Tenggara Timur", -9.00, 121.00},
	{"Kalimantan Barat", -0.10, 111.00},
	{"Kalimantan Tengah", -1.70, 113.40},
	{"Kalimantan Selatan", -3.00, 115.20},
	{"Kalimantan Timur", 1.00, 116.50},
	{"Kalimantan Utara", 3.00, 116.00},
	{"Sulawesi Utara", 1.00, 124.50},
	{"Gorontalo", 0.70, 122.50},
	{"Sulawesi Tengah", -1.50, 121.00},
	{"Sulawesi Barat", -2.50, 119.30},
	{"Sulawesi Selatan", -4.00, 120.00},
	{"Sulawesi Tenggara", -4.00, 122.00},
	{"Maluku", -3.00, 129.00},
	{"Maluku Utara", 1.50, 127.50},
	{"Papua", -4.00, 138.00},
	{"Papua Barat", -2.00, 133.00},
	{"Papua Barat Daya", -1.00, 132.00},
	{"Papua Tengah", -4.00, 136.00},
	{"Papua Pegunungan", -4.50, 139.50},
	{"Papua Selatan", -7.50, 139.50},
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
