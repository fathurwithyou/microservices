package application

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"sort"
	"time"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
)

const (
	clusterRadiusKm = 1.0
	clusterWindow   = 360 * time.Minute
	earthRadiusKm   = 6371.0088
)

type node struct {
	detection domain.Detection
	key       string
	at        time.Time
}

// Cluster groups connected detections into fires. Detections below the radius
// and time window form transitive components; lone detections are only counted.
func Cluster(detections []domain.Detection) (fires []domain.Fire, singles int) {
	nodes := make([]node, 0, len(detections))
	for _, d := range detections {
		if d.Ignored() {
			continue
		}
		at, err := d.Time()
		if err != nil {
			singles++
			continue
		}
		nodes = append(nodes, node{detection: d, key: d.Key(), at: at})
	}

	parent := make([]int, len(nodes))
	for i := range parent {
		parent[i] = i
	}
	var find func(int) int
	find = func(i int) int {
		if parent[i] != i {
			parent[i] = find(parent[i])
		}
		return parent[i]
	}
	for i := range nodes {
		for j := i + 1; j < len(nodes); j++ {
			if connected(nodes[i], nodes[j]) {
				parent[find(i)] = find(j)
			}
		}
	}

	groups := map[int][]node{}
	for i := range nodes {
		root := find(i)
		groups[root] = append(groups[root], nodes[i])
	}

	for _, group := range groups {
		if len(group) < 2 {
			singles++
			continue
		}
		fires = append(fires, newFire(group))
	}
	sort.Slice(fires, func(i, j int) bool {
		return fires[i].SourceRefID < fires[j].SourceRefID
	})
	return fires, singles
}

func connected(a, b node) bool {
	if a.at.Sub(b.at).Abs() > clusterWindow {
		return false
	}
	return distanceKm(a.detection.Latitude, a.detection.Longitude,
		b.detection.Latitude, b.detection.Longitude) <= clusterRadiusKm
}

func distanceKm(lat1, lon1, lat2, lon2 float64) float64 {
	const rad = math.Pi / 180
	dLat := (lat2 - lat1) * rad
	dLon := (lon2 - lon1) * rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusKm * math.Asin(math.Sqrt(a))
}

func newFire(group []node) domain.Fire {
	sort.Slice(group, func(i, j int) bool {
		if !group[i].at.Equal(group[j].at) {
			return group[i].at.Before(group[j].at)
		}
		return group[i].key < group[j].key
	})

	keys := make([]string, len(group))
	satellites := make([]domain.Satellite, 0, len(group))
	seenSatellite := map[domain.Satellite]bool{}
	maxConfidence := group[0].detection.Confidence
	var totalFRP, latitude, longitude float64
	for i, n := range group {
		keys[i] = n.key
		latitude += n.detection.Latitude
		longitude += n.detection.Longitude
		if !seenSatellite[n.detection.Satellite] {
			seenSatellite[n.detection.Satellite] = true
			satellites = append(satellites, n.detection.Satellite)
		}
		if n.detection.Confidence.Rank() > maxConfidence.Rank() {
			maxConfidence = n.detection.Confidence
		}
		totalFRP += n.detection.FRP
	}
	sort.Slice(satellites, func(i, j int) bool { return satellites[i] < satellites[j] })
	sort.Strings(keys)

	count := float64(len(group))
	sum := sha256.Sum256([]byte(group[0].key))
	return domain.Fire{
		SourceRefID:    "fire-" + hex.EncodeToString(sum[:]),
		Status:         domain.StatusActive,
		OccurredAt:     group[0].at,
		AreaName:       domain.NearestProvince(latitude/count, longitude/count),
		Latitude:       latitude / count,
		Longitude:      longitude / count,
		DetectionCount: len(group),
		Satellites:     satellites,
		MaxConfidence:  maxConfidence,
		TotalFRP:       totalFRP,
		DetectionKeys:  keys,
		Severity:       domain.SeverityFor(len(group), totalFRP),
	}
}
