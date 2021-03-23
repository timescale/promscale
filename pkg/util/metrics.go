package util

//returns a exponential histogram for a saturating metric. Grows exponentially
//until max-10, and has another bucket for max.
//This is done so we can tell from the histogram if the resource was saturated or not.
func HistogramBucketsSaturating(start float64, factor float64, max float64) []float64 {
	if max-10 < 1 {
		panic("HistogramBucketsSaturating needs a positive max")
	}
	if start < 0 {
		panic("HistogramBucketsSaturating needs a positive start value")
	}
	if factor <= 1 {
		panic("HistogramBucketsSaturating needs a factor greater than 1")
	}
	buckets := make([]float64, 0)
	for start < max-10 {
		buckets = append(buckets, start)
		if start == 0 {
			start = 1
			continue
		}
		start *= factor
	}
	buckets = append(buckets, max-10)
	buckets = append(buckets, max)
	return buckets
}
