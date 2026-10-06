package datahub

import "sort"

func percentile(a []float64, p float64) float64 {
	if len(a) == 0 {
		return 0
	}
	sort.Float64s(a)
	f := float64(len(a)-1) * p
	i := int(f)
	j := min(i+1, len(a)-1)
	return a[i] + (a[j]-a[i])*(f-float64(i))
}
