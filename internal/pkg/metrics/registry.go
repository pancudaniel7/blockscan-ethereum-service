package metrics

import "github.com/prometheus/client_golang/prometheus"

var reg prometheus.Registerer = prometheus.DefaultRegisterer

func Registerer() prometheus.Registerer { return reg }

func UseRegisterer(r prometheus.Registerer) {
	if r != nil {
		reg = r
	}
}
