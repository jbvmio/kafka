package kafka

import (
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	metrics "github.com/rcrowley/go-metrics"
)

// Metric Types:
const (
	meterMetricType = `meter`
	histoMetricType = `histogram`
)

type metricType map[string]bool

var meterMetric = metricType{meterMetricType: true}
var histoMetric = metricType{histoMetricType: true}

// RawMetric contains the raw metric measurements and values.
type RawMetric struct {
	Measurement string
	Values      map[string]interface{}
	Type        metricType
}

func (m *RawMetric) Update(r *metrics.Registry) {
	all := *r
	allMetrics := all.GetAll()
	m.Values = allMetrics[m.Measurement]
	m.getMetricType()
}

func (m *RawMetric) getMetricType() {
	if m.Type == nil {
		switch {
		case m.Measurement == "" || m.Values == nil:
			Warnf("metric not initialized!")
			break
		default:
			m.Type = make(map[string]bool, 1)
			for k := range m.Values {
				switch {
				case strings.Contains(k, "rate"):
					m.Type = meterMetric
					break
				case !strings.Contains(k, "rate") && !strings.Contains(k, "count"):
					m.Type = histoMetric
					break
				}
			}
		}
	}
}

// KafkaMetric represents a metric measurement from Kafka.
type KafkaMetric interface {
	GetType() string
	IsMeter() bool
	IsHisto() bool
}

// MeterMetric contains Meter values.
type MeterMetric struct {
	Measurement string
	Type        string
	Count       uint32
	OneRate     float32
	FiveRate    float32
	FifteenRate float32
	MeanRate    float32
}

// GetType returns the metric type.
func (m *MeterMetric) GetType() string {
	return m.Type
}

// IsMeter returns true if the metric type is a meter.
func (m *MeterMetric) IsMeter() bool {
	return m.Type == meterMetricType
}

// IsHisto returns true if the metric type is a histogram.
func (m *MeterMetric) IsHisto() bool {
	return m.Type == histoMetricType
}

// HistoMetric contains Histogram values.
type HistoMetric struct {
	Measurement string
	Type        string
	Count       uint32
	Min         uint32
	Max         uint32
	SeventyFive float32
	NinetyNine  float32
}

// GetType returns the metric type.
func (m *HistoMetric) GetType() string {
	return m.Type
}

// IsMeter returns true if the metric type is a meter.
func (m *HistoMetric) IsMeter() bool {
	return m.Type == meterMetricType
}

// IsHisto returns true if the metric type is a histogram.
func (m *HistoMetric) IsHisto() bool {
	return m.Type == histoMetricType
}

const (
	metricsReservoirSize = 1028
	metricsAlphaFactor   = 0.015
)

func getOrRegisterHistogram(name string, r metrics.Registry) metrics.Histogram {
	return r.GetOrRegister(name, func() metrics.Histogram {
		return metrics.NewHistogram(metrics.NewExpDecaySample(metricsReservoirSize, metricsAlphaFactor))
	}).(metrics.Histogram)
}

func getMetricNameForBroker(name string, broker *sarama.Broker) string {
	// Use broker id like the Java client as it does not contain '.' or ':' characters that
	// can be interpreted as special character by monitoring tool (e.g. Graphite)
	return fmt.Sprintf(name+"-for-broker-%d", broker.ID())
}

func getOrRegisterBrokerMeter(name string, broker *sarama.Broker, r metrics.Registry) metrics.Meter {
	return metrics.GetOrRegisterMeter(getMetricNameForBroker(name, broker), r)
}

func getOrRegisterBrokerHistogram(name string, broker *sarama.Broker, r metrics.Registry) metrics.Histogram {
	return getOrRegisterHistogram(getMetricNameForBroker(name, broker), r)
}

func getMetricNameForTopic(name string, topic string) string {
	// Convert dot to _ since reporters like Graphite typically use dot to represent hierarchy
	// cf. KAFKA-1902 and KAFKA-2337
	return fmt.Sprintf(name+"-for-topic-%s", strings.Replace(topic, ".", "_", -1))
}

func getOrRegisterTopicMeter(name string, topic string, r metrics.Registry) metrics.Meter {
	return metrics.GetOrRegisterMeter(getMetricNameForTopic(name, topic), r)
}

func getOrRegisterTopicHistogram(name string, topic string, r metrics.Registry) metrics.Histogram {
	return getOrRegisterHistogram(getMetricNameForTopic(name, topic), r)
}
