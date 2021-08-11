package main

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "github.com/IHI-Energy-Storage/sparkpluggw/Sparkplug"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
)

type EdgeNode struct {
	EdgeID  int
	Seq     int
	Device string
	Client  mqtt.Client
}

type Sample struct {
	name     string
	dataType string
	value    interface{}
}

var jsonMarshaller = jsonpb.Marshaler{Indent: "  "}

func (e *EdgeNode) Publish(wg *sync.WaitGroup, metricPercent int, debug bool) {
	defer wg.Done()
	for {
		<-time.Tick(10 * time.Second)
		timeStamp := uint64(time.Now().UnixNano() / int64(time.Millisecond))
			var p *pb.Payload

			n := rand.Intn(100)
			if n < metricPercent {
				p = metricsPayload(timeStamp, uint64(e.Seq))
			} else {
				p = eventPayload(timeStamp, uint64(e.Seq))
			}
			var toPublish []byte
			var err error
			if debug {
				var buf bytes.Buffer
				err = jsonMarshaller.Marshal(&buf, p)
				toPublish = buf.Bytes()
			} else {
				toPublish, err = proto.Marshal(p)
			}

			if err != nil {
				log.Printf("err while marshalling message: %s", err)
				continue
			}
			token := e.Client.Publish(fmt.Sprintf("spBv1.0/%s/DDATA/%d/%s",*group ,e.EdgeID, *device), 0, false, toPublish)
			token.Wait()
			e.Seq = (e.Seq + 1) % 256
		}
}

// metricsPayload slice always start with scan rate ms metric.
func metricsPayload(timestamp, seq uint64) *pb.Payload {
	var p pb.Payload
	p.Timestamp = &timestamp
	p.Seq = &seq

	// The metrics payload must include scanRateMS
	p.Metrics = append(p.Metrics, scanRateMS(timestamp))

	// randomly we are going to decide how many metrics to send.
	for i := 0; i < len(metricSamples); i++ {
		p.Metrics = append(p.Metrics, payloadMetric(timestamp, i, metricSamples))
	}

	return &p
}

// eventPayload requires only one event in the payload so
//  it just picks a number and use it to select what sample send as event.
func eventPayload(timestamp, seq uint64) *pb.Payload {
	var p pb.Payload
	p.Timestamp = &timestamp
	p.Seq = &seq

	p.Metrics = append(p.Metrics, payloadMetric(timestamp, 0, eventSamples))
	return &p
}

// scanRateMS returns a well known metric
func scanRateMS(timestamp uint64) *pb.Payload_Metric {
	name := "Device Control/Scan Rate ms"
	dataType := dataTypeValue["Int32"]

	pm := pb.Payload_Metric{
		Name:      &name,
		Timestamp: &timestamp,
		Datatype:  &dataType,
		Value:     &pb.Payload_Metric_IntValue{IntValue: 6000},
	}
	return &pm
}

// payloadMetric takes a sample and return a protocol buffer object
func payloadMetric(timestamp uint64, sample int, samples []Sample) *pb.Payload_Metric {
	var pm pb.Payload_Metric

	pm.Timestamp = &timestamp
	pm.Name = &samples[sample].name
	dType := dataTypeValue[samples[sample].dataType]
	pm.Datatype = &dType

	switch samples[sample].dataType {
	case "Boolean":
		pm.Value = &pb.Payload_Metric_BooleanValue{BooleanValue: samples[sample].value.(bool)}
	case "String":
		pm.Value = &pb.Payload_Metric_StringValue{StringValue: samples[sample].value.(string)}
	case "Float":
		pm.Value = &pb.Payload_Metric_FloatValue{FloatValue: float32(samples[sample].value.(float64))}
	case "Double":
		pm.Value = &pb.Payload_Metric_DoubleValue{DoubleValue: samples[sample].value.(float64)}
	case "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64":
		pm.Value = &pb.Payload_Metric_IntValue{IntValue: uint32(samples[sample].value.(int))}
	}
	return &pm
}

var metricSamples = []Sample{
	{name: "metric1", dataType: "Boolean", value: false},
	{name: "metric2", dataType: "Int8", value: 34},
	{name: "metric3", dataType: "Int8", value: 100},
	{name: "metric4", dataType: "Float", value: 24.0},
	{name: "metric5", dataType: "Int32", value: 84692},
	{name: "metric6", dataType: "UInt8", value: 99},
	{name: "metric7", dataType: "UInt16", value: 118},
	{name: "metric8", dataType: "UInt8", value: 0},
	{name: "metric9", dataType: "UInt32", value: 5},
	{name: "metric10", dataType: "Int16", value: 36},
}

// we are going to pretend that boolean metrics are events.
var eventSamples = []Sample{
	{name: "event", dataType: "Boolean", value: false},
}

// Enum value maps for DataType.
var (
	dataTypeName = map[uint32]string{
		0:  "Unknown",
		1:  "Int8",
		2:  "Int16",
		3:  "Int32",
		4:  "Int64",
		5:  "UInt8",
		6:  "UInt16",
		7:  "UInt32",
		8:  "UInt64",
		9:  "Float",
		10: "Double",
		11: "Boolean",
		12: "String",
		13: "DateTime",
		14: "Text",
		15: "UUID",
		16: "DataSet",
		17: "Bytes",
		18: "File",
		19: "Template",
		20: "PropertySet",
		21: "PropertySetList",
		22: "Int8Array",
		23: "Int16Array",
		24: "Int32Array",
		25: "Int64Array",
		26: "UInt8Array",
		27: "UInt16Array",
		28: "UInt32Array",
		29: "UInt64Array",
		30: "FloatArray",
		31: "DoubleArray",
		32: "BooleanArray",
		33: "StringArray",
		34: "DateTimeArray",
	}
	dataTypeValue = map[string]uint32{
		"Unknown":         0,
		"Int8":            1,
		"Int16":           2,
		"Int32":           3,
		"Int64":           4,
		"UInt8":           5,
		"UInt16":          6,
		"UInt32":          7,
		"UInt64":          8,
		"Float":           9,
		"Double":          10,
		"Boolean":         11,
		"String":          12,
		"DateTime":        13,
		"Text":            14,
		"UUID":            15,
		"DataSet":         16,
		"Bytes":           17,
		"File":            18,
		"Template":        19,
		"PropertySet":     20,
		"PropertySetList": 21,
		"Int8Array":       22,
		"Int16Array":      23,
		"Int32Array":      24,
		"Int64Array":      25,
		"UInt8Array":      26,
		"UInt16Array":     27,
		"UInt32Array":     28,
		"UInt64Array":     29,
		"FloatArray":      30,
		"DoubleArray":     31,
		"BooleanArray":    32,
		"StringArray":     33,
		"DateTimeArray":   34,
	}
)
