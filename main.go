package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var f mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("TOPIC: %s\n", msg.Topic())
	fmt.Printf("MSG: %s\n", msg.Payload())
}

var (
	debug         = flag.Bool("debug", false, "when debug is set, eon will send json instead of sparkplugb messages")
	nodes         = flag.Int("nodes", 4, "number of edge of network to simulate")
	device        = flag.String("device", "device", "device name to simulate it will add at the end of the topic")
	group         = flag.String("namespace", "MyGroupId", "group to be used as part of the topic")
	metricPercent = flag.Int("metric-percent", 80, "from the total of messages this percent will be metrics and the rest events")
	server        = flag.String("server", "tcp://localhost:1883", "the server where the mock will publish the metrics")
)

func main() {
	flag.Parse()
	mqtt.DEBUG = log.New(os.Stdout, "", 0)
	mqtt.ERROR = log.New(os.Stdout, "", 0)
	opts := mqtt.NewClientOptions().AddBroker(*server).SetClientID("sparkplugbmock")

	opts.SetKeepAlive(60 * time.Second)
	// Set the message callback handler
	opts.SetDefaultPublishHandler(f)
	opts.SetPingTimeout(1 * time.Second)

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	// Disconnect
	defer c.Disconnect(250)

	var wg sync.WaitGroup
	wg.Add(*nodes)
	for edgeId := 1; edgeId < *nodes; edgeId++ {
		node := EdgeNode{EdgeID: edgeId, Device: *device, Client: c}
		go node.Publish(&wg, *metricPercent, *debug)
	}
	wg.Wait()
}
