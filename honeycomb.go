package honeycomb

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gliderlabs/logspout/router"
	"github.com/honeycombio/libhoney-go"
)

const (
	DefaultHoneycombAPIURL = "https://api.honeycomb.io"
	DefaultSampleRate      = 1
	Version                = "v0.0.3"
)

func init() {
	router.AdapterFactories.Register(NewHoneycombAdapter, "honeycomb")
	libhoney.UserAgentAddition = "logspout-honeycomb"
}

type item struct {
	value      string
	lastAccess int64
}
type TTLMap struct {
	m map[string]*item
	l sync.Mutex
}

func NewTTLMap(ln int, maxTTL int) (m *TTLMap) {
	m = &TTLMap{m: make(map[string]*item, ln)}
	go func() {
		for now := range time.Tick(time.Second) {
			m.l.Lock()
			for k, v := range m.m {
				if now.Unix() - v.lastAccess > int64(maxTTL) {
					delete(m.m, k)
				}
			}
			m.l.Unlock()
		}
	}()
	return
}

func (m *TTLMap) Len() int {
	return len(m.m)
}

func (m *TTLMap) Put(k, v string) {
	m.l.Lock()
	it, ok := m.m[k]
	if !ok {
		it = &item{value: v}
		m.m[k] = it
	}
	it.lastAccess = time.Now().Unix()
	m.l.Unlock()
}

func (m *TTLMap) Get(k string) (v string) {
	m.l.Lock()
	if it, ok := m.m[k]; ok {
		v = it.value
		it.lastAccess = time.Now().Unix()
	}
	m.l.Unlock()
	return

}

// HoneycombAdapter is an adapter that streams JSON to Logstash.
type HoneycombAdapter struct {
	conn  net.Conn
	route *router.Route
}

// NewHoneycombAdapter creates a HoneycombAdapter
func NewHoneycombAdapter(route *router.Route) (router.LogAdapter, error) {
	writeKey := route.Options["writeKey"]
	if writeKey == "" {
		writeKey = os.Getenv("HONEYCOMB_WRITE_KEY")
	}
	if writeKey == "" {
		log.Fatal("Must provide Honeycomb WriteKey.")
		return nil, errors.New("Honeycomb 'WriteKey' was not provided.")
	}

	dataset := route.Options["dataset"]
	if dataset == "" {
		dataset = os.Getenv("HONEYCOMB_DATASET")
	}
	if dataset == "" {
		log.Fatal("Must provide Honeycomb Dataset.")
		return nil, errors.New("Honeycomb 'Dataset' was not provided.")
	}

	honeycombAPIURL := route.Options["apiUrl"]
	if honeycombAPIURL == "" {
		honeycombAPIURL = os.Getenv("HONEYCOMB_API_URL")
	}
	if honeycombAPIURL == "" {
		honeycombAPIURL = DefaultHoneycombAPIURL
	}

	var sampleRate uint = DefaultSampleRate
	sampleRateString := route.Options["sampleRate"]
	if sampleRateString == "" {
		sampleRateString = os.Getenv("HONEYCOMB_SAMPLE_RATE")
	}
	if sampleRateString != "" {
		parsedSampleRate, err := strconv.ParseUint(sampleRateString, 10, 32)
		if err != nil {
			log.Fatal("Must provide Honeycomb SampleRate.")
			return nil, errors.New("Honeycomb 'SampleRate' must be an integer.")
		}
		sampleRate = uint(parsedSampleRate)
	}

	libhoney.Init(libhoney.Config{
		WriteKey:   writeKey,
		Dataset:    dataset,
		APIHost:    honeycombAPIURL,
		SampleRate: sampleRate,
	})

	return &HoneycombAdapter{}, nil
}

// Stream implements the router.LogAdapter interface.
func (a *HoneycombAdapter) Stream(logstream chan *router.Message) {
	hostname, err := os.Hostname()
	if err != nil {
		log.Println("error getting hostname", err)
	}
	log.Println("******* saaspanel " + Version)

	for m := range logstream {

		var data map[string]interface{}
		if err := json.Unmarshal([]byte(m.Data), &data); err != nil {
			// The message is not in JSON.
			// Capture the log line and stash it in "message".
			data = make(map[string]interface{})
			data["message"] = m.Data
		}

		ev := libhoney.NewEvent()

		// The message is already in JSON, add the docker specific fields.
		data["stream"] = m.Source
		data["logspout_container"] = m.Container.Name
		data["logspout_container_id"] = m.Container.ID
		data["logspout_hostname"] = m.Container.Config.Hostname
		data["logspout_docker_image"] = m.Container.Config.Image
		data["router_hostname"] = hostname

		// adapt timestamp
		if timeVal, ok := data["timestamp"]; ok {
			ts, err := time.Parse(time.RFC3339Nano, timeVal.(string))
			if err == nil {
				// we got a valid timestamp. Override the event's timestamp and remove the
				// field from data so we don't get it reported twice
				ev.Timestamp = ts
				delete(data, "timestamp")
			} else {
				ev.AddField("timestamp_error", fmt.Sprintf("problem parsing: %s", err))
			}
		}

		// adapt hasura logs
		if detailVal, ok1 := data["detail"]; ok1 {
			d := detailVal.(map[string]interface{});
			if operation, ok2 := d["operation"]; ok2 { // adapt hasura http log
				data["sp_trace_id"], _ = operation.(map[string]interface{})["request_id"].(string)
			} else {
				data["sp_trace_id"] = "no-go2"
			}
			// } else if query, ok2 := detailMap["query"]; ok2 { // adapt hasura query log
			// 	data["sp_trace_id"], _ = detailMap["request_id"].(string)
			// 	data["hasura_operation_name"], _ = query.(map[string]interface{})["operation_name"].(string)
			// }
		} else {
			data["sp_trace_id"] = "no-go1"
		}


		ev.Add(data)

		if err := ev.Send(); err != nil {
			log.Println("error: ", err)
		}
	}
}
