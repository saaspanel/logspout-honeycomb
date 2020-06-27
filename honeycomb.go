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
	"github.com/google/uuid"
	"github.com/honeycombio/libhoney-go"
)

const (
	DefaultHoneycombAPIURL = "https://api.honeycomb.io"
	DefaultSampleRate      = 1
	Version                = "v0.0.9"
)

func init() {
	router.AdapterFactories.Register(NewHoneycombAdapter, "honeycomb")
	libhoney.UserAgentAddition = "logspout-honeycomb"
}

type ttlMapItem struct {
	value      string
	lastAccess int64
}
type TTLMap struct {
	m map[string]*ttlMapItem
	l sync.Mutex
}

func NewTTLMap(maxTTLInSeconds int) (m *TTLMap) {
	m = &TTLMap{m: make(map[string]*ttlMapItem)}
	go func() {
		for now := range time.Tick(time.Second) {
			m.l.Lock()
			for k, v := range m.m {
				if now.Unix() - v.lastAccess > int64(maxTTLInSeconds) {
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
		it = &ttlMapItem{value: v}
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
	log.Println(uuid.New())

	// initialize ttl map
	ttlMap := NewTTLMap(60) // set TTL to 1 minute, which should cover 99% of the max duration across all hasura queries

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
		// NOTE: for a given request id, the order in which Hasura write the logs is:
		//       1) Query Log
		//       2) HTTP Log
		if detailVal, ok1 := data["detail"]; ok1 {
			d := detailVal.(map[string]interface{})
			if operation, ok2 := d["operation"]; ok2 { // adapt hasura http log
				requestID, _ := operation.(map[string]interface{})["request_id"].(string)

				// set tracing props
				data["sp_trace_id"] = requestID
				data["sp_span_id"] = "http-" + requestID

				// set operation name that we stored in our TTL
				data["hasura_query_operation_name"] = ttlMap.Get(requestID)

				// convert query execution time from seconds to milliseconds
				data["hasura_query_execution_time_in_ms"] = operation.(map[string]interface{})["query_execution_time"].(float64) * 1000
			} else if query, ok2 := d["query"]; ok2 { // adapt hasura query log
				requestID, _ := d["request_id"].(string)

				// set tracing props
				data["sp_trace_id"] = requestID
				data["sp_span_id"] = "query-" + requestID
				data["sp_parent_span_id"] = "http-" + requestID

				// set hasura query operation name **AND** add to TTL Map so we can send it with the HTTP Log
				hasuraQueryOperationName, _ := query.(map[string]interface{})["operation_name"].(string)
				data["hasura_query_operation_name"] = hasuraQueryOperationName
				m[requestID] = hasuraQueryOperationName
			}
		}

		ev.Add(data)

		if err := ev.Send(); err != nil {
			log.Println("error: ", err)
		}
	}
}
