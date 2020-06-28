package honeycomb

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/araddon/dateparse"
	"github.com/gliderlabs/logspout/router"
	"github.com/google/uuid"
	"github.com/honeycombio/libhoney-go"
)

const (
	DefaultHoneycombAPIURL = "https://api.honeycomb.io"
	DefaultSampleRate      = 1
	Version                = "v0.0.21"
)

var timezone = ""

func init() {
	router.AdapterFactories.Register(NewHoneycombAdapter, "honeycomb")
	libhoney.UserAgentAddition = "logspout-honeycomb"

	// set timezone to UTC for all subsequent datetime parsing
	flag.StringVar(&timezone, "timezone", "UTC", "Timezone aka `America/Los_Angeles` formatted time-zone")
	flag.Parse()

	if timezone != "" {
		// NOTE:  This is very, very important to understand
		// time-parsing in go
		loc, err := time.LoadLocation(timezone)
		if err != nil {
			panic(err.Error())
		}
		time.Local = loc
	}

}

// finally tracked down how honeycomb sends its trace data via golang
// 		https://github.com/honeycombio/opentelemetry-exporter-go/blob/master/honeycomb/honeycomb.go#L331
// we'll use this approach as well (need to set service name)
// NOTE: better documentation of properties
//		https://docs.honeycomb.io/getting-data-in/tracing/send-trace-data/#manual-tracing
type span struct {
	ServiceName     string  `json:"service_name"`              // The name of the service that generated this span
	Name            string  `json:"name"`                      // The specific call location (like a function or method name)
	TraceID         string  `json:"trace.trace_id"`            // The ID of the trace this span belongs to
	ID              string  `json:"trace.span_id"`             // A unique ID for each span
	ParentID        string  `json:"trace.parent_id,omitempty"` // The ID of this span’s parent span, the call location the current span was called from
	DurationMS      float64 `json:"duration_ms"`               // How much time the span took, in milliseconds
	Status          string  `json:"response.status_code,omitempty"`
	Error           bool    `json:"error,omitempty"`
	HasRemoteParent bool    `json:"has_remote_parent"`
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
			// ts, err := time.Parse(time.RFC3339Nano, timeVal.(string))
			// ts, err := time.Parse("2006-01-02T15:04:05Z0700", timeVal) // NOTE: this is what we need for Hasura (ISO8061 w/o colon)
			ts, err := dateparse.ParseLocal(timeVal) // use datparse module, which performantly auto-detects and parses the datetime string
			if err == nil {
				// we got a valid timestamp. Override the event's timestamp and remove the
				// field from data so it's not duplicated
				ev.Timestamp = ts
				delete(data, "timestamp")
			} else {
				ev.AddField("timestamp_error", fmt.Sprintf("problem parsing: %s", err))
			}
		}

		// adapt hasura logs: https://hasura.io/docs/1.0/graphql/manual/deployment/logging.html#different-log-types
		// NOTE: for a given request id, the order in which Hasura write the logs is:
		//       1) Query Log
		//       2) HTTP Log
		if detailVal, ok := data["detail"]; ok { // we're dealing with a Hasura Log type
			d := detailVal.(map[string]interface{})
			span := span{ServiceName: "hasura"}

			// set error flag
			if logLevel, ok := data["level"]; ok {
				span.Error = strings.ToLower(logLevel.(string)) == "error"
			}

			if operation, ok := d["operation"]; ok { // adapt hasura http log
				requestID, _ := operation.(map[string]interface{})["request_id"].(string)

				// set tracing props
				span.TraceID = requestID
				span.ID = "http-" + requestID

				// get the operation name that we stored in our TTL Map
				// and use to set the span's Name property
				span.Name = ttlMap.Get(requestID)

				// convert query execution time from seconds to milliseconds
				if queryExecutionTime := operation.(map[string]interface{})["query_execution_time"]; queryExecutionTime != nil {
					span.DurationMS = queryExecutionTime.(float64) * 1000
				}
			} else if query, ok := d["query"]; ok { // adapt hasura query log
				requestID, _ := d["request_id"].(string)

				// set tracing props
				span.TraceID = requestID
				span.ID = "query-" + requestID
				span.ParentID = "http-" + requestID
				span.Name = "db query"

				// set hasura query operation name **AND** add to TTL Map so we can send it with the HTTP Log
				hasuraQueryOperationName, _ := query.(map[string]interface{})["operationName"].(string)
				ttlMap.Put(requestID, hasuraQueryOperationName)
			}

			// merge the hasura span properties with the honeycomb event properties
			if j, err := json.Marshal(span); err != nil {
				log.Panicln(err)
			} else {
				json.Unmarshal(j, &data)
			}
		}

		ev.Add(data)

		if err := ev.Send(); err != nil {
			log.Println("error: ", err)
		}
	}
}
