package hostedtsdb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/intelsdi-x/snap-plugin-utilities/logger"
	"github.com/intelsdi-x/snap/control/plugin"
	"gopkg.in/raintank/schema.v1"
	"gopkg.in/raintank/schema.v1/msg"
)

type TsdbPool struct {
	sync.Mutex
	Pool map[string]*Tsdb
}

func NewTsdbPool() *TsdbPool {
	p := &TsdbPool{
		Pool: make(map[string]*Tsdb),
	}
	go p.purgeStale()
	return p
}

func (p *TsdbPool) purgeStale() {
	ticker := time.NewTicker(time.Minute * 30)
	for range ticker.C {
		p.Lock()
		for key, t := range p.Pool {
			if t.Stale() {
				t.Close()
				delete(p.Pool, key)
			}
		}
		p.Unlock()
	}
}

func (p *TsdbPool) Get(u *url.URL, apiKey string) *Tsdb {
	key := u.String() + apiKey
	p.Lock()
	t, ok := p.Pool[key]
	if !ok {
		t = NewTsdb(u, apiKey)
		p.Pool[key] = t
	}
	p.Unlock()
	return t
}

type Tsdb struct {
	sync.Mutex
	Url          *url.URL
	ApiKey       string
	Metrics      []*schema.MetricData
	triggerFlush chan struct{}
	LastFlush    time.Time
	closeChan    chan struct{}
}

func NewTsdb(u *url.URL, apiKey string) *Tsdb {
	t := &Tsdb{
		Metrics:      make([]*schema.MetricData, 0),
		triggerFlush: make(chan struct{}),
		Url:          u,
		ApiKey:       apiKey,
	}
	go t.Run()
	return t
}

func (t *Tsdb) Add(metrics []*schema.MetricData) {
	t.Lock()
	t.Metrics = append(t.Metrics, metrics...)
	if len(t.Metrics) > maxMetricsPerPayload {
		t.triggerFlush <- struct{}{}
	}
	t.Unlock()
}

func (t *Tsdb) Stale() bool {
	t.Lock()
	stale := time.Since(t.LastFlush) > (time.Minute * 30)
	t.Unlock()
	return stale
}

func (t *Tsdb) Flush() {
	t.Lock()
	if len(t.Metrics) == 0 {
		t.Unlock()
		return
	}
	t.LastFlush = time.Now()
	metrics := make([]*schema.MetricData, len(t.Metrics))
	copy(metrics, t.Metrics)
	t.Metrics = t.Metrics[:0]
	t.Unlock()
	// Write the metrics to our HTTP server.
	logger.LogDebug("writing metrics to API", "count", len(metrics))
	id := t.LastFlush.UnixNano()
	body, err := msg.CreateMsg(metrics, id, msg.FormatMetricDataArrayMsgp)
	if err != nil {
		logger.LogError("unable to convert metrics to MetricDataArrayMsgp.", "error", err)
		return
	}
	sent := false
	for !sent {
		if err = t.PostData("metrics", body); err != nil {
			logger.LogError(err.Error())
			time.Sleep(time.Second)
		} else {
			sent = true
		}
	}
}

func (t *Tsdb) Run() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			t.Flush()
		case <-t.triggerFlush:
			t.Flush()
		case <-t.closeChan:
			return
		}
	}
}
func (t *Tsdb) Close() {
	t.triggerFlush <- struct{}{}
	t.closeChan <- struct{}{}
}

func (t *Tsdb) PostData(path string, body []byte) error {
	u := t.Url.String() + path
	req, err := http.NewRequest("POST", u, bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "rt-metric-binary")
	req.Header.Set("Authorization", "Bearer "+t.ApiKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	respBody, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("Posting data failed. %d - %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func sendEvent(tsdb *Tsdb, orgId int, m *plugin.MetricType) {
	ns := m.Namespace().Strings()
	if len(ns) != 4 {
		logger.LogWarn("dropping invalid event metric. Expected namesapce to be 4 fields.")
		return
	}
	if ns[0] != "worldping" || ns[1] != "event" {
		logger.LogWarn("dropping invalid event metric.  Metric namespace should begin with 'worldping.event'")
		return
	}
	hostname, _ := os.Hostname()
	id := time.Now().UnixNano()
	event := &schema.ProbeEvent{
		OrgId:     int64(orgId),
		EventType: ns[2],
		Severity:  ns[3],
		Source:    hostname,
		Timestamp: id / int64(time.Millisecond),
		Message:   m.Data().(string),
		Tags:      m.Tags(),
	}

	body, err := msg.CreateProbeEventMsg(event, id, msg.FormatProbeEventMsgp)
	if err != nil {
		logger.LogError("Unable to convert event to ProbeEventMsgp.", "error", err)
		return
	}
	sent := false
	for !sent {
		if err = tsdb.PostData("events", body); err != nil {
			logger.LogError(err.Error())
			time.Sleep(time.Second)
		} else {
			sent = true
		}
	}
}
