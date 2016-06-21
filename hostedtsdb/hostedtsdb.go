package hostedtsdb

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/intelsdi-x/snap-plugin-utilities/logger"
	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core/ctypes"
	"github.com/raintank/raintank-metric/schema"
)

const (
	name                 = "grafananet-tsdb"
	version              = 1
	pluginType           = plugin.PublisherPluginType
	maxMetricsPerPayload = 3000
)

var (
	ConnPool *TsdbPool
)

type HostedtsdbPublisher struct {
}

func NewHostedtsdbPublisher() *HostedtsdbPublisher {
	return &HostedtsdbPublisher{}
}

func Meta() *plugin.PluginMeta {
	return plugin.NewPluginMeta(
		name,
		version,
		pluginType,
		[]string{plugin.SnapGOBContentType},
		[]string{plugin.SnapGOBContentType},
		plugin.ConcurrencyCount(10000),
	)
}

func init() {
	ConnPool = NewTsdbPool()
}

func (f *HostedtsdbPublisher) GetConfigPolicy() (*cpolicy.ConfigPolicy, error) {
	c := cpolicy.New()
	rule, _ := cpolicy.NewStringRule("tsdbUrl", false, "https://tsdb.raintank.io/")
	rule2, _ := cpolicy.NewStringRule("apiKey", true)
	rule3, _ := cpolicy.NewIntegerRule("interval", true)
	rule4, _ := cpolicy.NewIntegerRule("orgId", false, 0)
	rule5, _ := cpolicy.NewStringRule("prefix", false, "")

	p := cpolicy.NewPolicyNode()
	p.Add(rule)
	p.Add(rule2)
	p.Add(rule3)
	p.Add(rule4)
	p.Add(rule5)
	c.Add([]string{""}, p)
	return c, nil
}

func (f *HostedtsdbPublisher) Publish(contentType string, content []byte, config map[string]ctypes.ConfigValue) error {
	var metrics []plugin.MetricType

	switch contentType {
	case plugin.SnapGOBContentType:
		dec := gob.NewDecoder(bytes.NewBuffer(content))
		if err := dec.Decode(&metrics); err != nil {
			logger.LogError(err.Error(), "content", content)
			return err
		}
	default:
		logger.LogError("unknown content type", "contentType", contentType)
		return errors.New(fmt.Sprintf("Unknown content type '%s'", contentType))
	}

	remote := config["tsdbUrl"].(ctypes.ConfigValueStr).Value
	if remote == "" {
		remote = "https://tsdb.raintank.io/"
	}
	if !strings.HasSuffix(remote, "/") {
		remote += "/"
	}
	remoteUrl, err := url.Parse(remote)
	if err != nil {
		logger.LogError("Invalid TSDB URL", "error", err)
		return err
	}

	logger.LogDebug("publishing metrics to %s, count %d", remote, len(metrics))

	apiKey := config["apiKey"].(ctypes.ConfigValueStr).Value
	interval := config["interval"].(ctypes.ConfigValueInt).Value
	orgId := config["orgId"].(ctypes.ConfigValueInt).Value
	prefix := config["prefix"].(ctypes.ConfigValueStr).Value

	tsdb := ConnPool.Get(remoteUrl, apiKey)

	metricsArray := make([]*schema.MetricData, len(metrics))
	for i, m := range metrics {
		var value float64
		rawData := m.Data()
		switch rawData.(type) {
		case nil:
			// there is no data to send.
			continue
		case string:
			//payload is an event.
			go sendEvent(tsdb, orgId, &m)
			continue
		case int:
			value = float64(rawData.(int))
		case int8:
			value = float64(rawData.(int8))
		case int16:
			value = float64(rawData.(int16))
		case int32:
			value = float64(rawData.(int32))
		case int64:
			value = float64(rawData.(int64))
		case uint8:
			value = float64(rawData.(uint8))
		case uint16:
			value = float64(rawData.(uint16))
		case uint32:
			value = float64(rawData.(uint32))
		case uint64:
			value = float64(rawData.(uint64))
		case float32:
			value = float64(rawData.(float32))
		case float64:
			value = rawData.(float64)
		case bool:
			if rawData.(bool) {
				value = 1
			} else {
				value = 0
			}
		default:
			return errors.New(fmt.Sprintf("unknown data type: %T", rawData))
		}

		tags := make([]string, 0)
		targetType := "gauge"
		unit := ""
		for k, v := range m.Tags() {
			switch k {
			case "targetType":
				targetType = v
			case "unit":
				unit = v
			default:
				tags = append(tags, fmt.Sprintf("%s:%s", k, v))
			}
		}

		var name string
		if prefix != "" {
			name = fmt.Sprintf("%s.%s", prefix, m.Namespace().Key())
		} else {
			name = m.Namespace().Key()
		}

		metric := &schema.MetricData{
			OrgId:      orgId,
			Name:       name,
			Interval:   interval,
			Value:      value,
			Time:       m.Timestamp().Unix(),
			TargetType: targetType,
			Unit:       unit,
			Tags:       tags,
		}
		metric.SetId()
		metricsArray[i] = metric
	}
	tsdb.Add(metricsArray)

	return nil
}
