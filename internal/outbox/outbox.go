// Package outbox encodes Confluent-framed Avro RiskEvent records for the
// risk-engine transactional outbox.
package outbox

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/hamba/avro/v2"
)

const riskEventSchema = `{
  "type": "record",
  "name": "RiskEvent",
  "namespace": "exchange.risk",
  "fields": [
    {"name":"event_id","type":"string"},
    {"name":"event_type","type":"string"},
    {"name":"account_id","type":"long"},
    {"name":"asset","type":"string"},
    {"name":"total","type":"string"},
    {"name":"available","type":"string"},
    {"name":"frozen","type":"string"},
    {"name":"order_id","type":"long","default":0},
    {"name":"amount","type":"string","default":"0"},
    {"name":"occurred_at","type":"long"},
    {"name":"version","type":"long"}
  ]
}`

type RiskEvent struct {
	EventID    string `avro:"event_id"`
	EventType  string `avro:"event_type"`
	AccountID  int64  `avro:"account_id"`
	Asset      string `avro:"asset"`
	Total      string `avro:"total"`
	Available  string `avro:"available"`
	Frozen     string `avro:"frozen"`
	OrderID    int64  `avro:"order_id"`
	Amount     string `avro:"amount"`
	OccurredAt int64  `avro:"occurred_at"`
	Version    int64  `avro:"version"`
}

type Encoder struct {
	schema   avro.Schema
	schemaID int
}

func NewEncoder(registryURL, subject string) (*Encoder, error) {
	if strings.TrimSpace(registryURL) == "" {
		return nil, fmt.Errorf("SCHEMA_REGISTRY_URL is required")
	}
	schema, err := avro.Parse(riskEventSchema)
	if err != nil {
		return nil, err
	}
	id, err := registerSchema(registryURL, subject, riskEventSchema)
	if err != nil {
		return nil, err
	}
	return &Encoder{schema: schema, schemaID: id}, nil
}

func registerSchema(baseURL, subject, schema string) (int, error) {
	body, _ := json.Marshal(map[string]string{"schema": schema})
	u := strings.TrimRight(baseURL, "/") + "/subjects/" + url.PathEscape(subject) + "/versions"
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusMultipleChoices {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return 0, fmt.Errorf("schema registry register: %s: %s", resp.Status, string(b))
	}
	var out struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	if out.ID == 0 {
		return 0, fmt.Errorf("schema registry returned empty id")
	}
	return out.ID, nil
}

func (e *Encoder) Encode(event RiskEvent) ([]byte, error) {
	payload, err := avro.Marshal(e.schema, event)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 5+len(payload))
	out[0] = 0
	binary.BigEndian.PutUint32(out[1:5], uint32(e.schemaID))
	copy(out[5:], payload)
	return out, nil
}
