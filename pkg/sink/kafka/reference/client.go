package reference

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/osrg/gobgp/v4/pkg/sink/kafka"
)

type ControlClient struct {
	URL  string
	HTTP *http.Client
}

func (c ControlClient) request(ctx context.Context, method, path string, body []byte) (kafka.CaptureReport, error) {
	var report kafka.CaptureReport
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.URL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return report, err
	}
	request.Header.Set("Content-Type", "application/json")
	client := c.HTTP
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return report, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusAccepted {
		return report, fmt.Errorf("capture management returned HTTP %d", response.StatusCode)
	}
	err = json.NewDecoder(http.MaxBytesReader(nil, response.Body, 1<<20)).Decode(&report)
	return report, err
}

func (c ControlClient) Status(ctx context.Context) (kafka.CaptureReport, error) {
	return c.request(ctx, http.MethodGet, "/v1/capture", nil)
}

func (c ControlClient) Resync(ctx context.Context, generation string) (kafka.CaptureReport, error) {
	body, _ := json.Marshal(map[string]string{"generation": generation})
	return c.request(ctx, http.MethodPost, "/v1/resync", body)
}

// IngestBatch refreshes the control fence before interpreting an unfamiliar
// generation. Otherwise a consumer could skip the new snapshot between polls.
func (c ControlClient) IngestBatch(ctx context.Context, s *State, records []*kgo.Record) error {
	for _, r := range records {
		generation := Header(r, "capture-generation")
		if Header(r, "sink") == s.Sink && generation != "" && generation != s.Generation() {
			report, err := c.Status(ctx)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrControlUnavailable, err)
			}
			if err := s.ObserveStatus(report); err != nil {
				return err
			}
			break
		}
	}
	for _, r := range records {
		if err := s.Ingest(r); err != nil {
			return err
		}
	}
	return nil
}

var ErrControlUnavailable = errors.New("capture management is unavailable")
