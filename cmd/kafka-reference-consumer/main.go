// kafka-reference-consumer materializes an Adj-RIB-In view and reports its
// validity. This laboratory client uses plaintext Kafka and trusted HTTP.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/osrg/gobgp/v4/pkg/sink/kafka/reference"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	brokers := flag.String("brokers", "127.0.0.1:9092", "Kafka brokers")
	topic := flag.String("topic", "", "sink topic")
	sink := flag.String("sink", "raw", "sink name")
	control := flag.String("control", "http://127.0.0.1:50052", "trusted capture management URL")
	auto := flag.Bool("auto-resync", true, "request a fresh snapshot after loss")
	gap := flag.Duration("gap-timeout", 15*time.Second, "maximum no-progress interval before resynchronization")
	flag.Parse()
	if *topic == "" || *gap <= 0 {
		return fmt.Errorf("topic and a positive gap timeout are required")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...), kgo.ConsumeTopics(*topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchMaxBytes(16<<20))
	if err != nil {
		return err
	}
	defer client.Close()
	manager := reference.ControlClient{URL: *control}
	state := reference.New(*sink)
	nextStatus, nextResync := time.Time{}, time.Time{}
	managementAvailable := false
	var pending []*kgo.Record
	for ctx.Err() == nil {
		if time.Now().After(nextStatus) {
			report, err := manager.Status(ctx)
			managementAvailable = err == nil
			if err == nil {
				err = state.ObserveStatus(report)
			}
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
			if err == nil && *auto && time.Now().After(nextResync) && state.NeedsResync(time.Now(), *gap) {
				nextResync = time.Now().Add(5 * time.Second)
				fresh, e := manager.Resync(ctx, state.Generation())
				if e != nil {
					fmt.Fprintln(os.Stderr, e)
				} else if e = state.ObserveStatus(fresh); e != nil {
					return e
				}
			}
			_ = json.NewEncoder(os.Stdout).Encode(struct {
				Generation          string `json:"generation"`
				Applied             uint64 `json:"applied_sequence"`
				Routes              int    `json:"routes"`
				Consistent          bool   `json:"consistent"`
				ManagementAvailable bool   `json:"management_available"`
				Digest              string `json:"digest"`
			}{state.Generation(), state.Applied(), state.RouteCount(), managementAvailable && state.Ready(), managementAvailable, state.Digest()})
			nextStatus = time.Now().Add(time.Second)
		}
		if pending != nil {
			if managementAvailable {
				err := manager.IngestBatch(ctx, state, pending)
				if errors.Is(err, reference.ErrControlUnavailable) {
					managementAvailable = false
					fmt.Fprintln(os.Stderr, err)
				} else {
					pending = nil
					if err != nil {
						fmt.Fprintln(os.Stderr, err)
					}
				}
			}
			if pending != nil {
				select {
				case <-ctx.Done():
				case <-time.After(100 * time.Millisecond):
				}
				continue
			}
		}
		poll, stop := context.WithTimeout(ctx, 100*time.Millisecond)
		fetches := client.PollRecords(poll, 1024)
		stop()
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			fmt.Fprintln(os.Stderr, err)
		}
		pending = fetches.Records()
	}
	return nil
}
