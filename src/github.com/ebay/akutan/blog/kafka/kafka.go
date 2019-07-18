// Copyright 2019 eBay Inc.
// Primary authors: Simon Fell, Diego Ongaro,
//                  Raymond Kroeker, and Sathish Kandasamy.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package kafka implements a Kafka client as a blog.AkutanLog.
package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/config"
	"github.com/ebay/akutan/discovery"
	"github.com/ebay/akutan/util/clocks"
	"github.com/ebay/akutan/util/errors"
	log "github.com/sirupsen/logrus"
	sarama "gopkg.in/Shopify/sarama.v1"
)

const topic = "akutan"

// Log is a client to Kafka. It implements blog.AkutanLog.
// TODO: many of these methods are misbehaved in that they handle errors
// differently than described in blog.AkutanLog.
type Log struct {
	client   sarama.Client
	producer sarama.SyncProducer
	consumer sarama.Consumer
	broker   *sarama.Broker
}

func init() {
	blog.Factories["kafka"] = func(
		ctx context.Context, cfg *config.Akutan, brokers discovery.Locator,
	) (blog.AkutanLog, error) {
		return NewLog(ctx, cfg, brokers)
	}
}

// NewLog constructs a new Log.
func NewLog(ctx context.Context, cfg *config.Akutan, brokers discovery.Locator) (*Log, error) {
	sarama.MaxRequestSize = 11 * 1024 * 1024
	kconfig := sarama.NewConfig()
	kconfig.Version = sarama.V0_11_0_0                // need 0.11 for DeleteRecords
	kconfig.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	kconfig.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	kconfig.Producer.Return.Successes = true
	kconfig.Producer.MaxMessageBytes = 10 * 1024 * 1024
	kconfig.Consumer.Return.Errors = true
	kconfig.Consumer.Fetch.Min = 1
	kconfig.Consumer.Fetch.Default = 4 * 1024 * 1024
	kconfig.Consumer.Fetch.Max = 25 * 1024 * 1024
	kconfig.Consumer.MaxProcessingTime = time.Second / 2
	kconfig.ClientID = "akutan"

	// Look for at least one Kafka broker.
	if len(brokers.Cached().Endpoints) == 0 {
		log.WithFields(log.Fields{
			"locator": brokers,
		}).Warn("Waiting indefinitely to discover at least one Kafka broker")
	}
	start := time.Now()
	result, err := discovery.GetNonempty(ctx, brokers)
	if err != nil {
		return nil, fmt.Errorf("failed to discover a Kafka broker: %v", err)
	}
	log.WithFields(log.Fields{
		"locator":  brokers,
		"duration": time.Since(start),
		"brokers":  result.Endpoints,
	}).Info("Discovered Kafka brokers")

	brokerHostPorts := make([]string, len(result.Endpoints))
	for i, e := range result.Endpoints {
		brokerHostPorts[i] = e.HostPort()
	}
	client, err := sarama.NewClient(brokerHostPorts, kconfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create Kafka client: %v", err)
	}
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("unable to start kafka producer: %v", err)
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("unable to start kafka consumer: %v", err)
	}

	broker := sarama.NewBroker(brokerHostPorts[0])
	err = broker.Open(kconfig)
	if err != nil {
		return nil, fmt.Errorf("unable to start kafka broker: %v", err)
	}

	bl := &Log{
		client:   client,
		producer: producer,
		consumer: consumer,
		broker:   broker,
	}
	return bl, nil
}

func (aLog *Log) lastIndex(context.Context) (index int64, err error) {
	offset, err := aLog.client.GetOffset(topic, 0, sarama.OffsetNewest)
	return offset, err
}

func (aLog *Log) firstIndex(context.Context) (index int64, err error) {
	offset, err := aLog.client.GetOffset(topic, 0, sarama.OffsetOldest)
	return offset + 1, err
}

// Info implements the method from blog.AkutanLog.
func (aLog *Log) Info(ctx context.Context) (*blog.Info, error) {
	var info blog.Info
	lastIndex, err := aLog.lastIndex(ctx)
	if err != nil {
		return nil, err
	}
	info.LastIndex = uint64(lastIndex)
	firstIndex, err := aLog.firstIndex(ctx)
	if err != nil {
		return nil, err
	}
	info.FirstIndex = uint64(firstIndex)
	info.BytesUsed = 1337
	info.BytesTotal = 1000000000
	return &info, nil
}

// InfoStream implements the method from blog.AkutanLog.
func (aLog *Log) InfoStream(ctx context.Context, infoCh chan<- *blog.Info) error {
	defer close(infoCh)
	for {
		info, err := aLog.Info(ctx)
		if err != nil {
			return err
		}
		infoCh <- info
		clocks.Wall.SleepUntil(ctx, clocks.Wall.Now().Add(100*time.Millisecond))
	}
}

// Append implements the method from blog.AkutanLog.
func (aLog *Log) Append(ctx context.Context, msgs [][]byte) ([]blog.Index, error) {
	indexes := make([]blog.Index, len(msgs))
	for i := range msgs {
		_, offset, err := aLog.producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(msgs[i]),
		})
		if err != nil {
			return nil, err
		}
		indexes[i] = uint64(offset + 1)
	}
	return indexes, nil
}

// AppendSingle implements the method from blog.AkutanLog.
func (aLog *Log) AppendSingle(ctx context.Context, data []byte) (blog.Index, error) {
	indexes, err := aLog.Append(ctx, [][]byte{data})
	if err != nil {
		return 0, err
	}
	return indexes[0], err
}

// Read implements the method from blog.AkutanLog.
func (aLog *Log) Read(ctx context.Context, startIndex blog.Index, entriesCh chan<- []blog.Entry) error {
	defer close(entriesCh)

	// When unable to determine log start/end index due to Kafka unavailable or
	// network failure, wait for a brief duration and continue to retry.
	for {
		fi, err1 := aLog.firstIndex(ctx)
		li, err2 := aLog.lastIndex(ctx)
		if err := errors.Any(err1, err2); err == nil {
			log.Infof("aLog topic %s: firstIndex:%d, lastIndex:%d startConsumer startIndex:%d", topic, fi, li, startIndex)
			break
		} else {
			log.Errorf("Unable to determine log start/end: %v", err)
			time.Sleep(time.Second)
		}
	}
	if startIndex == 0 {
		startIndex = 1
	}
	consumer, err := aLog.consumer.ConsumePartition(topic, 0, int64(startIndex)-1)
	if err != nil {
		if err == sarama.ErrOffsetOutOfRange {
			return blog.TruncatedError{Requested: startIndex}
		}
		return fmt.Errorf("unable to start partition consumer for topic %v: %T %v", topic, err, err)
	}
	defer func() {
		// Close PartitionConsumer, silently ignore any error returned.
		consumer.Close()
		log.Infof("Closed Kafka consumer for topic: %v", topic)
	}()

	collectAndSend := func(first *sarama.ConsumerMessage) error {
		entries := make([]blog.Entry, 0, 32)
		entries = append(entries, blog.Entry{
			Index: uint64(first.Offset) + 1,
			Data:  first.Value,
			Skip:  false,
		})
		for {
			select {
			case km := <-consumer.Messages():
				entries = append(entries, blog.Entry{
					Index: uint64(km.Offset) + 1,
					Data:  km.Value,
					Skip:  false,
				})
				if len(entries) == cap(entries) {
					select {
					case entriesCh <- entries:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}

			case <-ctx.Done():
				return ctx.Err()

			default:
				entriesCh <- entries
				return nil
			}
		}
	}

	for {
		select {
		case km := <-consumer.Messages():
			err := collectAndSend(km)
			if err != nil {
				return err
			}

		case kerr := <-consumer.Errors():
			return fmt.Errorf("error reading from Kafka consumer: %v", kerr)

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Discard implements the method from blog.AkutanLog.
func (aLog *Log) Discard(ctx context.Context, startIndex blog.Index) error {
	request := sarama.DeleteRecordsRequest{
		Topics: map[string]*sarama.DeleteRecordsRequestTopic{
			topic: &sarama.DeleteRecordsRequestTopic{
				PartitionOffsets: map[int32]int64{
					0: int64(startIndex) - 1,
				},
			},
		},
	}
	response, err := aLog.broker.DeleteRecords(&request)
	if err != nil {
		return err
	}
	p := response.Topics[topic].Partitions[0]
	if p.Err != 0 {
		return p.Err
	}
	return nil
}
