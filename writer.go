package kafka_zerolog

import (
	"context"
	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/zstd"
	"time"
)

type KafkaWriter struct {
	kw       *kafka.Writer
	MinLevel zerolog.Level
}

func NewZerologKafkaWriter(brokers []string, topicName string) *KafkaWriter {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:          brokers,
		Topic:            topicName,
		Balancer:         kafka.CRC32Balancer{},
		BatchTimeout:     time.Millisecond * 100,
		Async:            true,
		CompressionCodec: zstd.NewCompressionCodec(),
	})
	return &KafkaWriter{
		kw:       w,
		MinLevel: 0,
	}
}

func (k *KafkaWriter) SetMinLevel(level zerolog.Level) {
	k.MinLevel = level
}

func (k *KafkaWriter) WriteLevel(level zerolog.Level, p []byte) (n int, err error) {
	if level >= k.MinLevel {
		k.kw.WriteMessages(context.Background(), kafka.Message{
			Value: p,
			Time:  time.Now(),
		})
	}
	return len(p), nil
}

func (k *KafkaWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (k *KafkaWriter) Close() {
	k.kw.Close()
}
