package kafka

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/flexprice/flexprice/internal/config"
	"github.com/flexprice/flexprice/internal/logger"
	"github.com/flexprice/flexprice/internal/types"
	"go.uber.org/zap"
)

type MessageConsumer interface {
	Subscribe(topic string) (<-chan *message.Message, error)
	Close() error
}

type Consumer struct {
	subscriber message.Subscriber
	cfg        *config.Configuration
	logger     *logger.Logger

	// Metrics
	lastProcessedTime time.Time
	messageCount      int64
}

// EstimatedThroughput calculates the estimated messages per second based on configuration
func EstimatedThroughput(cfg *config.KafkaConfig) float64 {
	// Average message size estimation (adjust based on your actual message size)
	avgMessageSize := int32(1024) // 1KB

	// Calculate messages that can fit in one fetch
	msgsPerFetch := int32(cfg.Consumer.FetchSize) / avgMessageSize

	// Calculate fetches per second based on MaxWaitTime
	fetchesPerSecond := float64(time.Second) / float64(cfg.Consumer.MaxWaitTime)

	// Theoretical max throughput
	return float64(msgsPerFetch) * fetchesPerSecond
}

func NewConsumer(cfg *config.Configuration, log *logger.Logger) (MessageConsumer, error) {
	enableDebugLogs := cfg.Logging.Level == types.LogLevelDebug

	saramaConfig := GetSaramaConfig(cfg)
	if saramaConfig != nil {
		// Use configuration parameters for consumer tuning
		saramaConfig.Consumer.Group.Session.Timeout = cfg.Kafka.Consumer.SessionTimeout
		saramaConfig.Consumer.Fetch.Min = cfg.Kafka.Consumer.FetchMin
		saramaConfig.Consumer.Fetch.Default = cfg.Kafka.Consumer.FetchSize
		saramaConfig.Consumer.MaxWaitTime = cfg.Kafka.Consumer.MaxWaitTime
		saramaConfig.Consumer.MaxProcessingTime = cfg.Kafka.Consumer.MaxProcessingTime

		// Log estimated throughput
		estimatedThroughput := EstimatedThroughput(&cfg.Kafka)
		log.With(
			zap.Float64("estimated_msgs_per_sec", estimatedThroughput),
			zap.Int32("fetch_size_bytes", cfg.Kafka.Consumer.FetchSize),
			zap.Duration("max_wait_time", cfg.Kafka.Consumer.MaxWaitTime),
		).Info("estimated kafka consumer throughput")
	}

	subscriber, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:               cfg.Kafka.Brokers,
			ConsumerGroup:         cfg.Kafka.ConsumerGroup,
			Unmarshaler:           kafka.DefaultMarshaler{},
			OverwriteSaramaConfig: saramaConfig,
			ReconnectRetrySleep:   time.Second,
		},
		watermill.NewStdLogger(enableDebugLogs, enableDebugLogs),
	)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		subscriber:        subscriber,
		cfg:               cfg,
		logger:            log,
		lastProcessedTime: time.Now(),
	}, nil
}

func (c *Consumer) Subscribe(topic string) (<-chan *message.Message, error) {
	messages, err := c.subscriber.Subscribe(context.Background(), topic)
	if err != nil {
		return nil, err
	}

	// Create a monitoring channel
	monitoredChan := make(chan *message.Message)

	// Start throughput monitoring
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				now := time.Now()
				duration := now.Sub(c.lastProcessedTime).Seconds()
				if duration > 0 {
					throughput := float64(c.messageCount) / duration
					c.logger.With(
						zap.Float64("messages_per_second", throughput),
						zap.Int64("total_messages", c.messageCount),
						zap.Duration("duration", now.Sub(c.lastProcessedTime)),
					).Info("kafka consumer throughput")
				}
				c.messageCount = 0
				c.lastProcessedTime = now
			}
		}
	}()

	// Forward messages with monitoring
	go func() {
		for msg := range messages {
			c.messageCount++
			monitoredChan <- msg
		}
		close(monitoredChan)
	}()

	return monitoredChan, nil
}

func (c *Consumer) Close() error {
	return c.subscriber.Close()
}
