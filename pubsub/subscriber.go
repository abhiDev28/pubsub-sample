package pubsub

import (
	"context"
	"fmt"
	"sync"

	"cloud.google.com/go/pubsub"
)

type Subscriber struct {
	id     string  // subscriber id
	broker *Broker // pubsub broker instance
}

// Create new subscriber
func NewSubscriber(id string, broker *Broker) *Subscriber {
	return &Subscriber{
		id:     id,
		broker: broker,
	}
}

// Create subscription to given topic
func (s *Subscriber) Subscribe(topic string) error {
	return s.broker.CreateSubscription(s.id, topic)
}

func (s *Subscriber) Unsubscribe() error {
	return s.broker.RemoveSubscription(s.id)
}

func (s *Subscriber) PullMessages(topic string) error {
	ctx := context.Background()
	var mu sync.Mutex
	received := 0
	sub := s.broker.client.Subscription(s.id)
	cctx, cancel := context.WithCancel(ctx)
	err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		msg.Ack()
		fmt.Printf("Got message: %q\n", string(msg.Data))
		mu.Lock()
		received++
		mu.Unlock()
		if received == 10 {
			cancel()
		}
	})
	if err != nil {
		return err
	}
	return nil
}
