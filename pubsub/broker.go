package pubsub

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
)

type Broker struct {
	name   string
	client *pubsub.Client
}

// Create new broker instance
func NewBroker(name string, client *pubsub.Client) *Broker {
	return &Broker{
		name:   name,
		client: client,
	}
}

// Create topic if not exists
func (b *Broker) CreateTopic(topic string) error {
	ctx := context.Background()
	t := b.client.Topic(topic)
	ok, err := t.Exists(ctx)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	_, err = b.client.CreateTopic(ctx, topic)
	if err != nil {
		return err
	}
	return nil
}

// Create subscription to the given topic
func (b *Broker) CreateSubscription(name, topic string) error {
	ctx := context.Background()
	t := b.client.Topic(topic)

	sub, err := b.client.CreateSubscription(ctx, name, pubsub.SubscriptionConfig{
		Topic:       t,
		AckDeadline: 20 * time.Second,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Created subscription: %v\n", sub)
	return nil
}

// Remove subscription to the given topic
func (b *Broker) RemoveSubscription(name string) error {
	ctx := context.Background()
	_, err := b.client.DetachSubscription(ctx, name)
	if err != nil {
		return err
	}
	fmt.Printf("Subscription removed: %s\n", name)
	return nil
}

// Publish message to the given topic
func (b *Broker) Publish(topic, msg string) error {
	ctx := context.Background()
	t := b.client.Topic(topic)
	result := t.Publish(ctx, &pubsub.Message{
		Data: []byte(msg),
	})
	id, err := result.Get(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("Published a message, message ID: %s\n", id)
	return nil
}
