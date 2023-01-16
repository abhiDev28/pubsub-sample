package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub"
	pubsubx "github.com/pubsub-sample/pubsub"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func main() {
	pflag.String("type", "pub", "producer or subscriber")
	pflag.String("name", "example-subscription", "name of subscriber or producer")

	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)
	ctx := context.Background()
	proj := "dummy_project_id"
	topicName := "my-topic"
	client, err := pubsub.NewClient(ctx, proj)
	if err != nil {
		log.Fatalf("Could not create pubsub Client: %v", err)
	}
	broker := pubsubx.NewBroker("my-broker", client)
	err = broker.CreateTopic(topicName)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	clientType := viper.GetString("type")
	clientName := viper.GetString("name")
	if clientType == "pub" {
		// create producer instance
		producer := pubsubx.NewProducer(clientName, broker)
		fmt.Printf("Publishing messages on topic -> %s\n", topicName)
		for i := 0; i < 10; i++ {
			producer.Produce(topicName, "Hello World!")
		}
	} else {
		// create subscriber instance
		subscriber := pubsubx.NewSubscriber(clientName, broker)
		subscriber.Subscribe(topicName)
		fmt.Println("Waiting for messages...")
		subscriber.PullMessages(topicName)
	}

}
