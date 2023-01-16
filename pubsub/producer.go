package pubsub

type Producer struct {
	id     string  // producer id
	broker *Broker // pubsub broker instance
}

// Create producer instance
func NewProducer(id string, broker *Broker) *Producer {
	return &Producer{
		id:     id,
		broker: broker,
	}
}

// Publish message to given topic
func (p *Producer) Produce(topic, msg string) error {
	return p.broker.Publish(topic, msg)
}
