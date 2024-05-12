package util

type PubSub interface {
	Subscribe(channel string) chan PubSubMessage
	Unsubscribe(channel string)
	Publish(channel, message string)
}

type PubSubMessage struct {
	Topic string
	Message string
}

type PubSubImpl struct {
	Channels map[string]chan PubSubMessage
}

func NewPubSub() PubSub {
	return &PubSubImpl{
		Channels: make(map[string]chan PubSubMessage),
	}
}

func (ps *PubSubImpl) Subscribe(topic string) chan PubSubMessage {
	if _, ok := ps.Channels[topic]; !ok {
		ps.Channels[topic] = make(chan PubSubMessage)
	}
	return ps.Channels[topic]
}

func (ps *PubSubImpl) Unsubscribe(topic string) {
	if _, ok := ps.Channels[topic]; ok {
		close(ps.Channels[topic])
		delete(ps.Channels, topic)
	}
}

func (ps *PubSubImpl) Publish(topic, message string) {
	if ch, ok := ps.Channels[topic]; ok {
		ch <- PubSubMessage{Topic: topic, Message: message}
	}
}