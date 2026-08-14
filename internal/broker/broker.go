package broker

import (
	"errors"
	"io"
	"sync"

	blink "github.com/jonnycap/blink/go"
	"github.com/jonnycap/queuego/internal/auth"
)

type Topic struct {
	Name        string
	ID          uint32
	Flags       byte
	Key         string
	Subscribers map[io.Writer]struct{}
	mu          sync.RWMutex
}

type Broker struct {
	topics map[uint32]*Topic
	mu     sync.RWMutex
	store  *Store
}

func NewBroker(store *Store) *Broker {
	return &Broker{
		topics: make(map[uint32]*Topic),
		store:  store,
	}
}

// CreateTopic creates or registers a topic using the Blink CREATE packet.
func (b *Broker) CreateTopic(frame *blink.CreateFrame) (uint32, error) {
	queueKey, err := auth.VerifyCreate(frame.JWT)
	if err != nil {
		return 0, err
	}

	topicID := blink.HashTopic(frame.TopicName)

	b.mu.Lock()
	defer b.mu.Unlock()

	if existing, ok := b.topics[topicID]; ok {
		existing.mu.Lock()
		defer existing.mu.Unlock()
		if existing.Key != queueKey {
			return 0, errors.New("topic already exists with a different key")
		}
		existing.Flags = frame.Flags
		return topicID, nil
	}

	b.topics[topicID] = &Topic{
		Name:        frame.TopicName,
		ID:          topicID,
		Flags:       frame.Flags,
		Key:         queueKey,
		Subscribers: make(map[io.Writer]struct{}),
	}

	return topicID, nil
}

// Subscribe registers a subscriber writer for a topic if the JWT is valid and has "subscribe" permission.
func (b *Broker) Subscribe(frame *blink.SubscribeFrame, w io.Writer) error {
	b.mu.RLock()
	topic, ok := b.topics[frame.TopicID]
	b.mu.RUnlock()
	if !ok {
		return errors.New("topic not found")
	}

	topic.mu.RLock()
	expectedKey := topic.Key
	topic.mu.RUnlock()

	if err := auth.VerifyTopicAccess(frame.JWT, expectedKey, "subscribe"); err != nil {
		return err
	}

	topic.mu.Lock()
	topic.Subscribers[w] = struct{}{}
	topic.mu.Unlock()

	return nil
}

// Unsubscribe removes a subscriber writer from a topic.
func (b *Broker) Unsubscribe(frame *blink.UnsubscribeFrame, w io.Writer) error {
	b.mu.RLock()
	topic, ok := b.topics[frame.TopicID]
	b.mu.RUnlock()
	if !ok {
		return errors.New("topic not found")
	}

	topic.mu.RLock()
	expectedKey := topic.Key
	topic.mu.RUnlock()

	if err := auth.VerifyTopicAccess(frame.JWT, expectedKey, ""); err != nil {
		return err
	}

	topic.mu.Lock()
	delete(topic.Subscribers, w)
	topic.mu.Unlock()

	return nil
}

// Publish delivers a message to all active subscribers if the JWT is valid and has "publish" permission.
func (b *Broker) Publish(frame *blink.PublishFrame) error {
	b.mu.RLock()
	topic, ok := b.topics[frame.TopicID]
	b.mu.RUnlock()
	if !ok {
		return errors.New("topic not found")
	}

	topic.mu.RLock()
	expectedKey := topic.Key
	topic.mu.RUnlock()

	if err := auth.VerifyTopicAccess(frame.JWT, expectedKey, "publish"); err != nil {
		return err
	}

	if b.store != nil {
		_ = b.store.SaveMessage(frame.TopicID, frame.Payload)
	}

	msg := blink.NewMessageFrame(frame.TopicID, frame.Payload)

	topic.mu.Lock()
	defer topic.mu.Unlock()

	var deadSubscribers []io.Writer
	for subWriter := range topic.Subscribers {
		if err := blink.SendFrame(subWriter, msg); err != nil {
			deadSubscribers = append(deadSubscribers, subWriter)
		}
	}

	for _, dead := range deadSubscribers {
		delete(topic.Subscribers, dead)
	}

	return nil
}

// RotateKey rotates the queue key and broadcasts KEY_UPDATE to all active subscribers.
func (b *Broker) RotateKey(frame *blink.RotateKeyFrame) error {
	b.mu.RLock()
	topic, ok := b.topics[frame.TopicID]
	b.mu.RUnlock()
	if !ok {
		return errors.New("topic not found")
	}

	topic.mu.Lock()
	defer topic.mu.Unlock()

	if err := auth.VerifyTopicAccess(frame.JWT, topic.Key, "rotate"); err != nil {
		return err
	}

	topic.Key = string(frame.NewKey)

	keyUpdate := blink.NewKeyUpdateFrame(frame.TopicID, frame.NewKey)
	var deadSubscribers []io.Writer
	for subWriter := range topic.Subscribers {
		if err := blink.SendFrame(subWriter, keyUpdate); err != nil {
			deadSubscribers = append(deadSubscribers, subWriter)
		}
	}

	for _, dead := range deadSubscribers {
		delete(topic.Subscribers, dead)
	}

	return nil
}

// RemoveSubscriber removes the given writer from all topics (used upon disconnect).
func (b *Broker) RemoveSubscriber(w io.Writer) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, topic := range b.topics {
		topic.mu.Lock()
		delete(topic.Subscribers, w)
		topic.mu.Unlock()
	}
}

