package broker

import (
	"errors"
	"io"
	"sync"

	blink "github.com/jonnycap/blink/go"
	"github.com/jonnycap/queuego/internal/auth"
)

type Subscriber struct {
	Writer io.Writer
}

type Topic struct {
	Name       string
	ID         uint32
	Subscribers map[*Subscriber]struct{}
	Key         []byte
	mu          sync.Mutex
}

type Broker struct {
	topics map[uint32]*Topic
	nextID uint32
	mu     sync.Mutex
	store  *Store
}

func NewBroker(store *Store) *Broker {
	return &Broker{
		topics: make(map[uint32]*Topic),
		nextID: 1,
		store: store,
	}
}

func (b *Broker) CreateTopic(frame *blink.CreateFrame) (uint32, error) {
	if err := auth.VerifyJWT(frame.JWT); err != nil {
		return 0, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	id := b.nextID
	b.nextID++

	b.topics[id] = &Topic{
		Name:        frame.TopicName,
		ID:          id,
		Subscribers: make(map[*Subscriber]struct{}),
		Key:         frame.JWT,
	}
	auth.SetTopicKey(id, frame.JWT)
	return id, nil
}

func (b *Broker) Subscribe(frame *blink.SubscribeFrame, w io.Writer) error {
	if err := auth.VerifyTopicKey(frame.JWT, frame.TopicID); err != nil {
		return err
	}

	b.mu.Lock()
	topic, ok := b.topics[frame.TopicID]
	b.mu.Unlock()
	if !ok {
		return errors.New("topic not found")
	}

	topic.mu.Lock()
	defer topic.mu.Unlock()
	topic.Subscribers[&Subscriber{Writer: w}] = struct{}{}
	return nil
}

func (b *Broker) Unsubscribe(frame *blink.UnsubscribeFrame, w io.Writer) error {
	b.mu.Lock()
	topic, ok := b.topics[frame.TopicID]
	b.mu.Unlock()
	if !ok {
		return errors.New("topic not found")
	}

	topic.mu.Lock()
	defer topic.mu.Unlock()
	for sub := range topic.Subscribers {
		if sub.Writer == w {
			delete(topic.Subscribers, sub)
			break
		}
	}
	return nil
}

func (b *Broker) Publish(frame *blink.PublishFrame) error {
	if err := auth.VerifyTopicKey(frame.JWT, frame.TopicID); err != nil {
		return err
	}

	b.mu.Lock()
	topic, ok := b.topics[frame.TopicID]
	b.mu.Unlock()
	if !ok {
		return errors.New("topic not found")
	}

	topic.mu.Lock()
	defer topic.mu.Unlock()
	for sub := range topic.Subscribers {
		blink.SendFrame(sub.Writer, blink.NewMessageFrame(frame.TopicID, frame.Payload))
	}
	return nil
}

func (b *Broker) RotateKey(frame *blink.RotateKeyFrame) error {
	if err := auth.VerifyTopicKey(frame.JWT, frame.TopicID); err != nil {
		return err
	}

	auth.SetTopicKey(frame.TopicID, frame.NewKey)
	return nil
}
