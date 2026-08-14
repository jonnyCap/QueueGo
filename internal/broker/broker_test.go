package broker

import (
	"bytes"
	"net"
	"testing"
	"time"

	blink "github.com/jonnycap/blink/go"
	"github.com/jonnycap/queuego/internal/auth"
)

func TestBrokerPubSubAndKeyRotation(t *testing.T) {
	masterKey := "broker-test-master-key"
	auth.SetMasterKey(masterKey)

	b := NewBroker(nil)

	topicName := "orders"
	queueKey1 := "initial-secret-key"
	queueKey2 := "rotated-secret-key"

	// Generate tokens
	createToken, err := auth.GenerateToken(masterKey, "owner", queueKey1, "create")
	if err != nil {
		t.Fatal(err)
	}
	subToken, err := auth.GenerateToken(masterKey, "subscriber", queueKey1, "subscribe")
	if err != nil {
		t.Fatal(err)
	}
	pubToken, err := auth.GenerateToken(masterKey, "publisher", queueKey1, "publish")
	if err != nil {
		t.Fatal(err)
	}
	rotateToken, err := auth.GenerateToken(masterKey, "owner", queueKey1, "rotate")
	if err != nil {
		t.Fatal(err)
	}

	// 1. Create Topic
	createFrame := blink.NewCreateFrame([]byte(createToken), topicName, 0x00)
	topicID, err := b.CreateTopic(createFrame)
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}
	expectedID := blink.HashTopic(topicName)
	if topicID != expectedID {
		t.Fatalf("expected topic ID %d, got %d", expectedID, topicID)
	}

	// 2. Subscribe (using pipe for writer/reader with background reader)
	subReader, subWriter := net.Pipe()
	defer subReader.Close()
	defer subWriter.Close()

	framesReceived := make(chan blink.Frame, 10)
	go func() {
		for {
			frame, err := blink.ReadFrame(subReader)
			if err != nil {
				return
			}
			framesReceived <- frame
		}
	}()

	subFrame := blink.NewSubscribeFrame([]byte(subToken), topicID)
	if err := b.Subscribe(subFrame, subWriter); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// 3. Publish Message
	payload := []byte("order payload #1001")
	pubFrame := blink.NewPublishFrame([]byte(pubToken), topicID, payload)
	if err := b.Publish(pubFrame); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	select {
	case frame := <-framesReceived:
		msg, ok := frame.(*blink.MessageFrame)
		if !ok {
			t.Fatalf("expected *MessageFrame, got %T", frame)
		}
		if msg.TopicID != topicID {
			t.Errorf("expected topic ID %d, got %d", topicID, msg.TopicID)
		}
		if !bytes.Equal(msg.Payload, payload) {
			t.Errorf("expected payload %q, got %q", payload, msg.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for MessageFrame")
	}

	// 4. Rotate Key
	rotateFrame := blink.NewRotateKeyFrame([]byte(rotateToken), topicID, []byte(queueKey2))
	if err := b.RotateKey(rotateFrame); err != nil {
		t.Fatalf("RotateKey failed: %v", err)
	}

	// Subscriber should receive KEY_UPDATE frame
	select {
	case frame := <-framesReceived:
		keyUp, ok := frame.(*blink.KeyUpdateFrame)
		if !ok {
			t.Fatalf("expected *KeyUpdateFrame, got %T", frame)
		}
		if keyUp.TopicID != topicID {
			t.Errorf("expected topic ID %d, got %d", topicID, keyUp.TopicID)
		}
		if string(keyUp.NewKey) != queueKey2 {
			t.Errorf("expected new key %q, got %q", queueKey2, string(keyUp.NewKey))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for KeyUpdateFrame")
	}

	// 5. Old token publish should now fail because key rotated
	oldPubFrame := blink.NewPublishFrame([]byte(pubToken), topicID, []byte("should fail"))
	if err := b.Publish(oldPubFrame); err == nil {
		t.Fatal("expected publish with old token to fail after key rotation")
	}

	// 6. New token publish should succeed
	newPubToken, err := auth.GenerateToken(masterKey, "publisher", queueKey2, "publish")
	if err != nil {
		t.Fatal(err)
	}
	newPubFrame := blink.NewPublishFrame([]byte(newPubToken), topicID, []byte("order payload #1002"))
	if err := b.Publish(newPubFrame); err != nil {
		t.Fatalf("expected publish with new token to succeed, got: %v", err)
	}

	// Read second message
	select {
	case frame := <-framesReceived:
		msg, ok := frame.(*blink.MessageFrame)
		if !ok {
			t.Fatalf("expected *MessageFrame, got %T", frame)
		}
		if !bytes.Equal(msg.Payload, []byte("order payload #1002")) {
			t.Errorf("expected payload 'order payload #1002', got %q", msg.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for second MessageFrame")
	}

	// 7. Unsubscribe
	unsubToken, err := auth.GenerateToken(masterKey, "subscriber", queueKey2, "subscribe")
	if err != nil {
		t.Fatal(err)
	}
	unsubFrame := blink.NewUnsubscribeFrame([]byte(unsubToken), topicID)
	if err := b.Unsubscribe(unsubFrame, subWriter); err != nil {
		t.Fatalf("Unsubscribe failed: %v", err)
	}

	// Publish after unsubscribe -> subscriber should not receive anything
	_ = b.Publish(newPubFrame)
	b.RemoveSubscriber(subWriter)
}

func TestCreateTopicSecurityAndIdempotency(t *testing.T) {
	masterKey := "security-test-master-key"
	auth.SetMasterKey(masterKey)
	b := NewBroker(nil)

	topicName := "secure-queue"
	topicID := blink.HashTopic(topicName)
	originalKey := "original-secret-key"
	attackerKey := "attacker-secret-key"

	// 1. Legitimate creator registers the topic
	validToken, err := auth.GenerateToken(masterKey, "legit-owner", originalKey, "create")
	if err != nil {
		t.Fatal(err)
	}
	id, err := b.CreateTopic(blink.NewCreateFrame([]byte(validToken), topicName, 0x00))
	if err != nil {
		t.Fatalf("initial CreateTopic failed: %v", err)
	}
	if id != topicID {
		t.Fatalf("expected topic ID %d, got %d", topicID, id)
	}

	// 2. Legitimate creator creates again with same key (idempotent success)
	id2, err := b.CreateTopic(blink.NewCreateFrame([]byte(validToken), topicName, 0x00))
	if err != nil {
		t.Fatalf("idempotent CreateTopic failed: %v", err)
	}
	if id2 != topicID {
		t.Fatalf("expected topic ID %d, got %d", topicID, id2)
	}

	// 3. Attacker attempts to overwrite the topic key with their own key -> MUST FAIL
	attackerToken, err := auth.GenerateToken(masterKey, "attacker", attackerKey, "create")
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.CreateTopic(blink.NewCreateFrame([]byte(attackerToken), topicName, 0x00))
	if err == nil {
		t.Fatal("expected CreateTopic with mismatched key on existing topic to be rejected, but it succeeded")
	}

	// 4. Verify topic is still accessible with original key
	subToken, err := auth.GenerateToken(masterKey, "subscriber", originalKey, "subscribe")
	if err != nil {
		t.Fatal(err)
	}
	subReader, subWriter := net.Pipe()
	defer subReader.Close()
	defer subWriter.Close()

	if err := b.Subscribe(blink.NewSubscribeFrame([]byte(subToken), topicID), subWriter); err != nil {
		t.Fatalf("expected subscribe with original key to succeed, got: %v", err)
	}
}

