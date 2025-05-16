package auth

import (
	"errors"
	"sync"

	"github.com/golang-jwt/jwt/v5"
)

var masterKey string
var topicKeys = struct {
	sync.RWMutex
	keys map[uint32][]byte
}{keys: make(map[uint32][]byte)}

func SetMasterKey(key string) {
	masterKey = key
}

func VerifyJWT(token []byte) error {
	_, err := jwt.Parse(string(token), func(t *jwt.Token) (interface{}, error) {
		return []byte(masterKey), nil
	})
	return err
}

func VerifyTopicKey(token []byte, topicID uint32) error {
	topicKeys.RLock()
	key, ok := topicKeys.keys[topicID]
	topicKeys.RUnlock()
	if !ok {
		return errors.New("topic key not found")
	}
	_, err := jwt.Parse(string(token), func(t *jwt.Token) (interface{}, error) {
		return key, nil
	})
	return err
}

func SetTopicKey(topicID uint32, key []byte) {
	topicKeys.Lock()
	defer topicKeys.Unlock()
	topicKeys.keys[topicID] = key
}
