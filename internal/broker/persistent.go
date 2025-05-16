package broker

// --------------------------- INFO -----------------------------
// This file is not yet integrated in the framework
// --------------------------- ---- -----------------------------

import (
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/dgraph-io/badger/v4"
)

type Store struct {
	db *badger.DB
}

// OpenStore initializes a BadgerDB instance at the given path.
func OpenStore(dir string) (*Store, error) {
	opts := badger.DefaultOptions(filepath.Clean(dir)).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close closes the underlying BadgerDB
func (s *Store) Close() error {
	return s.db.Close()
}

// SaveMessage persists a message under a topic.
// Messages are stored under keys like: topic:<topicID>:<auto-incremented-seq>
func (s *Store) SaveMessage(topicID uint32, payload []byte) error {
	topicKeyPrefix := fmt.Sprintf("topic:%d:", topicID)

	var lastSeq uint64 = 0

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(topicKeyPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			suffix := key[len(prefix):]
			seq, err := strconv.ParseUint(string(suffix), 10, 64)
			if err != nil {
				continue
			}
			if seq > lastSeq {
				lastSeq = seq
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	nextSeq := lastSeq + 1
	key := fmt.Sprintf("%s%d", topicKeyPrefix, nextSeq)

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), payload)
	})
}

// LoadMessages retrieves all messages for a topic.
func (s *Store) LoadMessages(topicID uint32) ([][]byte, error) {
	var messages [][]byte
	prefix := []byte(fmt.Sprintf("topic:%d:", topicID))

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			messages = append(messages, val)
		}
		return nil
	})

	return messages, err
}

// DeleteTopicMessages deletes all messages related to a topic.
func (s *Store) DeleteTopicMessages(topicID uint32) error {
	prefix := []byte(fmt.Sprintf("topic:%d:", topicID))

	return s.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			err := txn.Delete(it.Item().Key())
			if err != nil {
				return err
			}
		}
		return nil
	})
}
