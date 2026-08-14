package broker

// --------------------------- INFO -----------------------------
// This file is not yet integrated in the framework
// --------------------------- ---- -----------------------------

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

type Store struct {
	db        *badger.DB
	sequences sync.Map // uint32 -> *badger.Sequence
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

// Close closes the underlying BadgerDB and releases sequence leases
func (s *Store) Close() error {
	s.sequences.Range(func(key, value interface{}) bool {
		if seq, ok := value.(*badger.Sequence); ok {
			_ = seq.Release()
		}
		return true
	})
	return s.db.Close()
}

func (s *Store) getSequence(topicID uint32) (*badger.Sequence, error) {
	if val, ok := s.sequences.Load(topicID); ok {
		return val.(*badger.Sequence), nil
	}
	seqKey := fmt.Sprintf("seq:%d", topicID)
	seq, err := s.db.GetSequence([]byte(seqKey), 1000)
	if err != nil {
		return nil, err
	}
	actual, loaded := s.sequences.LoadOrStore(topicID, seq)
	if loaded {
		_ = seq.Release()
	}
	return actual.(*badger.Sequence), nil
}

// SaveMessage persists a message under a topic with atomic monotonic sequence.
// Messages are stored under keys like: topic:<topicID>:<016x-sequence>
func (s *Store) SaveMessage(topicID uint32, payload []byte) error {
	seq, err := s.getSequence(topicID)
	if err != nil {
		return err
	}
	nextSeq, err := seq.Next()
	if err != nil {
		return err
	}
	key := fmt.Sprintf("topic:%d:%016x", topicID, nextSeq)

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
