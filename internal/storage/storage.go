package storage

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
)

type Storage interface {
	Get(key string) (string, error)
	Put(key, value string) error
	Delete(key string) error
	IterateOverKeyAndValues() (map[string]string, error)
}

type BadgerStore struct {
	db  *badger.DB
	log zerolog.Logger
}

func NewBadgerStore(db *badger.DB, log zerolog.Logger) *BadgerStore {
	return &BadgerStore{db: db, log: log}
}

func (s *BadgerStore) Get(key string) (string, error) {
	var result string
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			result = string(val)
			return nil
		})
	})
	if err != nil {
		s.log.Error().Err(err).Str("key", key).Msg("get failed")
		return "", fmt.Errorf("get %q: %w", key, err)
	}
	s.log.Debug().Str("key", key).Msg("get")
	return result, nil
}

func (s *BadgerStore) Put(key, value string) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), []byte(value))
	})
	if err != nil {
		s.log.Error().Err(err).Str("key", key).Msg("put failed")
		return err
	}
	s.log.Debug().Str("key", key).Msg("put")
	return nil
}

func (s *BadgerStore) Delete(key string) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
	if err != nil {
		s.log.Error().Err(err).Str("key", key).Msg("delete failed")
		return err
	}
	s.log.Debug().Str("key", key).Msg("delete")
	return nil
}


// IterateOverKeyAndValues returns every key/value in the store as a map.
// Used for draining a node's data during a planned removal/migration.
//
// Note: this loads the whole store into memory at once — fine at this
// project's scale, but a streaming callback would be the move for a large
// store.
func (s *BadgerStore) IterateOverKeyAndValues() (map[string]string, error) {
	result := make(map[string]string)
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			err := item.Value(func(val []byte) error {
				result[key] = string(val)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		s.log.Error().Err(err).Msg("iterate failed")
		return nil, fmt.Errorf("iterate store: %w", err)
	}
	s.log.Debug().Int("count", len(result)).Msg("iterated store")
	return result, nil
}