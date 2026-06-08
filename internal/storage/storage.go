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
