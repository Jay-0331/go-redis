package cache

import (
	"sync"
	"time"
)

type Cache interface {
	Get(key string) string
	Set(key, value string, px int64)
	Del(key string)
	Keys() []string
	GetType(key string) string
	SetStream(key string)
	AddToStream(streamKey, streamId, key, value string)
}

type Store struct {
	mu sync.Mutex
	data map[string]storeData
}

type item struct {
	String string
	Stream map[string]storeData
}

type storeData struct {
	value item
	dataType string
	ttl int64
}

func newStore() *Store {
	return &Store{
		data: make(map[string]storeData),
	}
}

func NewCache() Cache {
	s := newStore()
	go s.cleanUpRoutine()
	return s
}

func (store *Store) Get(key string) string {
	store.cleanUp()
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.data[key].value.String
}

func (store *Store) Set(key, value string, px int64) {
	store.mu.Lock()
	defer store.mu.Unlock()
	switch px {
	case 0:
		store.data[key] = storeData{
			value: item{String: value},
			dataType: "string",
			ttl: 0,
		}
	default:
		store.data[key] = storeData{
			value: item{String: value},
			dataType: "string",
			ttl: time.Now().UnixMilli() + px,
		}
	}
}

func (store *Store) Del(key string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.data, key)
}

func (store *Store) Keys() []string {
	store.mu.Lock()
	defer store.mu.Unlock()
	keys := []string{}
	for k := range store.data {
		keys = append(keys, k)
	}
	return keys
}

func (store *Store) GetType(key string) string {
	store.cleanUp()
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.data[key].dataType
}

func (store *Store) SetStream(key string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.data[key] = storeData{
		value: item{Stream: make(map[string]storeData)},
		dataType: "stream",
		ttl: 0,
	}
}

func (store *Store) AddToStream(streamKey, streamId, key, value string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.data[streamKey].value.Stream[streamId] = storeData{
		value: item{String: value},
		dataType: "string",
		ttl: 0,
	}
}

func (store *Store) cleanUp() {
	for key, value := range store.data {
		if value.ttl > 0 && value.ttl < time.Now().UnixMilli() {
			store.Del(key)
		}
	}
}

func (store *Store) cleanUpRoutine() {
	for {
		time.Sleep(120 * time.Second)
		store.cleanUp()
	}
}