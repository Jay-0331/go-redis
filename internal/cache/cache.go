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
}

type Store struct {
	mu sync.Mutex
	data map[string]storeData
}

type item struct {
	String string
	Stream map[string]map[string]string
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

func (d *Store) Get(key string) string {
	d.cleanUp()
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.data[key].value.String
}

func (d *Store) Set(key, value string, px int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch px {
	case 0:
		d.data[key] = storeData{
			value: item{String: value},
			dataType: "string",
			ttl: 0,
		}
	default:
		d.data[key] = storeData{
			value: item{String: value},
			dataType: "string",
			ttl: time.Now().UnixMilli() + px,
		}
	}
}

func (d *Store) Del(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.data, key)
}

func (d *Store) Keys() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	keys := []string{}
	for k := range d.data {
		keys = append(keys, k)
	}
	return keys
}

func (d *Store) cleanUp() {
	for key, value := range d.data {
		if value.ttl > 0 && value.ttl < time.Now().UnixMilli() {
			d.Del(key)
		}
	}
}

func (d *Store) cleanUpRoutine() {
	for {
		time.Sleep(120 * time.Second)
		d.cleanUp()
	}
}