package cache

import (
	"hash/crc32"
	"maps"
	"sync"
	"time"
)

const (
	defaultMinDeletion   = 10000
	defaultCompactFactor = 0.3
	defaultShardsNum     = 10
	defaultTTL           = 5 * time.Minute
	maxCompactFactor     = 0.7
)

// Config
// if deletionCount > MinDeletion && len(map) / MinDeletion < CompactFactor, do map compact
// CompactFactor == 0 means disable compact
// CompactFactor value range should be from 0 to 0.7
type Config struct {
	CompactFactor float64
	MinDeletion   float64
	ShardsNum     int
	TTL           time.Duration
}

var DefaultConfig = &Config{
	CompactFactor: defaultCompactFactor,
	MinDeletion:   defaultMinDeletion,
	ShardsNum:     defaultShardsNum,
	TTL:           defaultTTL,
}

type object[T any] struct {
	obj       T
	expiredAt time.Time
}

func (o object[T]) expired() bool {
	return time.Now().After(o.expiredAt)
}

type storage[T any] struct {
	m             map[string]*object[T]
	rw            sync.RWMutex
	deletionCount float64
	config        *Config
}

func (s *storage[T]) get(k string) (T, bool) {
	s.rw.RLock()
	defer s.rw.RUnlock()
	o, ok := s.m[k]
	if !ok {
		return *new(T), false
	}
	if o.expired() {
		go s.delete(k)
		return *new(T), false
	}
	return o.obj, true
}

func (s *storage[T]) exists(k string) bool {
	_, ok := s.get(k)
	return ok
}

func (s *storage[T]) set(k string, v T) {
	s.setWithTTL(k, v, s.config.TTL)
}

func (s *storage[T]) setWithTTL(k string, v T, ttl time.Duration) {
	s.rw.Lock()
	defer s.rw.Unlock()
	s.m[k] = &object[T]{obj: v, expiredAt: time.Now().Add(ttl)}
}

func (s *storage[T]) setIfNX(k string, v T) bool {
	return s.setIfNXWithTTL(k, v, s.config.TTL)
}

func (s *storage[T]) setIfNXWithTTL(k string, v T, ttl time.Duration) bool {
	s.rw.Lock()
	defer s.rw.Unlock()
	if o, ok := s.m[k]; !ok || o.expired() {
		s.m[k] = &object[T]{obj: v, expiredAt: time.Now().Add(ttl)}
		return true
	}
	return false
}

func (s *storage[T]) adjustExpirationTime(k string, offset time.Duration) {
	s.rw.Lock()
	defer s.rw.Unlock()
	if o, ok := s.m[k]; ok && !o.expired() {
		o.expiredAt = o.expiredAt.Add(offset)
	}
}

func (s *storage[T]) adjustExpiredAt(k string, ex time.Time) {
	s.rw.Lock()
	defer s.rw.Unlock()
	if o, ok := s.m[k]; ok && !o.expired() {
		o.expiredAt = ex
	}
}

func (s *storage[T]) delete(k string) {
	s.rw.Lock()
	defer s.rw.Unlock()
	delete(s.m, k)
	if s.config.CompactFactor > 0 {
		s.deletionCount++
		if s.deletionCount > s.config.MinDeletion {
			restPercent := float64(len(s.m)) / s.config.MinDeletion
			if restPercent < s.config.CompactFactor {
				s.m = maps.Clone(s.m)
				s.deletionCount = 0
			} else {
				delta := restPercent - s.config.CompactFactor
				s.deletionCount = (1 - delta) * s.config.MinDeletion
			}
		}
	}
}

func (s *storage[T]) keys() []string {
	s.rw.RLock()
	defer s.rw.RUnlock()
	var keys []string
	for k, v := range s.m {
		if v.expired() {
			go s.delete(k)
		} else {
			keys = append(keys, k)
		}
	}
	return keys
}

func (s *storage[T]) clear() {
	s.rw.Lock()
	defer s.rw.Unlock()
	s.m = make(map[string]*object[T])
	s.deletionCount = 0
}

func newStorage[T any](config *Config) *storage[T] {
	return &storage[T]{m: make(map[string]*object[T]), config: config}
}

type Storage[T any] struct {
	shards []*storage[T]
}

func (s *Storage[T]) storage(k string) *storage[T] {
	index := crc32.ChecksumIEEE([]byte(k))
	return s.shards[int(index)%len(s.shards)]
}

func (s *Storage[T]) Get(k string) (T, bool) {
	return s.storage(k).get(k)
}

func (s *Storage[T]) Exists(k string) bool {
	return s.storage(k).exists(k)
}

func (s *Storage[T]) Set(k string, v T) {
	s.storage(k).set(k, v)
}

func (s *Storage[T]) SetWithTTL(k string, v T, ttl time.Duration) {
	s.storage(k).setWithTTL(k, v, ttl)
}

func (s *Storage[T]) SetIfNX(k string, v T) bool {
	return s.storage(k).setIfNX(k, v)
}

func (s *Storage[T]) SetIfNXWithTTL(k string, v T, ttl time.Duration) bool {
	return s.storage(k).setIfNXWithTTL(k, v, ttl)
}

func (s *Storage[T]) AdjustExpirationTime(k string, offset time.Duration) {
	s.storage(k).adjustExpirationTime(k, offset)
}

func (s *Storage[T]) AdjustExpiredAt(k string, ex time.Time) {
	s.storage(k).adjustExpiredAt(k, ex)
}

func (s *Storage[T]) Delete(k string) {
	s.storage(k).delete(k)
}

func (s *Storage[T]) Keys() []string {
	var keys []string
	for _, shard := range s.shards {
		keys = append(keys, shard.keys()...)
	}
	return keys
}

func (s *Storage[T]) Clear() {
	for _, shard := range s.shards {
		shard.clear()
	}
}

func New[T any](config *Config) *Storage[T] {
	if config == nil {
		config = DefaultConfig
	}
	if config.CompactFactor > maxCompactFactor {
		config.CompactFactor = maxCompactFactor
	}
	if config.MinDeletion < defaultMinDeletion {
		config.MinDeletion = defaultMinDeletion
	}
	var shards []*storage[T]
	for i := config.ShardsNum; i > 0; i-- {
		shards = append(shards, newStorage[T](config))
	}
	return &Storage[T]{shards: shards}
}
