package cache

import (
	"context"
	"maps"
	"sync"
	"time"
)

type object[T any] struct {
	obj        T
	expiredAt  time.Time
	persistent bool
}

func (o *object[T]) expired() bool {
	return !o.persistent && time.Now().After(o.expiredAt)
}

func (o *object[T]) setExpiration(expiration time.Duration) {
	if expiration == 0 {
		o.persistent = true
	} else {
		o.expiredAt = time.Now().Add(expiration)
	}
}

func (o *object[T]) reset() {
	o.persistent = false
	o.obj = *new(T)
}

type storage[T any] struct {
	m             map[string]*object[T]
	mu            sync.RWMutex
	deletionCount float64
	config        *Config
	op            sync.Pool
}

func (s *storage[T]) get(k string) (T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	obj, exists := s.m[k]
	if !exists {
		return *new(T), false
	}
	if obj.expired() {
		go s.delete(k)
		return *new(T), false
	}
	return obj.obj, true
}

func (s *storage[T]) exists(k string) bool {
	_, ok := s.get(k)
	return ok
}

func (s *storage[T]) set(k string, v T, expiration time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, exists := s.m[k]; exists {
		old.reset()
		s.op.Put(old)
	}
	obj := s.op.Get().(*object[T])
	obj.obj = v
	obj.setExpiration(expiration)
	s.m[k] = obj
}

func (s *storage[T]) setXX(k string, v T, expiration time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, exists := s.m[k]
	if !exists {
		return false
	}
	old.reset()
	s.op.Put(old)
	obj := s.op.Get().(*object[T])
	obj.obj = v
	obj.setExpiration(expiration)
	s.m[k] = obj
	return true
}

func (s *storage[T]) setNX(k string, v T, expiration time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, exists := s.m[k]
	if exists {
		if !old.expired() {
			return false
		}
		old.reset()
		s.op.Put(old)
	}
	obj := s.op.Get().(*object[T])
	obj.obj = v
	obj.setExpiration(expiration)
	s.m[k] = obj
	return true
}

func (s *storage[T]) renew(k string, ttl time.Duration) (success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if o, ok := s.m[k]; ok && !o.expired() {
		o.expiredAt = o.expiredAt.Add(ttl)
		success = true
	}
	return
}

func (s *storage[T]) delete(keys ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		obj := s.m[k]
		obj.reset()
		s.op.Put(obj)
		delete(s.m, k)
	}
	if s.config.EnableCompact() {
		s.deletionCount += float64(len(keys))
		if s.deletionCount > s.config.MinDeletion {
			ratio := float64(len(s.m)) / s.config.MinDeletion
			if ratio < s.config.CompactFactor {
				s.m = maps.Clone(s.m)
				s.deletionCount = 0
			} else {
				delta := ratio - s.config.CompactFactor
				s.deletionCount = (1 - delta) * s.config.MinDeletion
			}
		}
	}
}

func (s *storage[T]) keys(f KeyFunc) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if f == nil {
		f = MatchAll
	}
	var keys []string
	var expired []string
	for k, v := range s.m {
		if v.expired() {
			expired = append(expired, k)
			continue
		}
		if f(k) {
			keys = append(keys, k)
		}
	}
	if len(expired) > 0 {
		go s.delete(expired...)
	}
	return keys
}

func (s *storage[T]) iterator(ctx context.Context, f KeyFunc) chan IteratorEntry[T] {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if f == nil {
		f = MatchAll
	}
	ch := make(chan IteratorEntry[T])
	go func() {
		var expired []string
		defer func() {
			if len(expired) > 0 {
				go s.delete(expired...)
			}
			close(ch)
		}()
		for k, v := range s.m {
			select {
			case <-ctx.Done():
				return
			default:
				if v.expired() {
					expired = append(expired, k)
					continue
				}
				if f(k) {
					ch <- IteratorEntry[T]{Key: k, Value: v.obj}
				}
			}
		}
	}()
	return ch
}

func (s *storage[T]) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m = make(map[string]*object[T])
	s.deletionCount = 0
}

func newStorage[T any](config *Config) *storage[T] {
	return &storage[T]{
		m:      make(map[string]*object[T]),
		config: config,
		op: sync.Pool{
			New: func() any {
				return new(object[T])
			},
		},
	}
}
