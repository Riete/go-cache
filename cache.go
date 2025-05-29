package cache

import (
	"context"
	"hash/crc32"
	"math/bits"
	"sync"
	"time"
)

type IteratorEntry[T any] struct {
	Key   string
	Value T
}

type Cache[T any] struct {
	shards []*storage[T]
	mask   uint64
}

func (c *Cache[T]) Get(k string) (T, bool) {
	return c.storage(k).get(k)
}

func (c *Cache[T]) Exists(k string) bool {
	return c.storage(k).exists(k)
}

// Set expiration == 0 means has no expiration time
func (c *Cache[T]) Set(k string, v T, expiration time.Duration) {
	c.storage(k).set(k, v, expiration)
}

// SetNX set key if it does not already exist, expiration == 0 means has no expiration time
func (c *Cache[T]) SetNX(k string, v T, expiration time.Duration) bool {
	return c.storage(k).setNX(k, v, expiration)
}

// SetXX set key if it already exists, expiration == 0 means has no expiration time
func (c *Cache[T]) SetXX(k string, v T, expiration time.Duration) bool {
	return c.storage(k).setXX(k, v, expiration)
}

// Renew increase "ttl" expiration time, or decrease if ttl is negative
func (c *Cache[T]) Renew(k string, ttl time.Duration) (success bool) {
	return c.storage(k).renew(k, ttl)
}

func (c *Cache[T]) Delete(k string) {
	c.storage(k).delete(k)
}

func (c *Cache[T]) Keys(f KeyFunc) []string {
	wg := new(sync.WaitGroup)
	wg.Add(len(c.shards))
	ch := make(chan []string, len(c.shards))
	var keys []string
	go func() {
		defer close(ch)
		for _, shard := range c.shards {
			go func(shard *storage[T]) {
				defer wg.Done()
				ch <- shard.keys(f)
			}(shard)
		}
		wg.Wait()
	}()
	for i := range ch {
		keys = append(keys, i...)
	}
	return keys
}

func (c *Cache[T]) Iterator(ctx context.Context, f KeyFunc) chan IteratorEntry[T] {
	wg := new(sync.WaitGroup)
	ch := make(chan IteratorEntry[T], 100)
	go func() {
		defer close(ch)
		for _, shard := range c.shards {
			select {
			case <-ctx.Done():
				return
			default:
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range shard.iterator(ctx, f) {
						ch <- i
					}
				}()
			}
		}
		wg.Wait()
	}()
	return ch
}

func (c *Cache[T]) Clear() {
	for _, shard := range c.shards {
		shard.clear()
	}
}

func (c *Cache[T]) storage(k string) *storage[T] {
	hash := uint64(crc32.ChecksumIEEE([]byte(k)))
	return c.shards[hash&c.mask]
}

func newCache[T any](config *Config) *Cache[T] {
	config.ShardsNum = 1 << bits.Len64(config.ShardsNum-1)
	shards := make([]*storage[T], config.ShardsNum)
	for i := 0; i < int(config.ShardsNum); i++ {
		shards[i] = newStorage[T](config)
	}
	return &Cache[T]{shards: shards, mask: config.ShardsNum - 1}
}

func New[T any](config *Config) *Cache[T] {
	if config == nil {
		config = DefaultConfig.Clone()
	}
	config.CompactFactor = min(config.CompactFactor, maxCompactFactor)
	config.MinDeletion = max(config.MinDeletion, defaultMinDeletion)
	if config.ShardsNum == 0 {
		config.ShardsNum = defaultShardsNum
	}
	return newCache[T](config.Clone())
}
