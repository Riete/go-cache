package cache

const (
	defaultMinDeletion   = 10000
	defaultCompactFactor = 0.3
	defaultShardsNum     = 8
	maxCompactFactor     = 0.7
)

// Config
// if deletionCount > MinDeletion && len(map) / MinDeletion < CompactFactor, do map compact
// CompactFactor == 0 means disable compact
// CompactFactor value range should be from 0 to 0.7
// ShardsNum should be 2^N
type Config struct {
	CompactFactor float64
	MinDeletion   float64
	ShardsNum     uint64
}

func (c *Config) Clone() *Config {
	return &Config{
		CompactFactor: c.CompactFactor,
		MinDeletion:   c.MinDeletion,
		ShardsNum:     c.ShardsNum,
	}
}

func (c *Config) EnableCompact() bool {
	return c.CompactFactor > 0 && c.CompactFactor <= maxCompactFactor
}

var DefaultConfig = &Config{
	CompactFactor: defaultCompactFactor,
	MinDeletion:   defaultMinDeletion,
	ShardsNum:     defaultShardsNum,
}
