// Package redis allows reading/writing from/to a Redis DB.
package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/domwong/rump/pkg/message"
	"github.com/go-redis/redis/v8"
)

// Redis holds references to a DB pool and a shared message bus.
// Silent disables verbose mode.
// TTL enables TTL sync.
type Redis struct {
	client *redis.Client
	//Pool   *radix.Pool
	Bus    message.Bus
	Silent bool
	TTL    bool
}

// New creates the Redis struct, used to read/write.
func New(source *redis.Client, bus message.Bus, silent, ttl bool) *Redis {
	return &Redis{
		client: source,
		Bus:    bus,
		Silent: silent,
		TTL:    ttl,
	}
}

// maybeLog may log, depending on the Silent flag
func (r *Redis) maybeLog(s string) {
	if r.Silent {
		return
	}
	fmt.Print(s)
}

// maybeTTL may sync the TTL, depending on the TTL flag
func (r *Redis) maybeTTL(key string) (string, error) {
	// noop if TTL is disabled, speeds up sync process
	if !r.TTL {
		return "0", nil
	}

	var ttl string

	// Try getting key TTL.
	res, err := r.client.PTTL(context.Background(), key).Result()
	if err != nil && err != redis.Nil {
		return ttl, err
	}

	// When key has no expire PTTL returns "-1".
	// We set it to 0, default for no expiration time.
	if res == time.Duration(-1) {
		ttl = "0"
	}

	return ttl, nil
}

// Read gently scans an entire Redis DB for keys, then dumps
// the key/value pair (Payload) on the message Bus channel.
// It leverages implicit pipelining to speedup large DB reads.
// To be used in an ErrGroup.
func (r *Redis) Read(ctx context.Context) error {
	defer close(r.Bus)

	var cursor uint64 = 0

	var ttl string

	// Scan and push to bus until no keys are left.
	// If context Done, exit early.
	for {
		var keys []string
		var err error
		keys, cursor, err = r.client.Scan(ctx, cursor, "", 400).Result()
		if err != nil && err != redis.Nil {
			return err
		}
		for _, key := range keys {
			start := time.Now()
			value, err := r.client.Dump(ctx, key).Result()
			if err != nil {
				fmt.Printf("key %s with error %s after %s\n", key, err, time.Since(start))
				continue
			}

			ttl, err = r.maybeTTL(key)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				fmt.Println("")
				fmt.Println("redis read: exit")
				return ctx.Err()
			case r.Bus <- message.Payload{Key: key, Value: value, Ttl: ttl}:
				r.maybeLog("r")
			}

		}
		if cursor == 0 {
			return nil
		}
	}

	return nil
}

// Write restores keys on the db as they come on the message bus.
func (r *Redis) Write(ctx context.Context) error {
	// Loop until channel is open
	for r.Bus != nil {
		select {
		// Exit early if context done.
		case <-ctx.Done():
			fmt.Println("")
			fmt.Println("redis write: exit")
			return ctx.Err()
		// Get Messages from Bus
		case p, ok := <-r.Bus:
			// if channel closed, set to nil, break loop
			if !ok {
				r.Bus = nil
				continue
			}
			ttl, _ := strconv.Atoi(p.Ttl)
			if ttl < 0 {
				ttl = 0
			}
			if err := r.client.RestoreReplace(ctx, p.Key, time.Duration(ttl)*time.Millisecond, p.Value).Err(); err != nil {
				return err
			}
			r.maybeLog("w")
		}
	}

	return nil
}
