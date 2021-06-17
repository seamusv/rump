package redis_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/domwong/rump/pkg/message"
	"github.com/domwong/rump/pkg/redis"
	rredis "github.com/go-redis/redis/v8"
)

var db1 *rredis.Client
var db2 *rredis.Client
var ch message.Bus
var expected map[string]string

func setup() {
	opts, _ := rredis.ParseURL("redis://localhost:6379/3")
	db1 = rredis.NewClient(opts)
	opts, _ = rredis.ParseURL("redis://localhost:6379/4")
	db2 = rredis.NewClient(opts)
	expected = make(map[string]string)

	ctx := context.Background()
	// generate source test data on db1
	for i := 1; i <= 20; i++ {
		k := fmt.Sprintf("key%v", i)
		v := fmt.Sprintf("value%v", i)
		db1.Set(ctx, k, v, 30*time.Second)
		expected[k] = v
	}
}

func teardown() {
	// Reset test dbs
	ctx := context.Background()
	db1.FlushDB(ctx)
	db2.FlushDB(ctx)
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

// Test db1 to db2 sync
func TestReadWrite(t *testing.T) {
	ch = make(message.Bus, 100)
	source := redis.New(db1, ch, false, false)
	target := redis.New(db2, ch, false, false)
	ctx := context.Background()

	// Read all keys from db1, push to shared message bus
	if err := source.Read(ctx); err != nil {
		t.Error("error: ", err)
	}

	// Write all keys from message bus to db2
	if err := target.Write(ctx); err != nil {
		t.Error("error: ", err)
	}

	// Get all db2 keys
	result := map[string]string{}
	for k := range expected {
		result[k] = db2.Get(ctx, k).Val()
	}

	// Compare db1 keys with db2 keys
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("expected: %v, result: %v", expected, result)
	}
}

// Test db1 to db2 sync with TTL
func TestReadWriteTTL(t *testing.T) {
	ch = make(message.Bus, 100)
	source := redis.New(db1, ch, false, true)
	target := redis.New(db2, ch, false, true)
	ctx := context.Background()

	// Read all keys from db1, push to shared message bus
	if err := source.Read(ctx); err != nil {
		t.Error("error: ", err)
	}

	// Write all keys from message bus to db2
	if err := target.Write(ctx); err != nil {
		t.Error("error: ", err)
	}

	// Get all db2 keys
	result := map[string]string{}
	for k := range expected {
		var err error
		result[k], err = db2.Get(ctx, k).Result()
		if err != nil {
			t.Errorf("Error reading %s", err)
		}
		if db2.PTTL(ctx, k).Val() == 0 {
			t.Errorf("ttl non transferred")
		}
	}

	// Compare db1 keys with db2 keys
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("expected: %v, result: %v", expected, result)
	}
}
