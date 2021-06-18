// Package file_test is the end-to-end test for Rump.
// Since we need the real Redis DUMP protocol to test files,
// we use pkg/redis as a data generator, violating Unit Testing
// principles.
package file_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/domwong/rump/pkg/file"
	"github.com/domwong/rump/pkg/message"
	"github.com/domwong/rump/pkg/redis"
	rredis "github.com/go-redis/redis/v8"
)

var db1 *rredis.Client
var db2 *rredis.Client
var ch message.Bus
var expected map[string]string
var path string

func setup() {
	opts, _ := rredis.ParseURL("redis://localhost:6379/3")
	db1 = rredis.NewClient(opts)
	opts, _ = rredis.ParseURL("redis://localhost:6379/4")
	db2 = rredis.NewClient(opts)
	ch = make(message.Bus, 100)
	expected = make(map[string]string)

	path = os.Getenv("TMPDIR") + "/dump.rump"
	ctx := context.Background()

	// generate source test data
	for i := 1; i <= 20; i++ {
		k := fmt.Sprintf("key%v", i)
		v := fmt.Sprintf("value%v", i)
		db1.Set(ctx, k, v, 0)
		expected[k] = v
	}
}

func teardown() {
	// Reset test dbs
	ctx := context.Background()
	db1.FlushDB(ctx)
	db2.FlushDB(ctx)
	// Delete dump file
	os.Remove(path)
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

func TestWriteRead(t *testing.T) {
	ctx := context.Background()
	// Read all keys from db1, push to shared message bus
	source := redis.New(db1, ch, false, false)
	if err := source.Read(ctx); err != nil {
		t.Error("error: ", err)
	}

	// Write rump dump from shared message bus
	target := file.New(path, ch, false, false)
	if err := target.Write(ctx); err != nil {
		t.Error("error: ", err)
	}

	ctx = context.Background()
	// Create second channel to test reading from file
	ch2 := make(message.Bus, 100)

	// Read rump dump file
	source2 := file.New(path, ch2, false, false)
	if err := source2.Read(ctx); err != nil {
		t.Error("error: ", err)
	}

	// Write from shared message bus to db2
	target2 := redis.New(db2, ch2, false, false)
	if err := target2.Write(ctx); err != nil {
		t.Error("error: ", err)
	}

	// Get all db2 keys
	result := map[string]string{}
	for k := range expected {
		res, err := db2.Get(ctx, k).Result()
		if err != nil {

			t.Errorf("Error getting for key %s %s", k, err)
		}
		result[k] = res
	}

	// Compare db1 keys with db2 keys
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("expected: %v, result: %v", expected, result)
	}
}
