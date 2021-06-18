// Example to test the command line output.
// Expected Redis monitor output: redis-cli -h redis monitor
// OK
// "SELECT" "9"
// "SELECT" "10"
// "SET" "key1" "value1"
// "SELECT" "9"
// "SELECT" "10"
// "SCAN" "0"
// "DUMP" "key1"
//  "RESTORE" "key1" "0" "..." "REPLACE"
// "FLUSHDB"
//  "FLUSHDB"
package run_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/domwong/rump/pkg/config"
	"github.com/domwong/rump/pkg/run"
	"github.com/go-redis/redis/v8"
)

var db1 *redis.Client
var db2 *redis.Client

func setup() {
	opts, _ := redis.ParseURL("redis://localhost:6379/9")
	db1 = redis.NewClient(opts)
	opts, _ = redis.ParseURL("redis://localhost:6379/10")
	db2 = redis.NewClient(opts)

	ctx := context.Background()
	// generate source test data on db1
	for i := 1; i <= 1; i++ {
		k := fmt.Sprintf("key%v", i)
		v := fmt.Sprintf("value%v", i)
		db1.Set(ctx, k, v, 10000*time.Millisecond)
	}
}

func teardown() {
	// Reset test dbs
	ctx := context.Background()
	db1.FlushDB(ctx)
	db2.FlushDB(ctx)

}

func ExampleRun_redisToRedis() {
	setup()
	defer teardown()

	cfg := config.Config{
		Source: config.Resource{
			URI:     "redis://localhost:6379/9",
			IsRedis: true,
		},
		Target: config.Resource{
			URI:     "redis://localhost:6379/10",
			IsRedis: true,
		},
		Silent: false,
	}

	run.Run(cfg)
	// Output:
	// rw
	// signal: exit
	// done
}

func ExampleRun_redisToRedisTTL() {
	setup()
	defer teardown()

	cfg := config.Config{
		Source: config.Resource{
			URI:     "redis://localhost:6379/9",
			IsRedis: true,
		},
		Target: config.Resource{
			URI:     "redis://localhost:6379/10",
			IsRedis: true,
		},
		Silent: false,
		TTL:    true,
	}

	run.Run(cfg)
	// Output:
	// rw
	// signal: exit
	// done
}

func ExampleRun_redisToRedisSilent() {
	setup()
	defer teardown()

	cfg := config.Config{
		Source: config.Resource{
			URI:     "redis://localhost:6379/9",
			IsRedis: true,
		},
		Target: config.Resource{
			URI:     "redis://localhost:6379/10",
			IsRedis: true,
		},
		Silent: true,
	}

	run.Run(cfg)
	// Output:
	// signal: exit
	// done
}

func ExampleRun_redisToFile() {
	setup()
	defer teardown()

	cfg := config.Config{
		Source: config.Resource{
			URI:     "redis://localhost:6379/9",
			IsRedis: true,
		},
		Target: config.Resource{
			URI:     os.TempDir() + "/dump.rump",
			IsRedis: false,
		},
	}

	run.Run(cfg)
	// Output:
	// rw
	// signal: exit
	// done
}

func ExampleRun_redisToFileTTL() {
	setup()
	defer teardown()

	cfg := config.Config{
		Source: config.Resource{
			URI:     "redis://localhost:6379/9",
			IsRedis: true,
		},
		Target: config.Resource{
			URI:     os.TempDir() + "/dump.rump",
			IsRedis: false,
		},
		TTL: true,
	}

	run.Run(cfg)
	// Output:
	// rw
	// signal: exit
	// done
}

func ExampleRun_fileToRedis() {
	setup()
	defer teardown()

	cfgFileDump := config.Config{
		Source: config.Resource{
			URI:     "redis://localhost:6379/9",
			IsRedis: true,
		},
		Target: config.Resource{
			URI:     os.TempDir() + "/dump.rump",
			IsRedis: false,
		},
	}
	run.Run(cfgFileDump)

	cfg := config.Config{
		Source: config.Resource{
			URI:     os.TempDir() + "/dump.rump",
			IsRedis: false,
		},
		Target: config.Resource{
			URI:     "redis://localhost:6379/10",
			IsRedis: true,
		},
	}
	run.Run(cfg)
	// Output:
	// rw
	// signal: exit
	// done
	// rw
	// signal: exit
	// done
}

func ExampleRun_fileToRedisTTL() {
	setup()
	defer teardown()

	cfgFileDump := config.Config{
		Source: config.Resource{
			URI:     "redis://localhost:6379/9",
			IsRedis: true,
		},
		Target: config.Resource{
			URI:     os.TempDir() + "/dump.rump",
			IsRedis: false,
		},
	}
	run.Run(cfgFileDump)

	cfg := config.Config{
		Source: config.Resource{
			URI:     os.TempDir() + "/dump.rump",
			IsRedis: false,
		},
		Target: config.Resource{
			URI:     "redis://localhost:6379/10",
			IsRedis: true,
		},
		TTL: true,
	}
	run.Run(cfg)
	// Output:
	// rw
	// signal: exit
	// done
	// rw
	// signal: exit
	// done
}
