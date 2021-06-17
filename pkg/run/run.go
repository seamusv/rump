// Package run manages Read, Write and Signal goroutines.
package run

import (
	"context"
	"fmt"
	"os"

	rredis "github.com/go-redis/redis/v8"
	"golang.org/x/sync/errgroup"

	"github.com/domwong/rump/pkg/config"
	"github.com/domwong/rump/pkg/file"
	"github.com/domwong/rump/pkg/message"
	"github.com/domwong/rump/pkg/redis"
	"github.com/domwong/rump/pkg/signal"
)

// Exit helper
func exit(e error) {
	fmt.Println(e)
	os.Exit(1)
}

// Run orchestrate the Reader, Writer and Signal handler.
func Run(cfg config.Config) {
	// create ErrGroup to manage goroutines
	ctx, cancel := context.WithCancel(context.Background())
	g, gctx := errgroup.WithContext(ctx)

	// Start signal handling goroutine
	g.Go(func() error {
		return signal.Run(gctx, cancel)
	})

	// Create shared message bus
	ch := make(message.Bus, 100)

	// Create and run either a Redis or File Source reader.
	if cfg.Source.IsRedis {
		opts, err := rredis.ParseURL(cfg.Source.URI)
		if err != nil {
			exit(err)
		}

		c := rredis.NewClient(opts)

		source := redis.New(c, ch, cfg.Silent, cfg.TTL)

		g.Go(func() error {
			return source.Read(gctx)
		})
	} else {
		source := file.New(cfg.Source.URI, ch, cfg.Silent, cfg.TTL)

		g.Go(func() error {
			return source.Read(gctx)
		})
	}

	// Create and run either a Redis or File Target writer.
	if cfg.Target.IsRedis {
		opts, err := rredis.ParseURL(cfg.Target.URI)
		if err != nil {
			exit(err)
		}

		c := rredis.NewClient(opts)

		target := redis.New(c, ch, cfg.Silent, cfg.TTL)

		g.Go(func() error {
			defer cancel()
			return target.Write(gctx)
		})
	} else {
		target := file.New(cfg.Target.URI, ch, cfg.Silent, cfg.TTL)

		g.Go(func() error {
			defer cancel()
			return target.Write(gctx)
		})
	}

	// Block and wait for goroutines
	err := g.Wait()
	if err != nil && err != context.Canceled {
		exit(err)
	} else {
		fmt.Println("done")
	}
}
