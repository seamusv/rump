package main

import (
	"github.com/domwong/rump/pkg/config"
	"github.com/domwong/rump/pkg/run"
)

func main() {
	// parse config flags, will exit in case of errors.
	cfg := config.Parse()

	run.Run(cfg)
}
