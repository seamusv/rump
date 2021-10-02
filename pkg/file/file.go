package file

import (
	"bufio"
	"context"
	"fmt"
	"github.com/domwong/rump/pkg/message"
	gogoio "github.com/gogo/protobuf/io"
	"io"
	"os"
)

// File can read and write, to a file Path, using the message Bus.
type File struct {
	Path   string
	Bus    message.Bus
	Silent bool
	TTL    bool
}

// New creates the File struct, to be used for reading/writing.
func New(path string, bus message.Bus, silent, ttl bool) *File {
	return &File{
		Path:   path,
		Bus:    bus,
		Silent: silent,
		TTL:    ttl,
	}
}

// Log read/write operations unless silent mode enabled
func (f *File) maybeLog(s string) {
	if f.Silent {
		return
	}
	fmt.Print(s)
}

// Read scans a Rump file and sends Payloads to the message bus.
func (f *File) Read(ctx context.Context) error {
	defer close(f.Bus)

	d, err := os.Open(f.Path)
	if err != nil {
		return err
	}
	defer d.Close()

	fmt.Println(f.Path)
	prdr := gogoio.NewDelimitedReader(d, 1024*1024*600)

	for {
		msg := &message.Payload{}
		if err := prdr.ReadMsg(msg); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		select {
		case <-ctx.Done():
			fmt.Println("")
			fmt.Println("file read: exit " + ctx.Err().Error())
			return ctx.Err()
		case f.Bus <- *msg:
			f.maybeLog("r")
		}
	}

	return nil
}

// Write writes to a Rump file Payloads from the message bus.
func (f *File) Write(ctx context.Context) error {
	d, err := os.Create(f.Path)
	if err != nil {
		return err
	}
	defer d.Close()

	// Buffered write to limit system IO calls
	w := bufio.NewWriter(d)
	wp := gogoio.NewDelimitedWriter(w)

	// Flush last open buffers
	defer w.Flush()

	for f.Bus != nil {
		select {
		// Exit early if context done.
		case <-ctx.Done():
			fmt.Println("")
			fmt.Println("file write: exit")
			return ctx.Err()
		// Get Messages from Bus
		case p, ok := <-f.Bus:
			// if channel closed, set to nil, break loop
			if !ok {
				f.Bus = nil
				continue
			}
			if err := wp.WriteMsg(&p); err != nil {
				return err
			}
			f.maybeLog("w")
		}
	}

	return nil
}
