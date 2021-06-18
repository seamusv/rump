// Package file allows reading/writing from/to a Rump file.
// Rump file protocol is key✝✝value✝✝ttl✝✝key✝✝value✝✝ttl✝✝...
package file

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/domwong/rump/pkg/message"
)

// File can read and write, to a file Path, using the message Bus.
type File struct {
	Path   string
	Bus    message.Bus
	Silent bool
	TTL    bool
}

// splitCross is a double-cross (✝✝) custom Scanner Split.
func splitCross(data []byte, atEOF bool) (advance int, token []byte, err error) {

	// end of file
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	// Split at separator
	if i := strings.Index(string(data), "✝✝"); i >= 0 {
		// Separator is 6 bytes long
		return i + 6, data[0:i], nil
	}

	return 0, nil, nil
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
	rdr := bufio.NewReader(d)

	// Scan line by line
	// file protocol is key✝✝value✝✝ttl✝✝
	for {

		scan := func() (string, error) {
			builder:=strings.Builder{}
			for {
				b, err := rdr.Peek(4096)
				if (err != nil && err != io.EOF) || (b == nil || len(b) == 0) {
					return "", err
				}

				// special cases
				// if currentBuf ends with ✝ and b starts with ✝
				peeked := string(b)
				if strings.HasPrefix(peeked,"✝") && strings.HasSuffix(builder.String(), "✝"){
					// found it
					rdr.Discard(3)
					return strings.TrimSuffix(builder.String(), "✝"), nil
				}

				if idx:=strings.Index(peeked, "✝✝"); idx > -1 {
					rdr.Discard(idx  + 6)
					builder.WriteString(peeked[:idx])
					break
				}
				builder.WriteString(peeked)
				rdr.Discard(4096)
			}
			return builder.String(), nil
		}

		key, err := scan()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		value, err := scan()
		if err != nil {
			return err
		}
		ttl, err := scan()
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			fmt.Println("")
			fmt.Println("file read: exit " + ctx.Err().Error())
			return ctx.Err()
		case f.Bus <- message.Payload{Key: key, Value: value, TTL: ttl}:
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
			_, err := w.WriteString(p.Key + "✝✝" + p.Value + "✝✝" + p.TTL + "✝✝")
			if err != nil {
				return err
			}
			f.maybeLog("w")
		}
	}

	return nil
}
