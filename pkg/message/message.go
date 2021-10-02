// Package message represents the message bus.
// Message Payloads pass through a Bus channel.
package message

// Bus is a channel where message Payloads pass.
type Bus chan Payload
