package relay

import (
	"bytes"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
)

// Publisher is a type that is used only for publishing messages to a single queue.
// Multiple Publishers can multiplex a single relay
type Publisher struct {
	conf        *Config
	queue       string
	channel     *amqp.Channel
	contentType string
	mode        uint8
	buf         bytes.Buffer
	ackCh       chan uint64
	nackCh      chan uint64
	errCh       chan *amqp.Error
}

// Publish will send the message to the server to be consumed
func (p *Publisher) Publish(in interface{}) error {
	// Check for close
	if p.channel == nil {
		return ChannelClosed
	}

	// Encode the message
	conf := p.conf
	buf := &p.buf
	buf.Reset()
	if err := conf.Serializer.Encode(buf, in); err != nil {
		return fmt.Errorf("Failed to encode message! Got: %s", err)
	}

	// Format the message
	msg := amqp.Publishing{
		DeliveryMode: p.mode,
		Timestamp:    time.Now().UTC(),
		ContentType:  p.contentType,
		Body:         buf.Bytes(),
	}

	// Publish the message
	if err := p.channel.Publish(conf.Exchange, p.queue, false, false, msg); err != nil {
		return fmt.Errorf("Failed to publish to '%s'! Got: %s", p.queue, err)
	}

	// Check if we wait for confirmation
	if !p.conf.DisablePublishConfirm {
		select {
		case _, ok := <-p.ackCh:
			if !ok {
				return ChannelClosed
			}
			return nil
		case _, ok := <-p.nackCh:
			if !ok {
				return ChannelClosed
			}
			return fmt.Errorf("Failed to publish to '%s'! Got negative ack.", p.queue)
		case err, ok := <-p.errCh:
			if !ok {
				return ChannelClosed
			}
			log.Printf("[ERR] Publisher got error: (Code %d Server: %v Recoverable: %v) %s",
				err.Code, err.Server, err.Recover, err.Reason)
			return fmt.Errorf("Failed to publish to '%s'! Got: %s", err.Error())
		}
	}
	return nil
}

// Close will shutdown the publisher
func (p *Publisher) Close() error {
	// Make sure close is idempotent
	if p.channel == nil {
		return nil
	}
	defer func() {
		p.channel = nil
	}()
	return p.channel.Close()
}
