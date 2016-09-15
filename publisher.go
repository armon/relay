package relay

import (
	"bytes"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"strconv"
	"sync"
	"time"
)

// Publisher is a type that is used only for publishing messages to a single queue.
// Multiple Publishers can multiplex a single relay
type Publisher struct {
	conf        *Config
	queue       string
	key         string
	channel     *amqp.Channel
	contentType string
	mode        uint8
	ackCh       chan uint64
	nackCh      chan uint64
	errCh       chan *amqp.Error
	buf         bytes.Buffer
	l           sync.Mutex
}

// Publish will send the message to the server to be consumed
func (p *Publisher) Publish(in interface{}) error {
	p.l.Lock()
	defer p.l.Unlock()

	// Check for close
	if p.channel == nil {
		return ChannelClosed
	}

	// Encode the message
	conf := p.conf
	buf := &p.buf
	buf.Reset()
	if err := conf.Serializer.RelayEncode(buf, in); err != nil {
		return fmt.Errorf("Failed to encode message! Got: %s", err)
	}

	// Format the message
	msg := amqp.Publishing{
		DeliveryMode: p.mode,
		Timestamp:    time.Now().UTC(),
		ContentType:  p.contentType,
		Body:         buf.Bytes(),
	}

	// Check for a message ttl
	if p.conf.MessageTTL > 0 {
		msec := int(p.conf.MessageTTL / time.Millisecond)
		msg.Expiration = strconv.Itoa(msec)
	}

	// Publish the message
	if err := p.channel.Publish(conf.Exchange, p.key, false, false, msg); err != nil {
		return fmt.Errorf("Failed to publish to %q (key %q)! Got: %s", p.queue, p.key, err)
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
			return fmt.Errorf("Failed to publish to '%s'! Got: %s", p.queue, err.Error())
		}
	}
	return nil
}

// Close will shutdown the publisher
func (p *Publisher) Close() error {
	p.l.Lock()
	defer p.l.Unlock()

	// Make sure close is idempotent
	if p.channel == nil {
		return nil
	}
	defer func() {
		p.channel = nil
	}()
	return p.channel.Close()
}
