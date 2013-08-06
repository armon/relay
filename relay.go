package relay

import (
	"bytes"
	"crypto/rand"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"github.com/streadway/amqp"
	"io"
	"os"
	"runtime"
	"sync"
	"time"
)

// Serializer interface is used to encode and
// decode messages. If not provided, a default serializer
// using gob is provided.
type Serializer interface {
	ContentType() string
	Encode(io.Writer, interface{}) error
	Decode(io.Reader, interface{}) error
}

// Config is passed into New when creating a Relay to tune
// various parameters around broker interactions.
type Config struct {
	Addr               string     // Host address to dial
	Port               int        // Host por to bind
	Vhost              string     // Broker Vhost
	Username           string     // Broker username
	Password           string     // Broker password
	DisableTLS         bool       // Broker TLS connection
	PrefetchCount      int        // How many messages to prefetch
	EnableMultiAck     bool       // Controls if we allow multi acks
	DisablePersistence bool       // Disables persistence
	Exchange           string     // Custom exchange if doing override
	Serializer         Serializer // Used to encode messages
}

type Relay struct {
	sync.Mutex
	conf     *Config
	pubConn  *amqp.Connection // Publisher connection.
	consConn *amqp.Connection // Consumer connection. Avoid TCP backpressure.
}

// Publisher is a type that is used only for publishing messages to a single queue.
// Multiple Publishers can multiplex a single relay
type Publisher struct {
	conf        *Config
	queue       string
	channel     *amqp.Channel
	contentType string
	mode        uint8
	buf         bytes.Buffer
}

// Consumer is a type that is used only for consuming messages from a single queue.
// Multiple Consumers can multiplex a single relay
type Consumer struct {
	conf        *Config
	consName    string
	queue       string
	channel     *amqp.Channel
	deliverChan <-chan amqp.Delivery
	lastMsg     uint64 // Last delivery tag, used for Ack
	needAck     bool
}

// New will create a new Relay that can be used to create
// new publishers or consumers.
func New(c *Config) (*Relay, error) {
	// Set the defaults if missing
	if c.Addr == "" {
		c.Addr = "localhost"
	}
	if c.Port == 0 {
		if c.DisableTLS {
			c.Port = 5672
		} else {
			c.Port = 5671
		}
	}
	if c.Vhost == "" {
		c.Vhost = "/"
	}
	if c.Username == "" {
		c.Username = "guest"
	}
	if c.Password == "" {
		c.Password = "guest"
	}
	if c.Exchange == "" {
		c.Exchange = "relay"
	}
	if c.Serializer == nil {
		c.Serializer = &GOBSerializer{}
	}

	// Create relay with finalizer
	r := &Relay{conf: c}
	runtime.SetFinalizer(r, (*Relay).Close)
	return r, nil
}

// Used to get a new server connection
func (r *Relay) getConn() (*amqp.Connection, error) {
	conf := r.conf
	uri := amqp.URI{Host: conf.Addr, Port: conf.Port,
		Username: conf.Username, Password: conf.Password,
		Vhost: conf.Vhost}
	if conf.DisableTLS {
		uri.Scheme = "amqp"
	} else {
		uri.Scheme = "amqps"
	}
	uri_s := uri.String()
	return amqp.Dial(uri_s)
}

// Used to get a new channel, possibly on a cached connection
func (r *Relay) getChan(conn **amqp.Connection) (*amqp.Channel, error) {
	// Prevent multiple connection opens
	r.Lock()
	defer r.Unlock()

	// Get a connection if none
	var isNew bool
	if *conn == nil {
		newConn, err := r.getConn()
		if err != nil {
			return nil, err
		}
		*conn = newConn
		isNew = true
	}

	// Get a channel
	ch, err := (*conn).Channel()
	if err != nil {
		return nil, err
	}

	// Declare an exchange if this is a new connection
	if isNew {
		if err := ch.ExchangeDeclare(r.conf.Exchange, "direct", true, false, false, false, nil); err != nil {
			return nil, fmt.Errorf("Failed to declare exchange '%s'! Got: %s", r.conf.Exchange, err)
		}
	}

	// Return the channel
	return ch, nil
}

// Ensures the given queue exists and is bound to the exchange
func (r *Relay) declareQueue(ch *amqp.Channel, name string) error {
	// Declare the queue
	if _, err := ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
		return fmt.Errorf("Failed to declare queue '%s'! Got: %s", name, err)
	}

	// Bind the queue to the exchange
	if err := ch.QueueBind(name, name, r.conf.Exchange, false, nil); err != nil {
		return fmt.Errorf("Failed to bind queue '%s'! Got: %s", name, err)
	}
	return nil
}

// Close will shutdown the relay. It is best to first Close all the
// Consumer and Publishers, as this will close the underlying connections.
func (r *Relay) Close() error {
	// Prevent multiple connection closes
	r.Lock()
	defer r.Unlock()

	var errors []error
	if r.pubConn != nil {
		if err := r.pubConn.Close(); err != nil {
			errors = append(errors, err)
		}
		r.pubConn = nil
	}
	if r.consConn != nil {
		if err := r.consConn.Close(); err != nil {
			errors = append(errors, err)
		}
		r.consConn = nil
	}
	switch len(errors) {
	case 1:
		return errors[0]
	case 2:
		return fmt.Errorf("Failed to Close! Got %s and %s", errors[0], errors[1])
	default:
		return nil
	}
}

// Consumer will return a new handle that can be used
// to consume messages from a given queue.
func (r *Relay) Consumer(queue string) (*Consumer, error) {
	// Get a new channel
	ch, err := r.getChan(&r.consConn)
	if err != nil {
		return nil, err
	}

	// Ensure the queue exists
	name := queueName(queue)
	if err := r.declareQueue(ch, name); err != nil {
		return nil, err
	}

	// Set the QoS if necessary
	if r.conf.PrefetchCount > 0 {
		if err := ch.Qos(r.conf.PrefetchCount, 0, false); err != nil {
			return nil, fmt.Errorf("Failed to set Qos prefetch! Got: %s", err)
		}
	}

	// Get a consumer name
	consName, err := channelName()
	if err != nil {
		return nil, err
	}

	// Start the consumer
	readCh, err := ch.Consume(name, consName, false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("Failed to start consuming messages! Got: %s", err)
	}

	// Create a new Consumer
	cons := &Consumer{r.conf, consName, name, ch, readCh, 0, false}

	// Set finalizer to ensure we close the channel
	runtime.SetFinalizer(cons, (*Consumer).Close)
	return cons, nil
}

// Publisher will return a new handle that can be used
// to publish messages to the given queue.
func (r *Relay) Publisher(queue string) (*Publisher, error) {
	// Get a new channel
	ch, err := r.getChan(&r.pubConn)
	if err != nil {
		return nil, err
	}

	// Ensure the queue exists
	name := queueName(queue)
	if err := r.declareQueue(ch, name); err != nil {
		return nil, err
	}

	// Determine content type
	contentType := r.conf.Serializer.ContentType()

	// Determine message mode
	var mode uint8
	if r.conf.DisablePersistence {
		mode = amqp.Transient
	} else {
		mode = amqp.Persistent
	}

	// Create a new Publisher
	pub := &Publisher{conf: r.conf, queue: name, channel: ch,
		contentType: contentType, mode: mode}

	// Set finalizer to ensure we close the channel
	runtime.SetFinalizer(pub, (*Publisher).Close)
	return pub, nil
}

// Consume will consume the next available message. The
// message must be acknowledged with Ack() or Nack() before
// the next call to Consume unless EnableMultiAck is true.
func (c *Consumer) Consume(out interface{}) error {
	// Check if we are closed
	if c.channel == nil {
		return fmt.Errorf("Consumer is closed")
	}

	// Check if an ack is required
	if c.needAck && !c.conf.EnableMultiAck {
		return fmt.Errorf("Ack required before consume!")
	}

	// Wait for a message
	d, ok := <-c.deliverChan
	if !ok {
		return fmt.Errorf("The channel has been closed!")
	}

	// Store the delivery tag for future Ack
	c.lastMsg = d.DeliveryTag
	c.needAck = true

	// Decode the message
	buf := bytes.NewBuffer(d.Body)
	if err := c.conf.Serializer.Decode(buf, out); err != nil {
		return fmt.Errorf("Failed to decode message! Got: %s", err)
	}
	return nil
}

// ConsumeAck will consume the next message and acknowledge
// that the message has been received. This prevents the message
// from being redelivered, and no call to Ack() or Nack() is needed.
func (c *Consumer) ConsumeAck(out interface{}) error {
	if err := c.Consume(out); err != nil {
		return err
	}
	if err := c.Ack(); err != nil {
		return err
	}
	return nil
}

// Ack will send an acknowledgement to the server that the
// last message returned by Consume was processed. If EnableMultiAck is true, then all messages up to the last consumed one will
// be acknowledged
func (c *Consumer) Ack() error {
	if c.channel == nil {
		return fmt.Errorf("Consumer is closed")
	}
	if !c.needAck {
		fmt.Errorf("Ack is not required!")
	}
	if err := c.channel.Ack(c.lastMsg, c.conf.EnableMultiAck); err != nil {
		return err
	}
	c.needAck = false
	return nil
}

// Nack will send a negative acknowledgement to the server that the
// last message returned by Consume was not processed and should be
// redelivered. If EnableMultiAck is true, then all messages up to
// the last consumed one will be negatively acknowledged
func (c *Consumer) Nack() error {
	if c.channel == nil {
		return fmt.Errorf("Consumer is closed")
	}
	if !c.needAck {
		fmt.Errorf("Nack is not required!")
	}
	if err := c.channel.Nack(c.lastMsg,
		c.conf.EnableMultiAck, true); err != nil {
		return err
	}
	c.needAck = false
	return nil
}

// Close will shutdown the Consumer. Any messages that are still
// in flight will be Nack'ed.
func (c *Consumer) Close() error {
	// Make sure close is idempotent
	if c.channel == nil {
		return nil
	}
	defer func() {
		c.channel = nil
	}()

	// Stop consuming inputs
	if err := c.channel.Cancel(c.consName, false); err != nil {
		return fmt.Errorf("Failed to stop consuming! Got: %s", err)
	}

	// Wait to read all the pending messages
	var lastMsg uint64
	var needAck bool
	for {
		d, ok := <-c.deliverChan
		if !ok {
			break
		}
		lastMsg = d.DeliveryTag
		needAck = true
	}

	// Send a Nack for all these messages
	if needAck {
		if err := c.channel.Nack(lastMsg, true, true); err != nil {
			return fmt.Errorf("Failed to send Nack for inflight messages! Got: %s", err)
		}
	}

	// Shutdown the channel
	return c.channel.Close()
}

// Publish will send the message to the server to be consumed
func (p *Publisher) Publish(in interface{}) error {
	// Check for close
	if p.channel == nil {
		return fmt.Errorf("Publisher is closed")
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

// Converts the user input name into the actual name
func queueName(name string) string {
	return "relay." + name
}

// Generates a channel name in the form of <host>.<rand>
// The random value is a hex encoding of 4 random bytes.
func channelName() (string, error) {
	// Get hostname
	host, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("Failed to get hostname! Got: %s", err)
	}

	// Get random bytes
	bytes := make([]byte, 4)
	n, err := io.ReadFull(rand.Reader, bytes)
	if n != len(bytes) || err != nil {
		return "", fmt.Errorf("Failed to read random bytes! Got: %s", err)
	}

	// Convert to hex
	h := hex.EncodeToString(bytes)

	// Return the new name
	return host + "." + h, nil
}

// GOBSerializer implements the Serializer interface and uses the GOB format
type GOBSerializer struct{}

func (*GOBSerializer) ContentType() string {
	return "binary/gob"
}
func (*GOBSerializer) Encode(w io.Writer, e interface{}) error {
	enc := gob.NewEncoder(w)
	return enc.Encode(e)
}
func (*GOBSerializer) Decode(r io.Reader, o interface{}) error {
	dec := gob.NewDecoder(r)
	return dec.Decode(o)
}
