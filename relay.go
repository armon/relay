package relay

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/armon/relay/broker"
	"github.com/streadway/amqp"
)

// Config is passed into New when creating a Relay to tune
// various parameters around broker interactions.
type Config struct {
	Addr                  string        // Host address to dial
	Port                  int           // Host por to bind
	Vhost                 string        // Broker Vhost
	Username              string        // Broker username
	Password              string        // Broker password
	EnableTLS             bool          // Broker TLS connection
	PrefetchCount         int           // How many messages to prefetch. If < 1, defaults to 1.
	EnableMultiAck        bool          // Controls if we allow multi acks
	DisablePublishConfirm bool          // Disables confirmations of publish
	DisablePersistence    bool          // Disables message persistence
	Exchange              string        // Custom exchange. Defaults to "relay"
	Serializer            Serializer    // Defaults to GOBSerializer
	MessageTTL            time.Duration // Optional, attempts to put a TTL on message life
	QueueTTL              time.Duration // Optional, attempts to make a TTL on a queue life
}

type Relay struct {
	sync.Mutex
	conf     *Config
	pubConn  *amqp.Connection // Publisher connection.
	consConn *amqp.Connection // Consumer connection. Avoid TCP backpressure.
}

// Returned to indicate a closed channel
var ChannelClosed = fmt.Errorf("Channel closed!")

// Returned to indicate a read timeout
var TimedOut = fmt.Errorf("Timeout")

// ConfigFromURI attempts to parse the given AMQP URI according to the spec
// and return a relay config based on it.
// See http://www.rabbitmq.com/uri-spec.html.
//
// Default values for the fields are:
//
//   Scheme: amqp
//   Host: localhost
//   Port: 5672
//   Username: guest
//   Password: guest
//   Vhost: /
//
func ConfigFromURI(amqpUri string) (*Config, error) {

	uri, err := amqp.ParseURI(amqpUri)

	if err != nil {
		return nil, err
	}

	// build the configuration, note the defaults above
	config := &Config{
		Addr:     uri.Host,
		Port:     uri.Port,
		Vhost:    uri.Vhost,
		Username: uri.Username,
		Password: uri.Password,
	}

	// assign this field based on wether the scheme is amqps, which is amqp with TLS
	config.EnableTLS = (uri.Scheme == "amqps")

	return config, nil
}

// New will create a new Relay that can be used to create
// new publishers or consumers. The caller should no longer modify
// the configuration once New is invoked, nor should it be
// shared between multiple relays.
func New(c *Config) (*Relay, error) {
	// Set the defaults if missing
	if c.Addr == "" {
		c.Addr = "localhost"
	}
	if c.Port == 0 {
		if c.EnableTLS {
			c.Port = 5671
		} else {
			c.Port = 5672
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
	if c.PrefetchCount < 1 {
		c.PrefetchCount = 1
	}

	// Create relay with finalizer
	r := &Relay{conf: c}
	return r, nil
}

// Used to get a new server connection
func (r *Relay) getConn() (*amqp.Connection, error) {
	conf := r.conf
	uri := amqp.URI{Host: conf.Addr, Port: conf.Port,
		Username: conf.Username, Password: conf.Password,
		Vhost: conf.Vhost}
	if conf.EnableTLS {
		uri.Scheme = "amqps"
	} else {
		uri.Scheme = "amqp"
	}
	uri_s := uri.String()
	return amqp.Dial(uri_s)
}

// Watches for connection errors and closes the connection
func (r *Relay) watchConn(conn **amqp.Connection, errCh chan *amqp.Error) {
	for {
		// Wait for an error
		err, ok := <-errCh
		if !ok {
			break
		}

		// Log the error
		log.Printf("[ERR] Relay got error: (Code %d Server: %v Recoverable: %v) %s",
			err.Code, err.Server, err.Recover, err.Reason)

		// If this is not recoverable, close the connection
		if !err.Recover {
			break
		}
	}

	// Unset the connection
	r.Lock()
	defer r.Unlock()
	*conn = nil
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

		// Watch for connection errors
		errCh := make(chan *amqp.Error)
		newConn.NotifyClose(errCh)
		go r.watchConn(conn, errCh)
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
func (r *Relay) declareQueue(ch *amqp.Channel, name string, routingKey string) error {
	var args amqp.Table
	if r.conf.QueueTTL > 0 {
		args = make(map[string]interface{})
		msec := int32(r.conf.QueueTTL / time.Millisecond)
		args["x-expires"] = msec
	}

	// Declare the queue
	if _, err := ch.QueueDeclare(name, true, false, false, false, args); err != nil {
		return fmt.Errorf("Failed to declare queue '%s'! Got: %s", name, err)
	}

	// Bind the queue to the exchange
	if err := ch.QueueBind(name, routingKey, r.conf.Exchange, false, nil); err != nil {
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
	return r.ConsumerByRoutingKey(queue, queue)
}

// ConsumerByRoutingKey will return a new handle that can be used
// to consume messages from a given queue and routing key.
func (r *Relay) ConsumerByRoutingKey(queue string, routingKey string) (*Consumer, error) {

	// Get a new channel
	ch, err := r.getChan(&r.consConn)
	if err != nil {
		return nil, err
	}

	// Ensure the channel is closed on error
	success := false
	defer func() {
		if !success {
			ch.Close()
		}
	}()

	// Ensure the queue exists
	name := queueName(queue)
	if err := r.declareQueue(ch, name, routingKey); err != nil {
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
	success = true
	cons := &Consumer{r.conf, consName, name, ch, readCh, 0, 0, false}
	return cons, nil
}

// Publisher will return a new handle that can be used
// to publish messages to the given queue.
func (r *Relay) Publisher(queue string) (*Publisher, error) {
	return r.PublisherWithRoutingKey(queue, queue)
}

// PublisherWithRoutingKey will return a new handle that can be used
// to publish messages to the given queue and routing key.
func (r *Relay) PublisherWithRoutingKey(queue string, routingKey string) (*Publisher, error) {
	// Get a new channel
	ch, err := r.getChan(&r.pubConn)
	if err != nil {
		return nil, err
	}

	// Ensure the channel is closed on error
	success := false
	defer func() {
		if !success {
			ch.Close()
		}
	}()

	// Ensure the queue exists
	name := queueName(queue)
	if err := r.declareQueue(ch, name, routingKey); err != nil {
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

	// Check if we need confirmations
	if !r.conf.DisablePublishConfirm {
		errCh := ch.NotifyClose(make(chan *amqp.Error, 1))
		ackCh, nackCh := ch.NotifyConfirm(make(chan uint64, 1), make(chan uint64, 1))
		if err := ch.Confirm(false); err != nil {
			return nil, fmt.Errorf("Failed to put publisher in confirm mode! Got: %s", err)
		}

		// Attach the channels
		pub.ackCh, pub.nackCh, pub.errCh = ackCh, nackCh, errCh
	}

	// Successful return
	success = true
	return pub, nil
}

// Broker is used to wrap a Relay connection in one that is Broker
// compatible.
func (r *Relay) Broker() broker.Broker {
	return &relayBroker{r}
}

type relayBroker struct {
	relay *Relay
}

func (r *relayBroker) Close() error {
	return r.relay.Close()
}

func (r *relayBroker) Publisher(q string) (broker.Publisher, error) {
	return r.relay.Publisher(q)
}

func (r *relayBroker) Consumer(q string) (broker.Consumer, error) {
	return r.relay.Consumer(q)
}
