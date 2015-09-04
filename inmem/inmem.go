package inmem

import (
	"reflect"
	"sync"
	"time"

	"github.com/armon/relay"
	"github.com/armon/relay/broker"
)

// InmemBroker implements the Broker interface in-memory
type InmemBroker struct {
	Closed bool
	Queues map[string][]interface{}

	lock sync.RWMutex
}

// InmemConsumer implements the Consumer interface
type InmemConsumer struct {
	Broker      *InmemBroker
	Queue       string
	Closed      bool
	NeedAck     bool
	LastDequeue interface{}
}

// InmemConsumer implements the Publisher interface
type InmemPublisher struct {
	Broker *InmemBroker
	Queue  string
	Closed bool
}

func NewInmemBroker() *InmemBroker {
	in := &InmemBroker{
		Queues: make(map[string][]interface{}),
	}
	return in
}

func (i *InmemBroker) Close() error {
	i.Closed = true
	return nil
}

func (i *InmemBroker) Consumer(q string) (broker.Consumer, error) {
	c := &InmemConsumer{
		Broker: i,
		Queue:  q,
	}
	return c, nil
}

func (i *InmemBroker) Publisher(q string) (broker.Publisher, error) {
	p := &InmemPublisher{
		Broker: i,
		Queue:  q,
	}
	return p, nil
}

func (i *InmemPublisher) Close() error {
	i.Closed = true
	return nil
}

func (i *InmemPublisher) Publish(in interface{}) error {
	i.Broker.lock.Lock()
	defer i.Broker.lock.Unlock()

	queue := i.Broker.Queues[i.Queue]
	queue = append(queue, in)
	i.Broker.Queues[i.Queue] = queue
	return nil
}

func (i *InmemConsumer) Close() error {
	if i.NeedAck {
		i.Nack()
	}
	i.Closed = true
	return nil
}

func (i *InmemConsumer) Consume(out interface{}) error {
	return i.ConsumeTimeout(out, 0)
}

func (i *InmemConsumer) ConsumeAck(out interface{}) error {
	err := i.ConsumeTimeout(out, 0)
	if err == nil {
		return i.Ack()
	}
	return err
}

func (i *InmemConsumer) ConsumeTimeout(out interface{}, timeout time.Duration) error {
	if i.NeedAck {
		panic("Consuming when NeedAck")
	}
	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timeoutCh = time.After(timeout)
	}

	haveMsg := false
	var msg interface{}
	for {
		i.Broker.lock.Lock()
		queue := i.Broker.Queues[i.Queue]
		if len(queue) > 0 {
			msg = queue[0]
			haveMsg = true
			copy(queue[0:], queue[1:])
			i.Broker.Queues[i.Queue] = queue[:len(queue)-1]
		}
		i.Broker.lock.Unlock()
		if haveMsg {
			break
		}

		select {
		case <-time.After(time.Millisecond):
			continue
		case <-timeoutCh:
			return relay.TimedOut
		}
	}

	// Set that we need ack
	i.NeedAck = true
	i.LastDequeue = msg

	// Set the message
	dst := reflect.Indirect(reflect.ValueOf(out))
	src := reflect.Indirect(reflect.ValueOf(msg))
	dst.Set(src)
	return nil
}

func (i *InmemConsumer) Ack() error {
	if !i.NeedAck {
		panic("Ack not needed")
	}
	i.NeedAck = false
	i.LastDequeue = nil
	return nil
}

func (i *InmemConsumer) Nack() error {
	if !i.NeedAck {
		panic("Nack not needed")
	}
	i.Broker.lock.Lock()
	defer i.Broker.lock.Unlock()

	// Push last entry back
	queue := i.Broker.Queues[i.Queue]
	n := len(queue)
	queue = append(queue, nil)
	copy(queue[1:], queue[0:n])
	queue[0] = i.LastDequeue
	i.Broker.Queues[i.Queue] = queue

	i.NeedAck = false
	i.LastDequeue = nil
	return nil
}
