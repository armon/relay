relay
=====

Relay is a Go framework for task queues, and makes writing code for publishers
and consumers very simple. It is a wrapper around the AMQP protocol, and relies
on a message broker like RabbitMQ.

The reason Relay exists is that AMQP is a tedious protocol to deal with, and the
high level abstraction of a task queue is often something that is desirable. With
Relay, you simply Publish objects into task queues, and Consume them on the other end.

# Features

* Simple to use, hides the AMQP details
* Flexible encoding and decoding support
* Configuration changes instead of code changes

# Documentation

See the online documentation here: [http://godoc.org/github.com/armon/relay](http://godoc.org/github.com/armon/relay).

# Example

Here is an example of a simple publisher:

    conf := &relay.Config{Addr: "rabbitmq"}
    conn, err := relay.New(conf)
    defer conn.Close()

    pub, err := conn.Publisher("tasks")
    defer pub.Close()

    pub.Publish("this is a test message")


Here is an example of a simple consumer:

    conf := &relay.Config{Addr: "rabbitmq"}
    conn, err := relay.New(conf)
    defer conn.Close()

    cons, err := conn.Consumer("tasks")
    defer cons.Close()

    var msg string
    for {
        cons.ConsumeAck(&msg)
        fmt.Printf("Got msg: %s\n", msg)
    }

Here is an example of a consumer using prefetching and multi Acks:

    conf := &relay.Config{Addr: "rabbitmq", PrefetchCount: 5, EnableMultiAck: true}
    conn, err := relay.New(conf)
    defer conn.Close()

    cons, err := conn.Consumer("tasks")
    defer cons.Close()

    var msg string
    for {
        // Consume 5 messages
        for i := 0; i < 5; i++ {
            cons.Consume(&msg)
            fmt.Printf("Got msg: %s\n", msg)
        }

        // Acks the last 5 messages
        cons.Ack()
    }

