package main

import (
	"fmt"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

/*
❇️ AMQP — A messaging protocol that defines how messages are delivered between systems.
❇️ RabbitMQ — A message broker/server that implements AMQP to send, store, and route messages.


Why Use RabbitMQ?
⭐ Advantages:
Very stable, runs in production for years.
Supports high throughput.
Works in microservices, event-driven systems, job queues.
Message persistence ensures no data loss.
Retry, DLX, priority queues supported.
Excellent monitoring dashboard.
🆚 Compared to Kafka?
RabbitMQ → traditional message queue, guaranteed delivery, priority, routing.
Kafka → distributed event log, high throughput, stream processing.


Summary of All Boolean Flags
🔹 QueueDeclare Booleans
Flag			Meaning	When to Use
durable			Queue survives RabbitMQ restart	Production queues
autoDelete		Delete queue when last consumer disconnects	Temporary queues
exclusive		Queue belongs only to this connection	Reply queues / RPC
noWait			Do not wait for server response	Rarely, mostly false


PublishWithContext Booleans
Flag		Meaning	When to Use
mandatory	If message cannot be routed → return error	Advanced routing cases
immediate	Deliver immediately or error (deprecated)	Always false

Recommended Best Practices (Professional)

Use durable queue for production
ch.QueueDeclare("Go", true, false, false, false, nil)


Use context
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

Publish
ch.PublishWithContext(ctx, "", "Go", false, false, msg)

*/

func main() {
	fmt.Println("Rabbit MQ Producer")

	arguments := os.Args
	fmt.Println(len(arguments))
	if len(arguments) == 2 {

		if arguments[1] == "client" {
			ClientRabbitMQ()
		}
		os.Exit(0)
	}

	// -----------------------------------------
	// 1. Create AMQP Connection String
	// -----------------------------------------
	// Format: amqp://username:password@host:port/vhost
	connectionString := "amqp://admin:admin123@localhost:5672/"

	// -----------------------------------------
	// 2. Dial = Establish TCP Connection to RabbitMQ
	// -----------------------------------------
	/**
	amqp.Dial(url string) (*amqp.Connection, error)

	- Opens a new TCP connection to the RabbitMQ server
	- Uses AMQP protocol
	- If the scheme is "amqps://" then TLS is used
	- Automatically sets:
	  - Heartbeat = 10 seconds
	  - Handshake timeout = 30 seconds
	*/
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		log.Println("amqp.Dial failed: ", err)
		return
	}

	// -----------------------------------------
	// 3. Create Channel
	// -----------------------------------------
	/**
	Each AMQP connection can have multiple channels.
	Channels are lightweight and handle message operations.

	conn.Channel() -> opens a virtual channel on the connection

	Why channels?
	- TCP connections are expensive
	- Channels are cheap
	- Used to send/receive messages
	*/
	ch, err := conn.Channel()
	if err != nil {
		log.Println(err)
		return
	}
	defer ch.Close() // Always close channel

	// -----------------------------------------
	// 4. Declare Queue
	// -----------------------------------------
	/**
	QueueDeclare(name string,
		durable bool,
		autoDelete bool,
		exclusive bool,
		noWait bool,
		args amqp.Table)

	Each Parameter Meaning:

	1) name → string
	   - Name of the queue (e.g., "Go").
	   - If empty (""), RabbitMQ generates a random queue name.

	2) durable → bool
	   - true  = queue survives server restart (persistent)
	   - false = queue is temporary (deleted on restart)
	   **Use true in production**

	3) autoDelete → bool
	   - Deletes queue when last consumer disconnects.
	   - true  = auto cleanup
	   - false = keep queue
	   Use true for temporary queues.

	4) exclusive → bool
	   - Queue is accessible ONLY to this connection.
	   - true  = private queue, deleted when connection closes
	   - false = can be used by multiple consumers

	5) noWait → bool
	   - true  = don’t wait for RabbitMQ acknowledgement
	   - false = wait for server response
	   Usually false.

	6) args → amqp.Table (map[string]interface{})
	   - Optional queue arguments
	   Example: TTL, Dead Letter Exchange, Message Limit

	   Message TTL (per message expiration)
	   args := amqp.Table{
		    "x-message-ttl": int32(30000), // 30 seconds
			}

		Queue TTL (delete queue after unused time)
		args := amqp.Table{
			"x-expires": int32(60000), // delete queue after 1 min unused
		}

		Max Queue Length (message count limit)
		args := amqp.Table{
				"x-max-length": int32(1000), // keep only last 1000 messages
			}

		Max Queue Size in bytes
		args := amqp.Table{
				"x-max-length-bytes": int32(5000000), // 5MB
			}

		Priority Queue
		args := amqp.Table{
				"x-max-priority": int32(10), // priority range 0–10
			}

		Lazy Queue (store messages on disk)
		args := amqp.Table{
				"x-queue-mode": "lazy",
			}

	Queue with TTL + DLX + Priority
	A Dead Letter Exchange (DLX) is a special exchange where failed messages are sent automatically.
	When a message cannot be processed, RabbitMQ moves it to a Dead Letter Queue (DLQ) through a DLX.

	args := amqp.Table{
			"x-message-ttl":             int32(30000),
			"x-max-priority":            int32(10),
			"x-dead-letter-exchange":    "dlx.exchange",
			"x-dead-letter-routing-key": "dlx.key",
		}

		ch.QueueDeclare(
			"GoQueue",
			true,
			false,
			false,
			false,
			args,
		)


	-----------------------------------------
	Your settings:
	QueueDeclare("Go",
		false, // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil)   // no args
	-----------------------------------------
	This creates:
	- A NON durable
	- NOT auto deleted
	- Shared
	- Waits for ok
	*/
	q, err := ch.QueueDeclare("Go", false, false, false, false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	fmt.Println("Queue:", q)

	// -----------------------------------------
	// 5. Prepare Message Body
	// -----------------------------------------
	message := "i love to Writing to RabbitMQ !"

	// -----------------------------------------
	// 6. Publish Message
	// -----------------------------------------
	/**
	PublishWithContext(ctx,
		exchange string,
		routingKey string,
		mandatory bool,
		immediate bool,
		msg amqp.Publishing)

	-----------------------------------------
	PARAMETERS:
	-----------------------------------------

	1) ctx → context.Context
	   Normally used for timeout, cancel.
	   But note: In this library context is ignored.

	   Pass: context.Background() (recommended)
	   You passed: nil → works but not recommended.

	2) exchange → string
	   "" (empty string) = default exchange
	   Default exchange uses routingKey = queue name

	3) routingKey → string
	   Must match the queue name
	   Here: "Go"

	4) mandatory → bool
	   - true  = return error if message cannot be routed
	   - false = silently drop if no queue is matched

	5) immediate → bool (deprecated in new RabbitMQ)
	   - true  = deliver immediately or return error
	   - false = normal behavior (always use false)

	6) msg → amqp.Publishing
	   Message to publish
	   Fields:
	   - ContentType (MIME) e.g. "text/plain"
	   - Body (byte slice)

	-----------------------------------------
	Your call:
	PublishWithContext(nil,
		"",     // default exchange
		"Go",   // routing key = queue name
		false,  // mandatory
		false,  // immediate
		msg)    // message
	-----------------------------------------
	This sends the message directly to queue "Go".
	*/
	err = ch.PublishWithContext(nil, "", "Go", false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(message),
	})

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Message Published to Queue")

}

//Knowledge

/*

What is AMQP? (Advanced Message Queuing Protocol)
AMQP is a standard messaging protocol used for sending messages between applications.
It defines how messages are formatted, delivered, routed, acknowledged, secured, etc.

⭐ Key Points:
Protocol, not a broker.
Designed for high reliability, interoperability, cross-language communication.
Used for asynchronous communication between services.
Supports message acknowledgement, routing, persistent messages, transactions.

🔧 Core Concepts in AMQP:
Producer → Sends messages
Consumer → Receives messages
Queue → Stores messages
Exchange → Routes messages to queues
Binding → Rule between exchange → queue
Routing Key → Used for message routing
Acknowledgement (ACK) → Confirms delivery
Prefetch / QoS → Control message load
🧱 Exchange Types (AMQP)

Direct → Exact routing key match
Topic → Pattern matching (*, #)
Fanout → Broadcast to all queues
Headers → Routing based on headers


What is RabbitMQ?

RabbitMQ is a message broker (message server) that implements AMQP.
It is one of the most popular, lightweight, easy-to-use brokers.

⭐ Key Points:
Open-source, written in Erlang.
Implements AMQP protocol.
Provides message queueing, delivery guarantees, routing, retries, dead-lettering.
Works across all languages: Go, Node, Python, Java, etc.
Supports plugins (like Management UI, Shovel, Federation).
*/

// ClientRabbitMQ connects to RabbitMQ, consumes messages from the "Go" queue,
// and prints each received message to the console.
// This function runs indefinitely until manually stopped.
func ClientRabbitMQ() {
	fmt.Println("RabbitMQ Consumer!")

	// 1. Connection string for AMQP protocol
	// Format: amqp://username:password@host:port/vhost
	connectionString := "amqp://admin:admin123@localhost:5672/"

	// 2. Establish a TCP connection to RabbitMQ server
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		log.Println("Failed to connect to RabbitMQ:", err)
		return
	}
	// Ensure connection is closed when function exits
	defer conn.Close()

	// 3. Open a channel on the connection
	// Channels are virtual links inside a single TCP connection
	ch, err := conn.Channel()
	if err != nil {
		log.Println("Failed to open a channel:", err)
		return
	}
	// Close channel on function exit
	defer ch.Close()

	// 4. Start consuming messages from the "Go" queue
	//
	// Params:
	// queue        = "Go"       → Name of queue to consume from
	// consumerTag  = ""         → Let RabbitMQ generate a consumer tag
	// autoAck      = true       → Automatically ACK messages immediately
	// exclusive    = false      → Not exclusive (multiple consumers can listen)
	// noLocal      = false      → Allow receiving messages published by same connection
	// noWait       = false      → Wait for RabbitMQ response
	// args         = nil        → Additional optional arguments
	msgs, err := ch.Consume(
		"Go",  // queue name
		"",    // consumer tag
		true,  // auto acknowledge
		false, // not exclusive
		false, // no-local disabled
		false, // wait for server confirm
		nil,   // no additional arguments
	)
	if err != nil {
		log.Println("Failed to register consumer:", err)
		return
	}

	// 5. Create a channel to keep the main goroutine running forever
	forever := make(chan bool)

	// 6. Start a goroutine to process messages
	go func() {
		for d := range msgs {
			// d.Body → actual message payload
			fmt.Printf("Receiver: %s\n", d.Body)
		}
	}()

	fmt.Println("Connected to RabbitMQ Server! Waiting for messages...")

	// 7. Block forever (until the process is forcefully stopped)
	<-forever
}
