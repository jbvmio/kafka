package kafka

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

// ProducerError is the type of error generated when the producer fails to deliver a message.
// It contains the original ProducerMessage as well as the actual error value.
type ProducerError struct {
	Msg *Message
	Err error
}

// SendMSG sends a message to the targeted topic/partition defined in the message.
func (kc *KClient) SendMSG(message *Message) (partition int32, offset int64, err error) {
	producer, err := sarama.NewSyncProducerFromClient(kc.cl)
	if err != nil {
		return
	}
	partition, offset, err = producer.SendMessage(message.toSarama())
	producer.Close()
	return
}

// SendMessages sends groups of messages to the targeted topic/partition defined in each message.
func (kc *KClient) SendMessages(messages []*Message) (err error) {
	producer, err := sarama.NewSyncProducerFromClient(kc.cl)
	if err != nil {
		return
	}
	var msgs []*sarama.ProducerMessage
	for _, message := range messages {
		msgs = append(msgs, message.toSarama())
	}
	err = producer.SendMessages(msgs) //.SendMessage(message.toSarama())
	producer.Close()
	return
}

// Producer is the implementation of an AsyncProducer.
type Producer struct {
	producer         sarama.AsyncProducer
	cl               *KClient
	errors           chan *ProducerError
	input, successes chan *Message
}

// NewProducer returns a new Producer with Successes and Error channels that must be read from.
func NewProducer(addrs []string, config *sarama.Config) (*Producer, error) {
	var producer Producer
	if config == nil {
		config = GetConf("")
		config.Version = RecKafkaVersion
		config.Producer.Return.Successes = true
		config.Producer.Return.Errors = true
		config.Producer.RequiredAcks = 1
		config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	}
	kc, err := NewCustomClient(config, addrs...)
	if err != nil {
		return &producer, err
	}
	p, err := sarama.NewAsyncProducerFromClient(kc.cl)
	if err != nil {
		return &producer, err
	}
	producer.producer = p
	producer.cl = kc
	producer.errors = make(chan *ProducerError)
	producer.input = make(chan *Message)
	producer.successes = make(chan *Message)
	return &producer, nil
}

// NewProducer returns a new Producer with Successes and Error channels that must be read from.
func (kc *KClient) NewProducer(addrs []string, config *sarama.Config) (*Producer, error) {
	/*	Producer Options to be aware of:
		config.Version = RecKafkaVersion
		config.Producer.Return.Successes = true
		config.Producer.Return.Errors = true
		config.Producer.RequiredAcks = 1
		config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	*/
	p, err := sarama.NewAsyncProducerFromClient(kc.cl)
	if err != nil {
		return &Producer{}, err
	}
	return &Producer{
		producer:  p,
		cl:        kc,
		errors:    make(chan *ProducerError),
		input:     make(chan *Message),
		successes: make(chan *Message),
	}, nil
}

// RequiredAcks sets the desired number of replica acknowledgements it must see from the broker
// when producing messages. Default is 1.
// 0 = NoResponse doesn't send any response, the TCP ACK is all you get.
// 1 = WaitForLocal waits for only the local commit to succeed before responding. (default)
// 2 = WaitForAll waits for all in-sync replicas to commit before responding.
func (p *Producer) RequiredAcks(n int) {
	switch n {
	case 0:
		p.cl.SaramaConfig().Producer.RequiredAcks = 0
	case 2:
		p.cl.SaramaConfig().Producer.RequiredAcks = -1
	default:
		p.cl.SaramaConfig().Producer.RequiredAcks = 1
	}
}

// SetPartitioner sets the desired Partitioner which decides which partition messages are sent.
// 0 - ManualPartitioner
// 1 - RandomPartitioner
// 2 - HashPartitioner
// 3 - RoundRobinPartitioner (default)
func (p *Producer) SetPartitioner(n int) {
	switch n {
	case 0:
		p.cl.SaramaConfig().Producer.Partitioner = sarama.NewManualPartitioner
	case 1:
		p.cl.SaramaConfig().Producer.Partitioner = sarama.NewRandomPartitioner
	case 2:
		p.cl.SaramaConfig().Producer.Partitioner = sarama.NewHashPartitioner
	default:
		p.cl.SaramaConfig().Producer.Partitioner = sarama.NewRoundRobinPartitioner
	}
}

// AsyncClose triggers a shutdown of the producer. The shutdown has completed
// when both the Errors and Successes channels have been closed. When calling
// AsyncClose, you *must* continue to read from those channels in order to
// drain the results of any messages in flight.
func (p *Producer) AsyncClose() {
	p.producer.AsyncClose()
}

// Close shuts down the producer and waits for any buffered messages to be
// flushed. You must call this function before a producer object passes out of
// scope, as it may otherwise leak memory. You must call this before calling
// Close on the underlying client.
func (p *Producer) Close() error {
	return p.producer.Close()
}

// Shutdown starts the shutdown of the producer, draining both Errors and Successes channels, then closes the underlying client.
// A given timeout value (in seconds) can be used to specify an allotted time for draining the channels.
// If the timer expires and error will be returned.
func (p *Producer) Shutdown(timeout int) error {
	var errd error
	var closed bool
	p.producer.AsyncClose()
	timer := time.NewTicker(time.Duration(timeout) * time.Second)
drainLoop:
	for {
		select {
		case <-timer.C:
			err := p.producer.Close()
			if err != nil {
				errd = fmt.Errorf("Timed out Draining Producer. Error Closing Client: %v", err)
			} else {
				closed = true
				errd = fmt.Errorf("Timed out Draining Producer")
			}
			break drainLoop
		case <-p.producer.Successes():
		case <-p.producer.Errors():
		}
	}
	if !closed {
		errd = p.producer.Close()
	}
	return errd
}

// Input is the input channel for the user to write messages to that they
// wish to send.
func (p *Producer) Input() chan<- *Message {
	return p.producer.Input()
}
