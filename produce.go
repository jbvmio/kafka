package kafka

import "github.com/Shopify/sarama"

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
	producer sarama.AsyncProducer
	cl       *KClient
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
	return &Producer{producer: p, cl: kc}, nil
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
