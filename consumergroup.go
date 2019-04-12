package kafka

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
)

// ConsumerGroup implements a sarama ConsumerGroup.
type ConsumerGroup struct {
	GroupID  string
	Metadata CGMeta

	handlers CGHandler
	consumer sarama.ConsumerGroup
	ctx      context.Context
}

// CGMeta contains Metadata related to a ConsumerGroup.
type CGMeta struct {
	Claims  map[string][]int32
	Members map[string]map[int32]string
	GenIDs  map[string]int32
}

// GetCGMeta returns the ConsumerGroup Metadata.
func (cg *ConsumerGroup) GetCGMeta() CGMeta {
	return cg.Metadata
}

// GET configures the ConsumerGroup to process the given topic with the corresponding ProcessMessageFunc.
func (cg *ConsumerGroup) GET(topic string, handler ProcessMessageFunc) {
	cg.handlers.Handles[topic] = handler
}

// Consume joins a cluster of consumers for a given list of topics and
// starts a blocking ConsumerGroupSession through the ConsumerGroupHandler.
//func (cg *ConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
func (cg *ConsumerGroup) Consume() error {
	if len(cg.handlers.Handles) > 1 {
		panic("No topics configured for Consumer Group ")
	}
	var topics []string
	for t := range cg.handlers.Handles {
		topics = append(topics, t)
	}
	return cg.consumer.Consume(cg.ctx, topics, cg.handlers)
}

// Errors returns a read channel of errors that occurred during the consumer life-cycle.
func (cg *ConsumerGroup) Errors() <-chan error {
	return cg.consumer.Errors()
}

// Close stops the ConsumerGroup and detaches any running sessions.
func (cg *ConsumerGroup) Close() error {
	return cg.consumer.Close()
}

// CGHandler implements sarama ConsumerGroupHandlers.
type CGHandler struct {
	Handles TopicHandlerMap

	cg *ConsumerGroup
}

func newCGHandler(cg *ConsumerGroup) CGHandler {
	return CGHandler{
		Handles: newTopicHandlerMap(),
		cg:      cg,
	}
}

// ProcessMessageFunc is used to iterate over Messages.
// Returns true for each Message if successful, false if not with optional error.
type ProcessMessageFunc func(*Message) (bool, error)

// DefaultMessageFunc is default ProcessMessageFunc that can be used with a TopicHandlerMap.
func DefaultMessageFunc(msg *Message) (bool, error) {
	if msg != nil {
		fmt.Printf("Message topic:%q partition:%d offset:%d\n%s\n\n", msg.Topic, msg.Partition, msg.Offset, msg.Value)
		return true, nil
	}
	return false, fmt.Errorf("Recieved nil message")
}

// TopicHandlerMap maps topics to ProcessMessageFuncs.
type TopicHandlerMap map[string]ProcessMessageFunc

func newTopicHandlerMap() TopicHandlerMap {
	return make(map[string]ProcessMessageFunc)
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (h CGHandler) Setup(sess sarama.ConsumerGroupSession) error {
	claims := sess.Claims()
	mID := sess.MemberID()
	for t, p := range claims {
		memMap := make(map[int32]string, len(p))
		for _, part := range p {
			memMap[part] = mID
		}
		h.cg.Metadata.Members[t] = memMap
		h.cg.Metadata.Claims[t] = claims[t]
		h.cg.Metadata.GenIDs[t] = sess.GenerationID()
	}
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (h CGHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (h CGHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	var (
		currentOffset int64
		good          bool
		err           error
	)
	handleFunc, ok := h.Handles[claim.Topic()]
	if ok {
	ProcessMessages:
		for msg := range claim.Messages() {
			good, err = handleFunc(convertMsg(msg))
			if !good {
				break ProcessMessages
			}
			sess.MarkMessage(msg, "")
			currentOffset = msg.Offset
		}
	} else {
		panic("No handler for given topic " + claim.Topic())
	}
	sess.MarkOffset(claim.Topic(), claim.Partition(), currentOffset+1, "")
	return err
}

// NewConsumerGroup returns a new ConsumerGroup.
func NewConsumerGroup(addrs []string, groupID string, config *sarama.Config, topics ...string) (*ConsumerGroup, error) {
	if config == nil {
		config = GetConf("")
		config.Version = RecKafkaVersion
	}
	config.Consumer.Return.Errors = true
	group, err := sarama.NewConsumerGroup(addrs, groupID, config)
	if err != nil {
		return nil, err
	}
	cg := &ConsumerGroup{
		GroupID:  groupID,
		consumer: group,
		ctx:      context.Background(),
	}
	cg.handlers = newCGHandler(cg)
	cg.Metadata = CGMeta{
		Claims:  make(map[string][]int32),
		Members: make(map[string]map[int32]string),
		GenIDs:  make(map[string]int32),
	}
	if len(topics) > 0 {
		for _, topic := range topics {
			cg.handlers.Handles[topic] = DefaultMessageFunc
		}
	}
	return cg, nil
}

// NewConsumerGroup returns a new ConsumerGroup using an existing KClient.
func (kc *KClient) NewConsumerGroup(groupID string, topics ...string) (*ConsumerGroup, error) {
	kc.config.Consumer.Return.Errors = true
	group, err := sarama.NewConsumerGroupFromClient(groupID, kc.cl)
	if err != nil {
		return nil, err
	}
	cg := &ConsumerGroup{
		GroupID:  groupID,
		consumer: group,
		ctx:      context.Background(),
	}
	cg.handlers = newCGHandler(cg)
	cg.Metadata = CGMeta{
		Claims:  make(map[string][]int32),
		Members: make(map[string]map[int32]string),
		GenIDs:  make(map[string]int32),
	}
	if len(topics) > 0 {
		for _, topic := range topics {
			cg.handlers.Handles[topic] = DefaultMessageFunc
		}
	}
	return cg, nil
}

// CG returns the underlying Consumer from sarama-cluster
/*
func (cg *ConsumerGroup) CG() sarama.ConsumerGroup {
	return cg.consumer
}
*/

// NewConsumerGroup returns a new ConsumerGroup
/*
func (kc *KClient) NewConsumerGroup(groupID string, debug bool, topics ...string) (*ConsumerGroup, error) {
	var cg ConsumerGroup
	var dChan chan *DEBUG
	kc.config.Producer.RequiredAcks = sarama.WaitForAll
	kc.config.Producer.Return.Successes = true
	config := cluster.NewConfig()
	conf := kc.config
	config.Config = *conf
	config.Group.Mode = cluster.ConsumerModePartitions
	if debug {
		config.Consumer.Return.Errors = true
		config.Group.Return.Notifications = true
		dChan = make(chan *DEBUG, 256)
	}
	brokers, err := kc.BrokerList()
	if err != nil {
		return &cg, err
	}
	consumer, err := cluster.NewConsumer(brokers, groupID, topics, config)
	if err != nil {
		return &cg, err
	}
	cg.consumer = consumer
	if debug {
		cg.debugChan = dChan
		cg.haveDebugChan = make(chan bool, 1)
		cg.debugEnabled = true
		go startDEBUG(&cg)
	}
	return &cg, nil
}

// ConsumeMessages here
func (cg *ConsumerGroup) ConsumeMessages(pc cluster.PartitionConsumer, iterator func(msg *Message) bool) {
ProcessMessages:
	for m := range pc.Messages() {
		message := convertMsg(m)
		if !iterator(message) {
			break ProcessMessages
		}
		cg.consumer.MarkOffset(m, "") // mark message as processed
	}
	cg.consumer.CommitOffsets()
}

// DeBug here
func (cg *ConsumerGroup) DeBug() <-chan *DEBUG {
	return cg.debugChan
}

// DebugAvailale here
func (cg *ConsumerGroup) DebugAvailale() <-chan bool {
	return cg.haveDebugChan
}

// DeBug here
func startDEBUG(cg *ConsumerGroup) {
	for {
		select {
		case note, ok := <-cg.consumer.Notifications():
			if !ok {
				break
			}
			d := DEBUG{}
			d.Type = note.Type.String()
			d.Claimed = note.Claimed
			d.Released = note.Released
			d.Current = note.Current
			d.HasData = true
			d.IsNote = true
			cg.debugChan <- &d
			cg.haveDebugChan <- true
		case err, ok := <-cg.consumer.Errors():
			if !ok {
				break
			}
			if err != nil {
				cg.debugChan <- &DEBUG{
					HasData: true,
					Err:     err,
				}
			}
		}
	}
}

// ProcessDEBUG here
func (cg *ConsumerGroup) ProcessDEBUG(iterator func(d *DEBUG) bool) {
ProcessDEBUG:
	for m := range cg.debugChan {
		if !iterator(m) {
			break ProcessDEBUG
		}
	}
}
*/
