package kafka

import (
	"context"
	"fmt"
	"time"

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
	Offsets map[string]map[int32]int64
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

// GETALL configures the ConsumerGroup to process all configured topics with the corresponding ProcessMessageFunc.
func (cg *ConsumerGroup) GETALL(handler ProcessMessageFunc) {
	for topic := range cg.handlers.Handles {
		cg.handlers.Handles[topic] = handler
	}
}

// Consume joins a cluster of consumers for a given list of topics and
// starts a blocking ConsumerGroupSession through the ConsumerGroupHandler.
func (cg *ConsumerGroup) Consume() error {
	var err error
	if len(cg.handlers.Handles) < 1 {
		panic("No topics configured for Consumer Group ")
	}
	var topics []string
	for t := range cg.handlers.Handles {
		topics = append(topics, t)
	}
ConsumeLoop:
	for {
		err = cg.consumer.Consume(cg.ctx, topics, cg.handlers)
		if err != nil {
			break ConsumeLoop
		}
		if cg.ctx.Err() != nil {
			break ConsumeLoop
		}
	}
	return err
}

// Errors returns a read channel of errors that occurred during the consumer life-cycle.
func (cg *ConsumerGroup) Errors() <-chan error {
	return cg.consumer.Errors()
}

// Close stops the ConsumerGroup and detaches any running sessions.
func (cg *ConsumerGroup) Close() error {
	return cg.consumer.Close()
}

// ResumeAll resumes all partitions that have been paused.
func (cg *ConsumerGroup) ResumeAll() {
	cg.consumer.ResumeAll()
}

// PauseAll suspends fetching from all partitions.
func (cg *ConsumerGroup) PauseAll() {
	cg.consumer.PauseAll()
}

// CGHandler implements sarama ConsumerGroupHandlers.
type CGHandler struct {
	Handles           TopicHandlerMap
	cg                *ConsumerGroup
	autoCommitEnabled bool
}

func newCGHandler(cg *ConsumerGroup, autoCommitEnabled bool) CGHandler {
	return CGHandler{
		Handles:           newTopicHandlerMap(),
		autoCommitEnabled: autoCommitEnabled,
		cg:                cg,
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
	for t, p := range sess.Claims() {
		if h.cg.Metadata.Members[t] == nil {
			h.cg.Metadata.Members[t] = make(map[int32]string)
		}
		if h.cg.Metadata.Offsets[t] == nil {
			h.cg.Metadata.Offsets[t] = make(map[int32]int64)
		}
		if sess.GenerationID() > h.cg.Metadata.GenIDs[t] {
			h.cg.Metadata.GenIDs[t] = sess.GenerationID()
		}
		for _, part := range p {
			h.cg.Metadata.Members[t][part] = sess.MemberID()
		}
	}
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (h CGHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	/* For Future TroubleShooting or Debugging options:
	for t, parts := range sess.Claims() {
		for _, p := range parts {
			fmt.Println(h.cg.Metadata.Members[t][p], "> Topic:", t, ">", p)
		}
	}
	*/
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (h CGHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	var (
		currentOffset = claim.InitialOffset()
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
			sess.MarkOffset(claim.Topic(), claim.Partition(), currentOffset+1, "")
			if !h.autoCommitEnabled {
				sess.Commit()
			}
		}
	} else {
		panic("No handler for given topic " + claim.Topic())
	}
	//sess.ResetOffset(claim.Topic(), claim.Partition(), currentOffset+1, "")
	/*  May not need to reset offset if none were committed.
	if currentOffset >= 0 {
		sess.ResetOffset(claim.Topic(), claim.Partition(), currentOffset+1, "")
	}
	*/
	return err
}

// NewConsumerGroup returns a new ConsumerGroup.
func NewConsumerGroup(ctx context.Context, addrs []string, groupID string, config *sarama.Config, topics ...string) (*ConsumerGroup, error) {
	if config == nil {
		config = GetConf("")
		config.Version = RecKafkaVersion
		config.Consumer.Return.Errors = true
		config.Consumer.Group.Session.Timeout = time.Second * 10
		config.Consumer.Group.Heartbeat.Interval = time.Second * 3
		config.Consumer.Group.Rebalance.Retry.Backoff = time.Second * 2
		config.Consumer.Group.Rebalance.Retry.Max = 3
		config.Consumer.MaxProcessingTime = time.Millisecond * 500
	}
	group, err := sarama.NewConsumerGroup(addrs, groupID, config)
	if err != nil {
		return nil, err
	}
	cg := &ConsumerGroup{
		GroupID:  groupID,
		consumer: group,
		ctx:      ctx,
	}
	cg.handlers = newCGHandler(cg, config.Consumer.Offsets.AutoCommit.Enable)
	cg.Metadata = CGMeta{
		Offsets: make(map[string]map[int32]int64),
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
func (kc *KClient) NewConsumerGroup(ctx context.Context, groupID string, topics ...string) (*ConsumerGroup, error) {
	/*	Consumer Group Options to be aware of:
		kc.config.Consumer.Return.Errors = true
		kc.config.Consumer.Group.Session.Timeout = time.Second * 10
		kc.config.Consumer.Group.Heartbeat.Interval = time.Second * 3
		kc.config.Consumer.Group.Rebalance.Retry.Backoff = time.Second * 2
		kc.config.Consumer.Group.Rebalance.Retry.Max = 3
		kc.config.Consumer.MaxProcessingTime = time.Millisecond * 500
	*/
	group, err := sarama.NewConsumerGroupFromClient(groupID, kc.cl)
	if err != nil {
		return nil, err
	}
	cg := &ConsumerGroup{
		GroupID:  groupID,
		consumer: group,
		ctx:      ctx,
	}
	cg.handlers = newCGHandler(cg, kc.config.Consumer.Offsets.AutoCommit.Enable)
	cg.Metadata = CGMeta{
		Offsets: make(map[string]map[int32]int64),
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
