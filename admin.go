package kafka

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"

	"github.com/Shopify/sarama"
)

type clusterAdmin struct {
	client sarama.Client
	conf   *sarama.Config
}

// Admin contructs and returns an underlying Cluster Admin struct.
func (kc *KClient) Admin() sarama.ClusterAdmin {
	return &clusterAdmin{
		client: kc.cl,
		conf:   kc.config,
	}
}

func (ca *clusterAdmin) Close() error {
	return ca.client.Close()
}

func (ca *clusterAdmin) Controller() (*sarama.Broker, error) {
	return ca.client.Controller()
}

func (ca *clusterAdmin) CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error {

	if topic == "" {
		return sarama.ErrInvalidTopic
	}

	if detail == nil {
		return fmt.Errorf("You must specify topic details")
	}

	topicDetails := make(map[string]*sarama.TopicDetail)
	topicDetails[topic] = detail

	request := &sarama.CreateTopicsRequest{
		TopicDetails: topicDetails,
		ValidateOnly: validateOnly,
		Timeout:      ca.conf.Admin.Timeout,
	}

	if ca.conf.Version.IsAtLeast(sarama.V0_11_0_0) {
		request.Version = 1
	}
	if ca.conf.Version.IsAtLeast(sarama.V1_0_0_0) {
		request.Version = 2
	}

	b, err := ca.Controller()
	if err != nil {
		return err
	}

	rsp, err := b.CreateTopics(request)
	if err != nil {
		return err
	}

	topicErr, ok := rsp.TopicErrors[topic]
	if !ok {
		return sarama.ErrIncompleteResponse
	}

	if topicErr.Err != sarama.ErrNoError {
		return topicErr.Err
	}

	return nil
}

func (ca *clusterAdmin) DeleteTopic(topic string) error {

	if topic == "" {
		return sarama.ErrInvalidTopic
	}

	request := &sarama.DeleteTopicsRequest{
		Topics:  []string{topic},
		Timeout: ca.conf.Admin.Timeout,
	}

	if ca.conf.Version.IsAtLeast(sarama.V0_11_0_0) {
		request.Version = 1
	}

	b, err := ca.Controller()
	if err != nil {
		return err
	}

	rsp, err := b.DeleteTopics(request)
	if err != nil {
		return err
	}

	topicErr, ok := rsp.TopicErrorCodes[topic]
	if !ok {
		return sarama.ErrIncompleteResponse
	}

	if topicErr != sarama.ErrNoError {
		return topicErr
	}
	return nil
}

func (ca *clusterAdmin) findAnyBroker() (*sarama.Broker, error) {
	brokers := ca.client.Brokers()
	if len(brokers) > 0 {
		index := rand.Intn(len(brokers))
		return brokers[index], nil
	}
	return nil, errors.New("no available broker")
}

// ListTopics returns topic-details mapping. (Needs WiP*)
func (ca *clusterAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	// In order to build TopicDetails we need to first get the list of all
	// topics using a MetadataRequest and then get their configs using a
	// DescribeConfigsRequest request. To avoid sending many requests to the
	// broker, we use a single DescribeConfigsRequest.

	// Send the all-topic MetadataRequest
	b, err := ca.findAnyBroker()
	if err != nil {
		return nil, err
	}
	_ = b.Open(ca.client.Config())

	metadataReq := &sarama.MetadataRequest{}
	metadataResp, err := b.GetMetadata(metadataReq)
	if err != nil {
		return nil, err
	}

	topicsDetailsMap := make(map[string]sarama.TopicDetail)

	var describeConfigsResources []*sarama.ConfigResource

	for _, topic := range metadataResp.Topics {
		topicDetails := sarama.TopicDetail{
			NumPartitions: int32(len(topic.Partitions)),
		}
		if len(topic.Partitions) > 0 {
			topicDetails.ReplicaAssignment = map[int32][]int32{}
			for _, partition := range topic.Partitions {
				topicDetails.ReplicaAssignment[partition.ID] = partition.Replicas
			}
			topicDetails.ReplicationFactor = int16(len(topic.Partitions[0].Replicas))
		}
		topicsDetailsMap[topic.Name] = topicDetails

		// we populate the resources we want to describe from the MetadataResponse
		topicResource := sarama.ConfigResource{
			Type: sarama.TopicResource,
			Name: topic.Name,
		}
		describeConfigsResources = append(describeConfigsResources, &topicResource)
	}

	// Send the DescribeConfigsRequest
	describeConfigsReq := &sarama.DescribeConfigsRequest{
		Resources: describeConfigsResources,
	}
	describeConfigsResp, err := b.DescribeConfigs(describeConfigsReq)
	if err != nil {
		return nil, err
	}

	for _, resource := range describeConfigsResp.Resources {
		topicDetails := topicsDetailsMap[resource.Name]
		topicDetails.ConfigEntries = make(map[string]*string)

		for _, entry := range resource.Configs {
			// only include non-default non-sensitive config
			// (don't actually think topic config will ever be sensitive)
			if entry.Default || entry.Sensitive {
				continue
			}
			topicDetails.ConfigEntries[entry.Name] = &entry.Value
		}

		topicsDetailsMap[resource.Name] = topicDetails
	}

	return topicsDetailsMap, nil
}

func (ca *clusterAdmin) CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error {
	if topic == "" {
		return sarama.ErrInvalidTopic
	}

	topicPartitions := make(map[string]*sarama.TopicPartition)
	topicPartitions[topic] = &sarama.TopicPartition{Count: count, Assignment: assignment}

	request := &sarama.CreatePartitionsRequest{
		TopicPartitions: topicPartitions,
		Timeout:         ca.conf.Admin.Timeout,
		ValidateOnly:    validateOnly,
	}

	b, err := ca.Controller()
	if err != nil {
		return err
	}

	rsp, err := b.CreatePartitions(request)
	if err != nil {
		return err
	}

	topicErr, ok := rsp.TopicPartitionErrors[topic]
	if !ok {
		return sarama.ErrIncompleteResponse
	}

	if topicErr.Err != sarama.ErrNoError {
		return topicErr.Err
	}

	return nil
}

func (ca *clusterAdmin) DeleteRecords(topic string, partitionOffsets map[int32]int64) error {

	if topic == "" {
		return sarama.ErrInvalidTopic
	}

	topics := make(map[string]*sarama.DeleteRecordsRequestTopic)
	topics[topic] = &sarama.DeleteRecordsRequestTopic{PartitionOffsets: partitionOffsets}
	request := &sarama.DeleteRecordsRequest{
		Topics:  topics,
		Timeout: ca.conf.Admin.Timeout,
	}

	b, err := ca.Controller()
	if err != nil {
		return err
	}

	rsp, err := b.DeleteRecords(request)
	if err != nil {
		return err
	}

	_, ok := rsp.Topics[topic]
	if !ok {
		return sarama.ErrIncompleteResponse
	}

	//todo since we are dealing with couple of partitions it would be good if we return slice of errors
	//for each partition instead of one error
	return nil
}

// DescribeCluster retrieves information about the nodes in the cluster.
func (ca *clusterAdmin) DescribeCluster() (brokers []*sarama.Broker, controllerID int32, err error) {
	controller, err := ca.Controller()
	if err != nil {
		return nil, int32(0), err
	}

	request := &sarama.MetadataRequest{
		Topics: []string{},
	}

	response, err := controller.GetMetadata(request)
	if err != nil {
		return nil, int32(0), err
	}

	return response.Brokers, response.ControllerID, nil
}

func (ca *clusterAdmin) DescribeTopics(topics []string) (metadata []*sarama.TopicMetadata, err error) {
	controller, err := ca.Controller()
	if err != nil {
		return nil, err
	}

	request := &sarama.MetadataRequest{
		Topics:                 topics,
		AllowAutoTopicCreation: false,
	}

	if ca.conf.Version.IsAtLeast(sarama.V0_11_0_0) {
		request.Version = 4
	}

	response, err := controller.GetMetadata(request)
	if err != nil {
		return nil, err
	}
	return response.Topics, nil
}

func (ca *clusterAdmin) DescribeConfig(resource sarama.ConfigResource) ([]sarama.ConfigEntry, error) {

	var entries []sarama.ConfigEntry
	var resources []*sarama.ConfigResource
	resources = append(resources, &resource)

	request := &sarama.DescribeConfigsRequest{
		Resources: resources,
	}

	b, err := ca.Controller()
	if err != nil {
		return nil, err
	}

	rsp, err := b.DescribeConfigs(request)
	if err != nil {
		return nil, err
	}

	for _, rspResource := range rsp.Resources {
		if rspResource.Name == resource.Name {
			if rspResource.ErrorMsg != "" {
				return nil, fmt.Errorf("%v", rspResource.ErrorMsg)
			}
			for _, cfgEntry := range rspResource.Configs {
				entries = append(entries, *cfgEntry)
			}
		}
	}
	return entries, nil
}

func (ca *clusterAdmin) AlterConfig(resourceType sarama.ConfigResourceType, name string, entries map[string]*string, validateOnly bool) error {

	var resources []*sarama.AlterConfigsResource
	resources = append(resources, &sarama.AlterConfigsResource{
		Type:          resourceType,
		Name:          name,
		ConfigEntries: entries,
	})

	request := &sarama.AlterConfigsRequest{
		Resources:    resources,
		ValidateOnly: validateOnly,
	}

	b, err := ca.Controller()
	if err != nil {
		return err
	}

	rsp, err := b.AlterConfigs(request)
	if err != nil {
		return err
	}

	for _, rspResource := range rsp.Resources {
		if rspResource.Name == name {
			if rspResource.ErrorMsg != "" {
				return fmt.Errorf(rspResource.ErrorMsg)
			}
		}
	}
	return nil
}

func (ca *clusterAdmin) CreateACL(resource sarama.Resource, acl sarama.Acl) error {
	var acls []*sarama.AclCreation
	acls = append(acls, &sarama.AclCreation{resource, acl})
	request := &sarama.CreateAclsRequest{AclCreations: acls}

	b, err := ca.Controller()
	if err != nil {
		return err
	}

	_, err = b.CreateAcls(request)
	return err
}

func (ca *clusterAdmin) ListAcls(filter sarama.AclFilter) ([]sarama.ResourceAcls, error) {

	request := &sarama.DescribeAclsRequest{AclFilter: filter}

	b, err := ca.Controller()
	if err != nil {
		return nil, err
	}

	rsp, err := b.DescribeAcls(request)
	if err != nil {
		return nil, err
	}

	var lAcls []sarama.ResourceAcls
	for _, rAcl := range rsp.ResourceAcls {
		lAcls = append(lAcls, *rAcl)
	}
	return lAcls, nil
}

func (ca *clusterAdmin) DeleteACL(filter sarama.AclFilter, validateOnly bool) ([]sarama.MatchingAcl, error) {
	var filters []*sarama.AclFilter
	filters = append(filters, &filter)
	request := &sarama.DeleteAclsRequest{Filters: filters}

	b, err := ca.Controller()
	if err != nil {
		return nil, err
	}

	rsp, err := b.DeleteAcls(request)
	if err != nil {
		return nil, err
	}

	var mAcls []sarama.MatchingAcl
	for _, fr := range rsp.FilterResponses {
		for _, mACL := range fr.MatchingAcls {
			mAcls = append(mAcls, *mACL)
		}

	}
	return mAcls, nil
}

// DescribeConsumerGroups returns consumer group details. (Needs WiP*)
func (ca *clusterAdmin) DescribeConsumerGroups(groups []string) (result []*sarama.GroupDescription, err error) {
	groupsPerBroker := make(map[*sarama.Broker][]string)

	for _, group := range groups {
		controller, err := ca.client.Coordinator(group)
		if err != nil {
			return nil, err
		}
		groupsPerBroker[controller] = append(groupsPerBroker[controller], group)

	}

	for broker, brokerGroups := range groupsPerBroker {
		response, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{
			Groups: brokerGroups,
		})
		if err != nil {
			return nil, err
		}

		result = append(result, response.Groups...)
	}
	return result, nil
}

// ListConsumerGroups returns a group-protocol mapping. (Needs WiP*)
func (ca *clusterAdmin) ListConsumerGroups() (allGroups map[string]string, err error) {
	allGroups = make(map[string]string)

	// Query brokers in parallel, since we have to query *all* brokers
	brokers := ca.client.Brokers()
	groupMaps := make(chan map[string]string, len(brokers))
	errors := make(chan error, len(brokers))
	wg := sync.WaitGroup{}

	for _, b := range brokers {
		wg.Add(1)
		go func(b *sarama.Broker, conf *sarama.Config) {
			defer wg.Done()
			_ = b.Open(conf) // Ensure that broker is opened

			response, err := b.ListGroups(&sarama.ListGroupsRequest{})
			if err != nil {
				errors <- err
				return
			}

			groups := make(map[string]string)
			for group, typ := range response.Groups {
				groups[group] = typ
			}

			groupMaps <- groups

		}(b, ca.conf)
	}

	wg.Wait()
	close(groupMaps)
	close(errors)

	for groupMap := range groupMaps {
		for group, protocolType := range groupMap {
			allGroups[group] = protocolType
		}
	}

	// Intentionally return only the first error for simplicity
	err = <-errors
	return
}

// ListConsumerGroupOffsets returns group offsets for the given topic-partition map. (Needs WiP*)
func (ca *clusterAdmin) ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	coordinator, err := ca.client.Coordinator(group)
	if err != nil {
		return nil, err
	}

	request := &sarama.OffsetFetchRequest{
		ConsumerGroup: group,
		//partitions:    topicPartitions,
	}

	if ca.conf.Version.IsAtLeast(sarama.V0_8_2_2) {
		request.Version = 1
	}

	return coordinator.FetchOffset(request)
}
