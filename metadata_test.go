package kafka

import (
	"testing"
	"time"
)

func TestGetTopicMetadata(t *testing.T) {
	clientTimeout := (time.Second * 5)
	clientRetries := 1
	//seedBroker := sarama.NewMockBroker(t, 1)
	//leader := sarama.NewMockBroker(t, 5)
	seedBroker, controllerBroker := getTestingBrokers(t)
	defer seedBroker.Close()
	defer controllerBroker.Close()

	/*
		replicas := []int32{3, 1, 5}
		isr := []int32{5, 1}
		metadataResponse := new(sarama.MetadataResponse)
		metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
		metadataResponse.AddTopicPartition("my_topic", 0, leader.BrokerID(), replicas, isr, sarama.ErrNoError)
		metadataResponse.AddTopicPartition("my_topic", 1, leader.BrokerID(), replicas, isr, sarama.ErrLeaderNotAvailable)
		seedBroker.Returns(metadataResponse)
	*/

	conf := GetConf()
	conf.Net.DialTimeout = clientTimeout
	conf.Net.ReadTimeout = clientTimeout
	conf.Net.WriteTimeout = clientTimeout
	conf.Metadata.Retry.Max = clientRetries
	conf.Version = MinKafkaVersion
	client, err := NewCustomClient(conf, seedBroker.Addr())
	if err != nil {
		t.Fatal(err)
	}

	tm, err := client.GetTopicMeta()
	if err != nil {
		t.Fatal(err)
	}
	tom := client.MakeTopicOffsetMap(tm)
	if len(tom) != 1 {
		t.Error("Client returned incorrect number of topics, expected 1, received:", len(tom))
	}
	if len(tom) == 1 {
		toMap := tom[0]
		if len(toMap.PartitionOffsets) != 2 {
			t.Error("Client returned incorrect number of partitions, expected 2, received:", len(toMap.PartitionOffsets))
		}
	}
	err = client.Close()
	if err != nil {
		t.Fatal(err)
	}
}
