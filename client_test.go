package kafka

import (
	"sort"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/spf13/cast"
)

func TestNewClient(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 1)
	metaResponse := new(sarama.MetadataResponse)
	seedBroker.Returns(metaResponse)
	client, err := NewClient(seedBroker.Addr())
	if err != nil {
		t.Fatal(err)
	}
	err = client.Close()
	if err != nil {
		t.Fatal(err)
	}
	seedBroker.Close()
}

func TestCustomClient(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 1)
	metaResponse := new(sarama.MetadataResponse)
	seedBroker.Returns(metaResponse)
	conf := GetConf("testID")
	conf.Metadata.Retry.Max = 0
	client, err := NewCustomClient(conf, seedBroker.Addr())
	if err != nil {
		t.Fatal(err)
	}
	err = client.Close()
	if err != nil {
		t.Fatal(err)
	}
	seedBroker.Close()
}

func TestClusterMetaRequest(t *testing.T) {
	clientTimeout := (time.Second * 5)
	clientRetries := 1
	seedBroker := sarama.NewMockBroker(t, 1)
	defer seedBroker.Close()
	controllerBroker := sarama.NewMockBroker(t, 2)
	defer controllerBroker.Close()
	seedBroker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(controllerBroker.BrokerID()).
			SetBroker(seedBroker.Addr(), seedBroker.BrokerID()).
			SetBroker(controllerBroker.Addr(), controllerBroker.BrokerID()),
	})
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
	cm, err := client.clusterMetaTest()
	if err != nil {
		t.Fatal(err)
	}
	if cm.BrokerCount() != 2 {
		t.Error("Client returned incorrect number of available brokers, expected 2, received:", cm.BrokerCount())
	}
	err = client.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func (kc *KClient) clusterMetaTest() (ClusterMeta, error) {
	cm := ClusterMeta{}
	res, err := kc.ReqMetadata()
	if err != nil {
		return cm, err
	}
	grps, errs := kc.ListGroups()
	if len(errs) > 0 {
		cm.ErrorStack = append(cm.ErrorStack, errs...)
	}
	cm.Controller = res.ControllerID
	for _, b := range res.Brokers {
		id := b.ID()
		addr := b.Addr()
		broker := string(addr + "/" + cast.ToString(id))
		cm.Brokers = append(cm.Brokers, broker)
		cm.BrokerIDs = append(cm.BrokerIDs, id)
	}
	for _, t := range res.Topics {
		cm.Topics = append(cm.Topics, t.Name)
	}
	cm.Groups = grps
	sort.Strings(cm.Groups)
	sort.Strings(cm.Brokers)
	sort.Strings(cm.Topics)
	return cm, nil
}
