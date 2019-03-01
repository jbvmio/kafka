package kafka

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

const defaultTimestampFormat = time.RFC3339

var (
	logger  *log.Logger
	logging bool
)

type KClient struct {
	apiVers map[int16]int16
	brokers []*sarama.Broker

	cl     sarama.Client
	ca     clusterAdmin
	config *sarama.Config
	logger *log.Logger
}

func NewClient(brokerList ...string) (*KClient, error) {
	conf := GetConf("")
	client, err := getClient(conf, brokerList...)
	if err != nil {
		return nil, err
	}
	kc := KClient{
		cl:      client,
		config:  conf,
		brokers: client.Brokers(),
		logger:  logger,
	}
	kc.Connect()
	return &kc, nil
}

func NewCustomClient(conf *sarama.Config, brokerList ...string) (*KClient, error) {
	var client sarama.Client
	switch {
	case conf == nil:
		var err error
		client, err = getClient(conf, brokerList...)
		if err != nil {
			return nil, err
		}
	default:
		err := conf.Validate()
		if err != nil {
			return nil, err
		}
		client, err = getClient(conf, brokerList...)
		if err != nil {
			return nil, err
		}
	}
	kc := KClient{
		cl:      client,
		config:  conf,
		brokers: client.Brokers(),
		logger:  logger,
	}
	kc.Connect()
	return &kc, nil
}

func (kc *KClient) Controller() (*sarama.Broker, error) {
	return kc.cl.Controller()
}

func (kc *KClient) Connect() error {
	for _, broker := range kc.brokers {
		if ok, _ := broker.Connected(); !ok {
			if err := broker.Open(kc.config); err != nil {
				return fmt.Errorf("Error connecting to broker: %v", err)
			}
		}
	}
	return nil
}

func (kc *KClient) IsConnected() bool {
	for _, broker := range kc.brokers {
		if ok, _ := broker.Connected(); ok {
			return true
		}
	}
	return false
}

func (kc *KClient) Close() error {
	errString := fmt.Sprintf("ERROR:\n")
	var errFound bool
	for _, b := range kc.brokers {
		if ok, _ := b.Connected(); ok {
			if err := b.Close(); err != nil {
				e := fmt.Sprintf("  Error closing broker, %v: %v\n", b.Addr(), err)
				errString = string(errString + e)
				errFound = true
			}
		}
	}
	if err := kc.cl.Close(); err != nil {
		e := fmt.Sprintf("  Error closing client: %v\n", err)
		errString = string(errString + e)
		errFound = true
	}
	if errFound {
		return fmt.Errorf("%v", errString)
	}
	return nil
}

// Logger Enables Verbose Logging in the logFormat given. Format is text by default. Valid option for now are either `json` or `text`.
func Logger(logFormat ...string) {
	validateLogger(logFormat...)
}

func validateLogger(logFormat ...string) {
	switch {
	case !logging:
		switch {
		case len(logFormat) > 0:
			switch {
			case logFormat[0] == "json":
				configJSONLogger()
			default:
				configDefaultLogger()
			}
		default:
			configDefaultLogger()
		}
	case len(logFormat) > 0:
		switch {
		case logFormat[0] == "json":
			configJSONLogger()
		}
	case logging:
		break
	default:
		configDefaultLogger()
	}
}

func configDefaultLogger() {
	logger = log.New()
	logger.Out = os.Stdout
	logger.Formatter = &log.TextFormatter{
		TimestampFormat: defaultTimestampFormat,
		FullTimestamp:   true,
	}
	logging = true
	sarama.Logger = logger
}

func configJSONLogger() {
	logger = log.New()
	logger.Out = os.Stdout
	logger.Formatter = &log.JSONFormatter{
		TimestampFormat: defaultTimestampFormat,
	}
	logging = true
	sarama.Logger = logger
}

func Warnf(format string, v ...interface{}) {
	validateLogger()
	logger.Warnf(format, v...)
}

func (kc *KClient) Logf(format string, v ...interface{}) {
	validateLogger()
	kc.logger.Printf(format, v...)
}

func (kc *KClient) Log(v ...interface{}) {
	validateLogger()
	kc.logger.Println(v...)
}

func (kc *KClient) Warnf(format string, v ...interface{}) {
	validateLogger()
	kc.logger.Warnf(format, v...)
}

// SaramaConfig returns and allows modification of the underlying sarama client config
func (kc *KClient) SaramaConfig() *sarama.Config {
	return kc.config
}

// GetConf returns a *sarama.Config and optionally sets the ClientID if specified.
// If entered, the ClientID is automatically postfixed with a random Hex value, ie: clientID-6f5ecf.
// If a zero value string is entered (""), then a random Hex value will be set as the ClientID, ie: 6f5ecf.
func GetConf(clientID ...string) *sarama.Config {
	conf := sarama.NewConfig()
	switch {
	case len(clientID) == 0:
		break
	case clientID[0] == "":
		random := makeHex(3)
		conf.ClientID = string(random)
	case len(clientID) > 0 && clientID[0] != "":
		random := makeHex(3)
		conf.ClientID = string(clientID[0] + "-" + random)
	}
	return conf
}

func getClient(conf *sarama.Config, brokers ...string) (sarama.Client, error) {
	return sarama.NewClient(brokers, conf)
}

// ReturnFirstValid returns the first available, connectable broker provided from a broker list
func ReturnFirstValid(brokers ...string) (string, error) {
	conf := GetConf()
	for _, b := range brokers {
		broker := sarama.NewBroker(b)
		broker.Open(conf)
		ok, _ := broker.Connected()
		if ok {
			return b, nil
		}
	}
	return "", fmt.Errorf("No Connectivity to Provided Brokers")
}

func randomBytes(n int) []byte {
	return makeByte(n)
}

func makeHex(s int) string {
	b := randomBytes(s)
	hexstring := hex.EncodeToString(b)
	return hexstring
}

func makeByte(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}
