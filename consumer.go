package consumer

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/nsqio/go-nsq"
)

type logger interface {
	Output(int, string) error
}

// Consumer is a convenient layer to the standard NSQ Consumer.
type Consumer struct {
	client      *nsq.Consumer
	config      *nsq.Config
	nsqds       []string
	nsqlookupds []string
	concurrency int
	channel     string
	topic       string
	level       nsq.LogLevel
	log         logger
	err         error
}

// NewConsumer returns a new consumer of a given topic and channel.
func NewConsumer(topic, channel string) *Consumer {
	return &Consumer{
		log:         log.New(os.Stderr, "", log.LstdFlags),
		config:      nsq.NewConfig(),
		level:       nsq.LogLevelInfo,
		channel:     channel,
		topic:       topic,
		concurrency: 1,
	}
}

// SetLogger replaces the default NSQ logger.
func (c *Consumer) SetLogger(log logger, level nsq.LogLevel) {
	c.level = level
	c.log = log
}

// SetMap applies all options at once.
func (c *Consumer) SetMap(options map[string]interface{}) {
	for k, v := range options {
		c.Set(k, v)
	}
}

// Set takes an option as a string and a value as an interface
// and trying to set the appropriate option of consumer or its configuration.
//
// Any error will be returned in the Start() function.
//
// The following consumer options is implemented:
//
//  - `topic` consumer topic
//  - `channel` consumer channel
//  - `nsqd` nsqd address
//  - `nsqds` nsqd addresses separated by comma or space
//  - `nsqlookupd` nsqlookupd address
//  - `nsqlookupds` nsqlookupd addresses separated by comma or space
//  - `concurrency` concurrent handlers (default: 1)
func (c *Consumer) Set(option string, value interface{}) {
	switch option {
	case "topic":
		if s, ok := value.(string); ok {
			c.topic = s
		} else {
			c.err = fmt.Errorf("%q: expected string", option)
			return
		}
	case "channel":
		if s, ok := value.(string); ok {
			c.channel = s
		} else {
			c.err = fmt.Errorf("%q: expected string", option)
			return
		}
	case "concurrency":
		if s, ok := value.(int); ok {
			c.concurrency = s
		} else {
			c.err = fmt.Errorf("%q: expected integer", option)
			return
		}
	case "nsqd":
		if s, ok := value.(string); ok {
			c.nsqds = []string{s}
		} else {
			c.err = fmt.Errorf("%q: expected string", option)
			return
		}
	case "nsqlookupd":
		if s, ok := value.(string); ok {
			c.nsqlookupds = []string{s}
		} else {
			c.err = fmt.Errorf("%q: expected string", option)
			return
		}
	case "nsqds":
		if s, err := split(value); err == nil {
			c.nsqds = s
		} else {
			c.err = fmt.Errorf("%q: %v", option, err)
			return
		}
	case "nsqlookupds":
		if s, err := split(value); err == nil {
			c.nsqlookupds = s
		} else {
			c.err = fmt.Errorf("%q: %v", option, err)
			return
		}
	default:
		if err := c.config.Set(option, value); err != nil {
			c.err = err
		}
	}
}

// Start starts the consumer with a given handler.
//
// If there were an error on the configuration step, it will be returned here.
func (c *Consumer) Start(handler nsq.Handler) error {
	if c.err != nil {
		return c.err
	}

	client, err := nsq.NewConsumer(c.topic, c.channel, c.config)
	if err != nil {
		return err
	}
	c.client = client

	client.SetLogger(c.log, c.level)
	client.AddConcurrentHandlers(handler, c.concurrency)

	return c.connect()
}

// Stop initiates a graceful stop of the NSQ Consumer and waiting
// until this process completes.
func (c *Consumer) Stop() error {
	c.client.Stop()
	<-c.client.StopChan
	return nil
}

// Connect dials the connection to the specified nsqd(s) or nsqlookupd(s).
func (c *Consumer) connect() error {
	if len(c.nsqds) == 0 && len(c.nsqlookupds) == 0 {
		return fmt.Errorf(`at least one "nsqd" or "nsqlookupd" address must be specified`)
	}

	if len(c.nsqds) > 0 {
		err := c.client.ConnectToNSQDs(c.nsqds)
		if err != nil {
			return err
		}
	}

	if len(c.nsqlookupds) > 0 {
		err := c.client.ConnectToNSQLookupds(c.nsqlookupds)
		if err != nil {
			return err
		}
	}

	return nil
}

// Split slices an interface value into all substrings separated by comma or space and
// returns a slice of the substrings.
func split(value interface{}) ([]string, error) {
	switch value.(type) {
	case []string:
		return value.([]string), nil
	case string:
		return strings.FieldsFunc(value.(string), func(r rune) bool {
			switch r {
			case ',', ' ':
				return true
			}
			return false
		}), nil
	default:
		return nil, fmt.Errorf("expected string or slice of strings")
	}
}
