package kafka

import (
	"time"

	"github.com/kevwan/go-stash/stash/format"
)

type Topic struct {
	topicFormat func(map[string]interface{}) string
}

func NewTopic(topicFormat string, loc *time.Location) *Topic {
	return &Topic{topicFormat: format.Format(topicFormat, loc)}
}

func (t *Topic) GetIndex(m map[string]interface{}) string {
	return t.topicFormat(m)
}
