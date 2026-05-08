package kafka

import (
	"context"

	"github.com/kevwan/go-stash/stash/config"
	kafkaio "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/logx"
)

type Writer struct {
	writer   *kafkaio.Writer
	inserter *executors.ChunkExecutor
}

type valueWithTopic struct {
	topic string
	val   string
}

func NewWriter(c config.KafkaWriterConf) (*Writer, error) {
	batch := c.MaxChunkBytes
	if batch <= 0 {
		batch = 1048576
	}

	var transport *kafkaio.Transport
	if c.Username != "" || c.Password != "" {
		transport = &kafkaio.Transport{
			SASL: plain.Mechanism{
				Username: c.Username,
				Password: c.Password,
			},
		}
	}

	kw := &kafkaio.Writer{
		Addr:                   kafkaio.TCP(c.Brokers...),
		Transport:              transport,
		Balancer:               &kafkaio.LeastBytes{},
		BatchBytes:             int64(batch),
		AllowAutoTopicCreation: true,
	}

	w := &Writer{writer: kw}
	w.inserter = executors.NewChunkExecutor(w.execute, executors.WithChunkBytes(batch))
	return w, nil
}

func (w *Writer) Write(topic, val string) error {
	return w.inserter.Add(valueWithTopic{topic: topic, val: val}, len(val))
}

func (w *Writer) execute(vals []interface{}) {
	msgs := make([]kafkaio.Message, 0, len(vals))
	for _, v := range vals {
		p := v.(valueWithTopic)
		msgs = append(msgs, kafkaio.Message{
			Topic: p.topic,
			Value: []byte(p.val),
		})
	}
	if err := w.writer.WriteMessages(context.Background(), msgs...); err != nil {
		logx.Error(err)
	}
}

func (w *Writer) Close() error {
	return w.writer.Close()
}
