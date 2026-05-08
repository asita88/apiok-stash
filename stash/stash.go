package main

import (
	"database/sql"
	"flag"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/kevwan/go-stash/stash/config"
	"github.com/kevwan/go-stash/stash/es"
	"github.com/kevwan/go-stash/stash/filter"
	"github.com/kevwan/go-stash/stash/handler"
	"github.com/kevwan/go-stash/stash/input/file"
	stashkafka "github.com/kevwan/go-stash/stash/kafka"
	"github.com/kevwan/go-stash/stash/mysql"
	ip2regionsvc "github.com/lionsoul2014/ip2region/binding/golang/service"
	"github.com/olivere/elastic/v7"
	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/core/service"
)

var configFile = flag.String("f", "etc/config.yaml", "Specify the config file")

func toKqConf(c config.KafkaConf) []kq.KqConf {
	var ret []kq.KqConf

	for _, topic := range c.Topics {
		ret = append(ret, kq.KqConf{
			ServiceConf: c.ServiceConf,
			Brokers:     c.Brokers,
			Group:       c.Group,
			Topic:       topic,
			Offset:      c.Offset,
			Conns:       c.Conns,
			Consumers:   c.Consumers,
			Processors:  c.Processors,
			MinBytes:    c.MinBytes,
			MaxBytes:    c.MaxBytes,
			Username:    c.Username,
			Password:    c.Password,
		})
	}

	return ret
}

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	proc.SetTimeToForceQuit(c.GracePeriod)

	group := service.NewServiceGroup()
	defer group.Stop()

	for _, processor := range c.Clusters {
		var ip2r *ip2regionsvc.Ip2Region
		if len(processor.Ip2Region.Db) > 0 {
			var err error
			ip2r, err = ip2regionsvc.NewIp2RegionWithPath(processor.Ip2Region.Db, "")
			logx.Must(err)
			defer ip2r.Close()
		}

		filters := filter.CreateFilters(processor, ip2r)
		var handle *handler.MessageHandler

		if len(processor.Output.MySQL.DSN) > 0 {
			db, err := sql.Open("mysql", processor.Output.MySQL.DSN)
			logx.Must(err)
			logx.Must(db.Ping())

			writer, err := mysql.NewWriter(db, processor.Output.MySQL)
			logx.Must(err)
			indexer, err := mysql.NewTableWithDB(db, processor.Output.MySQL)
			logx.Must(err)
			handle = handler.NewHandler(writer, indexer)
		} else if len(processor.Output.Kafka.Brokers) > 0 && len(processor.Output.Kafka.Topic) > 0 {
			writer, err := stashkafka.NewWriter(processor.Output.Kafka)
			logx.Must(err)
			defer func() {
				if err := writer.Close(); err != nil {
					logx.Errorf("kafka writer close: %v", err)
				}
			}()

			var loc *time.Location
			if len(processor.Output.Kafka.TimeZone) > 0 {
				loc, err = time.LoadLocation(processor.Output.Kafka.TimeZone)
				logx.Must(err)
			} else {
				loc = time.Local
			}
			indexer := stashkafka.NewTopic(processor.Output.Kafka.Topic, loc)
			handle = handler.NewHandler(writer, indexer)
		} else {
			client, err := elastic.NewClient(
				elastic.SetSniff(false),
				elastic.SetURL(processor.Output.ElasticSearch.Hosts...),
				elastic.SetBasicAuth(processor.Output.ElasticSearch.Username, processor.Output.ElasticSearch.Password),
			)
			logx.Must(err)

			writer, err := es.NewWriter(processor.Output.ElasticSearch)
			logx.Must(err)

			var loc *time.Location
			if len(processor.Output.ElasticSearch.TimeZone) > 0 {
				loc, err = time.LoadLocation(processor.Output.ElasticSearch.TimeZone)
				logx.Must(err)
			} else {
				loc = time.Local
			}
			indexer := es.NewIndex(client, processor.Output.ElasticSearch.Index, loc)
			handle = handler.NewHandler(writer, indexer)
		}

		handle.AddFilters(filters...)
		handle.AddFilters(filter.AddUriFieldFilter("url", "uri"))

		if len(processor.Input.File.Paths) > 0 {
			fi, err := file.NewFileInput(processor.Input.File, handle)
			logx.Must(err)
			group.Add(fi)
		} else {
			for _, k := range toKqConf(processor.Input.Kafka) {
				group.Add(kq.MustNewQueue(k, handle))
			}
		}
	}

	group.Start()
}
