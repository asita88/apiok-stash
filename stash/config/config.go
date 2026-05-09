package config

import (
	"time"

	"github.com/zeromicro/go-zero/core/service"
)

type (
	Condition struct {
		Key   string
		Value string
		Type  string `json:",default=match,options=match|contains"`
		Op    string `json:",default=and,options=and|or"`
	}

	ElasticSearchConf struct {
		Hosts         []string
		Index         string
		DocType       string `json:",default=doc"`
		TimeZone      string `json:",optional"`
		MaxChunkBytes int    `json:",default=15728640"`
		Compress      bool   `json:",default=false"`
		Username      string `json:",optional"`
		Password      string `json:",optional"`
	}

	MySQLConf struct {
		DSN            string `json:",optional"`
		Table          string
		CreateTableSQL string   `json:",optional"`
		InsertColumns  []string `json:",optional"`
		TimeZone       string   `json:",optional"`
		MaxChunkSize   int      `json:",default=1000"`
		MaxChunkBytes  int      `json:",default=5242880"`
	}

	Filter struct {
		Action     string      `json:",options=drop|remove_field|transfer|ip2region|parse_time"`
		Conditions []Condition `json:",optional"`
		Fields     []string    `json:",optional"`
		Field      string      `json:",optional"`
		Target     string      `json:",optional"`
		TimeZone   string      `json:",optional"`
	}

	Ip2RegionConf struct {
		Db string `json:",optional"`
	}

	KafkaConf struct {
		service.ServiceConf
		Brokers    []string
		Group      string
		Topics     []string
		Offset     string `json:",options=first|last,default=last"`
		Conns      int    `json:",default=1"`
		Consumers  int    `json:",default=8"`
		Processors int    `json:",default=8"`
		MinBytes   int    `json:",default=10240"`
		MaxBytes   int    `json:",default=10485760"`
		Username   string `json:",optional"`
		Password   string `json:",optional"`
	}

	KafkaWriterConf struct {
		Brokers       []string
		Topic         string
		TimeZone      string `json:",optional"`
		Username      string `json:",optional"`
		Password      string `json:",optional"`
		MaxChunkBytes int    `json:",default=1048576"`
	}

	FileConf struct {
		Paths         []string `json:",optional"`
		Format        string   `json:",default=json,options=json|plain"`
		Follow        bool     `json:",default=true"`
		FromBeginning bool     `json:",default=false"`
		MaxLineSize   int      `json:",default=1048576"`
	}

	Cluster struct {
		Input struct {
			Kafka KafkaConf `json:",optional"`
			File  FileConf  `json:",optional"`
		}
		Ip2Region Ip2RegionConf `json:",optional"`
		Filters   []Filter      `json:",optional"`
		Output    struct {
			ElasticSearch ElasticSearchConf `json:",optional"`
			MySQL         MySQLConf         `json:",optional"`
			Kafka         KafkaWriterConf   `json:",optional"`
		}
	}

	Config struct {
		Clusters    []Cluster
		GracePeriod time.Duration `json:",default=10s"`
	}
)
