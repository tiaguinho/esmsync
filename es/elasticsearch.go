package es

import (
	"encoding/json"
	"github.com/mattbaird/elastigo/lib"
	"io/ioutil"
)

//elasticsearch config struct
type ElasticConf struct {
	Server string `json:"server"`
	Port   string `json:"port"`
	User   string `json:"user"`
	Pass   string `json:"pass"`
	Index  string `json:"index"`
	Type   string `json:"type"`
}

var conf ElasticConf

var c *elastigo.Conn

//create connection
func init() {
	content, err := ioutil.ReadFile("./config/elasticsearch.json")
	if err == nil {
		err = json.Unmarshal(content, &conf)
		if err == nil {
			c = elastigo.NewConn()
			c.SetPort(conf.Port)
		}
	}
}

//index object
func Execute(esdata Elasticsearch) {
	var response elastigo.BaseResponse

	if esdata.Operation != "d" {
		response, _ = c.Index(conf.Index, conf.Type, esdata.Id, nil, esdata.Data)
	} else {
		response, _ = c.Delete(conf.Index, conf.Type, esdata.Id, nil)
	}

	if response.Ok {

	}
}
