package es

import (
	"github.com/mattbaird/elastigo/lib"
)

//elasticsearch config struct
type ElasticConf struct {
	Server string
	Port   string
	User   string
	Pass   string
	Index  string
	Type   string
}

//elasticsearch client stuct
type Client struct {
	Conn *elastigo.Conn
	Conf ElasticConf
}

//struct to store data before sent request to elasticserach
type Elasticsearch struct {
	Id        string
	Data      map[string]interface{}
	Operation string
}

//create connection
func Connect(conf ElasticConf) *Client {
	client := new(Client)

	client.Conf = conf

	client.Conn = elastigo.NewConn()
	client.Conn.SetHosts([]string{conf.Server})
	client.Conn.SetPort(conf.Port)

	//check if index exists
	exists, _ := client.Conn.IndicesExists(conf.Index)
	if exists == false {
		client.Conn.CreateIndex(conf.Index)
	}

	//check health of the cluster
	health, _ := client.Conn.Health(conf.Index)
	if health.Status != "green" {
		client.Conn.DoCommand("PUT", "/_settings", nil, map[string]map[string]int{"index": {"number_of_replicas": 0}})
	}

	return client
}

//insert, update or delete index
func (c *Client) Execute(esdata Elasticsearch) {
	if esdata.Operation != "d" {
		c.Conn.Index(c.Conf.Index, c.Conf.Type, esdata.Id, nil, esdata.Data)
	} else {
		c.Conn.Delete(c.Conf.Index, c.Conf.Type, esdata.Id, nil)
	}
}
