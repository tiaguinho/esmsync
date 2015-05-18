package core

import (
	"fmt"
	"github.com/tiaguinho/esmsync/es"
	"github.com/tiaguinho/esmsync/mongo"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Config struct {
	Mongo         mongo.MongoConf
	Elasticsearch es.ElasticConf
}

//configuration struct for yaml data
var conf Config

//elasticserach client
var elastic *es.Client

//mongodb client
var mongodb *mongo.Client

//create file to control the data to sincronize
func init() {
	content, err := ioutil.ReadFile("./config/esmsync.yaml")
	if err == nil {
		err = yaml.Unmarshal(content, &conf)
		if err != nil {
			fmt.Println(err)
		}
	}
}

//start the connection with mongodb and elasticsearch
func Start() {
	mongodb = mongo.Connect(conf.Mongo)
	elastic = es.Connect(conf.Elasticsearch)

	insertOplogs := mongodb.GetOplogsInsert()
	if len(insertOplogs) > 0 {
		sync(insertOplogs)
	}

	updateOplogs := mongodb.GetOplogsUpdate()
	if len(updateOplogs) > 0 {
		sync(updateOplogs)
	}

	deleteOplogs := mongodb.GetOplogsDelete()
	if len(deleteOplogs) > 0 {
		sync(deleteOplogs)
	}
}
