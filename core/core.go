package core

import (
	"fmt"
	"github.com/tiaguinho/esmsync/es"
	"github.com/tiaguinho/esmsync/mongo"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"strconv"
	"time"
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

//last oplog document date
var LastTs int64

//create file to control the data to sincronize
func init() {
	content, err := ioutil.ReadFile("./config/esmsync.yaml")
	if err == nil {
		err = yaml.Unmarshal(content, &conf)
		if err != nil {
			fmt.Println(err)
		}
	}

	mongodb = mongo.Connect(conf.Mongo)
	elastic = es.Connect(conf.Elasticsearch)

	//check
	content, err = ioutil.ReadFile("./esmsync.txt")
	if err == nil {
		LastTs, _ = strconv.ParseInt(string(content), 10, 64)
	}
}

//start the connection with mongodb and elasticsearch to sync data
func Do(last_ts int64) {
	LastTs = last_ts
	ioutil.WriteFile("./esmsync.txt", []byte(strconv.FormatInt(LastTs, 10)), 0644)

	var total int64

	insertOplogs := mongodb.GetOplogsInsert()
	if len(insertOplogs) > 0 {
		total += sync(insertOplogs)
	}

	updateOplogs := mongodb.GetOplogsUpdate()
	if len(updateOplogs) > 0 {
		total += sync(updateOplogs)
	}

	deleteOplogs := mongodb.GetOplogsDelete()
	if len(deleteOplogs) > 0 {
		total += sync(deleteOplogs)
	}

	fmt.Println(total, " documents synchonized")
}

//create a listener to oplog collection
func Listen() *mgo.Iter {
	fmt.Println("Listen mongodb on port:", mongodb.Conf.Port)

	collection := mongodb.Conn.DB("local").C("oplog.rs")

	var query bson.M
	if LastTs != 0 {
		query = bson.M{"_id": bson.M{"$gt": LastTs}}
	}

	return collection.Find(query).Sort("$natural").Tail(5 * time.Second)
}
