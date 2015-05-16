package main

import (
	"encoding/json"
	"github.com/tiaguinho/esmsync/es"
	"github.com/tiaguinho/esmsync/mongo"
	"io/ioutil"
	"os"
	"reflect"
)

func main() {
	content, err := ioutil.ReadFile("./config/mongo.json")
	if err == nil {
		var conf mongo.MongoConf

		err = json.Unmarshal(content, &conf)
		if err == nil {
			session := mongo.GetConnection(conf)
			defer session.Close()

			inserts := mongo.GetOplogsInsert(session, conf.Db, conf.C)
			if len(inserts) > 0 {
				sync(inserts)
			}

			updates := mongo.GetOplogsUpdate(session, conf.Db, conf.C)
			if len(updates) > 0 {
				sync(updates)
			}

			deletes := mongo.GetOplogsDelete(session, conf.Db, conf.C)
			if len(deletes) > 0 {
				sync(deletes)
			}
		}
	}
}

//sync data between mongo and elasticsearch
func sync(oplogs interface{}) {
	length := reflect.ValueOf(oplogs).Len()

	for i := 1; i < length; i++ {
		es.Mapping(reflect.ValueOf(oplogs).Index(i).Interface())

		os.Exit(400)
	}
}
