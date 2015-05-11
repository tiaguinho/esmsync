package main

import (
	"encoding/json"
	"fmt"
	"github.com/tiaguinho/esmsync/es"
	"github.com/tiaguinho/esmsync/mongo"
	"io/ioutil"
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

			}

			updates := mongo.GetOplogsUpdate(session, conf.Db, conf.C)
			if len(updates) > 0 {
				//TODO
			}

			deletes := mongo.GetOplogsDelete(session, conf.Db, conf.C)
			if len(deletes) > 0 {
				//TODO
			}
		}
	}
}
