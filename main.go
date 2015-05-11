package main

import (
	"encoding/json"
	"fmt"
	"github.com/tiaguinho/esmsync/mongo"
	"io/ioutil"
)

func main() {
	content, err := ioutil.ReadFile("./config/db.json")
	if err == nil {
		var conf mongo.MongoConf

		err = json.Unmarshal(content, &conf)
		if err == nil {
			session := mongo.GetConnection(conf)
			defer session.Close()

			deletes := mongo.GetOplogsDelete(session, conf.Db, conf.C)
			if len(deletes) > 0 {
				//TODO
			}

			updates := mongo.GetOplogsUpdate(session, conf.Db, conf.C)
			if len(updates) > 0 {
				//TODO
				fmt.Println(updates)
			}
		}
	}
}
