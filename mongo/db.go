package mongo

import (
	"gopkg.in/mgo.v2"
	"log"
	"time"
)

//mongodb config struct
type MongoConf struct {
	Server string `json:"server"`
	Port   string `json:"port"`
	User   string `json:"user"`
	Pass   string `json:"pass"`
	Db     string `json:"database"`
	C      string `json:"collection"`
}

//return the connection with mongodb
func GetConnection(conf MongoConf) *mgo.Session {
	session, err := mgo.DialWithTimeout(conf.Server+":"+conf.Port, time.Second*60)
	if err == nil {
		if conf.User != "" {
			err := session.Login(&mgo.Credential{Username: conf.User, Password: conf.Pass})
			if err != nil {
				log.Fatal(err)
			}
		}
	} else {
		log.Fatal(err)
	}

	return session
}
