package mongo

import (
	"gopkg.in/mgo.v2"
	"log"
	"time"
)

//mongodb config struct
type MongoConf struct {
	Server     string
	Port       string
	User       string
	Pass       string
	Database   string
	Collection string
}

//mongodb client struct
type Client struct {
	Conn *mgo.Session
	Conf MongoConf
}

//return the client struct
func Connect(conf MongoConf) *Client {
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

	client := &Client{
		Conn: session,
		Conf: conf,
	}

	return client
}
