package main

import (
	"github.com/tiaguinho/esmsync/core"
	"github.com/tiaguinho/esmsync/mongo"
)

func main() {
	core.Start()
	listen := core.Listen()

	var docs []mongo.Oplog
	for {
		err := listen.All(&docs)
		if err == nil && len(docs) > 0 {
			//call function to sync data
			core.Do(docs[len(docs)-1].Ts)
		}

		if listen.Err() != nil {
			listen.Close()
		}

		if listen.Timeout() {
			continue
		}

		listen = core.Listen()
	}

	listen.Close()
}
