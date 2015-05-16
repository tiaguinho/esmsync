package es

import (
	"encoding/json"
	"fmt"
	"github.com/tiaguinho/esmsync/mongo"
	"io/ioutil"
	"reflect"
	"strings"
)

//Mapping struct
type Node struct {
	MongoField string `json:"mongo"`
	Type       string `json:"type"`
	EsField    string `json:"es"`
}

func getNodesFile() []Node {
	var nodes []Node

	content, err := ioutil.ReadFile("./config/mapping.json")
	if err == nil {
		json.Unmarshal(content, &nodes)
	}

	return nodes
}

//map a struct to the model defined in mapping.json
func Mapping(oplog interface{}) {
	nodes := getNodesFile()

	var data map[string]interface{}
	switch reflect.ValueOf(oplog).Field(0).FieldByName("Op").String() {
	case "i":
		s := oplog.(mongo.OplogInsert)
		data = s.O
	case "u":
		s := oplog.(mongo.OplogUpdate)
		data = s.O
		//case "d":
		//s := oplog.(mongo.OplogDelete)
	}

	if len(data) != 0 {
		for _, node := range nodes {
			getValue(node.MongoField, data)
		}
	} else {
		//TODO
	}
}

//return a value of the field
func getValue(key string, data map[string]interface{}) {
	if data[key] == nil {
		fields := strings.Split(key, ">")

		for _, field := range fields {
			if reflect.TypeOf(data).Kind() == reflect.Map {
				resp = data[field]
				//TODO
			} else {
				fmt.Println(key, "=", data)
			}
		}
	} else {
		fmt.Println(key, "=", data[key])
	}
}
