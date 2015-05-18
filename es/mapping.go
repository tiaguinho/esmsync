package es

import (
	"encoding/json"
	"github.com/tiaguinho/esmsync/mongo"
	"gopkg.in/mgo.v2/bson"
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
func Mapping(oplog interface{}) (object Elasticsearch) {
	nodes := getNodesFile()

	var data map[string]interface{}
	switch reflect.ValueOf(oplog).Field(0).FieldByName("Op").String() {
	case "i":
		s := oplog.(mongo.OplogInsert)
		data = s.O
		object = Elasticsearch{
			Id:        s.O["_id"].(bson.ObjectId).Hex(),
			Operation: "i",
		}
	case "u":
		s := oplog.(mongo.OplogUpdate)
		data = s.O["$set"].(map[string]interface{})
		object = Elasticsearch{
			Id:        s.O2["_id"].Hex(),
			Operation: "u",
		}
	case "d":
		s := oplog.(mongo.OplogDelete)
		object = Elasticsearch{
			Id:        s.O["_id"].Hex(),
			Operation: "d",
		}
	}

	if len(data) != 0 {
		object.Data = make(map[string]interface{}, len(nodes))

		for _, node := range nodes {
			rs := getValue(node.MongoField, node.Type, data)
			if rs != nil {
				object.Data[node.EsField] = rs
			}
		}
	}

	return object
}

//return a value of the field
func getValue(key, data_type string, data map[string]interface{}) (resp interface{}) {
	if data[key] == nil {
		fields := strings.Split(key, ">")

		r := extractValue(fields, data)
		if r != nil {
			resp = r
		}
	} else {
		resp = data[key]
	}

	if resp != nil {
		if data_type != reflect.TypeOf(resp).Kind().String() {
			temps := make([]interface{}, 1)
			temps[0] = resp

			resp = temps
		}
	}

	return
}

func extractValue(fields []string, data interface{}) (result interface{}) {
	var index int
	if len(fields) > 1 {
		index = 1
	}

	if data != nil {
		if reflect.TypeOf(data).Kind() == reflect.Map {
			result = extractValue(fields[index:], data.(map[string]interface{})[fields[0]])
		} else if reflect.TypeOf(data).Kind() == reflect.Slice {
			length := reflect.ValueOf(data).Len()

			results := make([]interface{}, length)
			for i := 0; i < length; i++ {
				results[i] = extractValue(fields[index:], reflect.ValueOf(data).Index(i).Interface())
			}

			result = results
		} else if data != nil {
			result = data
		}
	}

	return
}
