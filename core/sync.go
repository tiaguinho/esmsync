package core

import (
	"fmt"
	"github.com/tiaguinho/esmsync/es"
	"reflect"
)

//sync data between mongo and elasticsearch
func sync(oplogs interface{}) int {
	length := reflect.ValueOf(oplogs).Len()

	var total int
	for i := 0; i < length; i++ {
		esdata := es.Mapping(reflect.ValueOf(oplogs).Index(i).Interface())

		if len(esdata.Data) > 0 {
			elastic.Execute(esdata)

			total++
		}
	}

	return total
}

//sync all data on the collection
func syncAll() {
	oplogs := mongodb.GetAll()
	if len(oplogs) > 0 {
		total := sync(oplogs)

		fmt.Println(total, " documents synchronized")
	}
}
