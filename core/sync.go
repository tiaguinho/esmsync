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
	total := mongodb.CountAll()
	fmt.Println(total, " documents finded")

	page := 0
	limit := 0
	for total > 0 {
		skip := page * 100
		if ok := total - 100; ok > 0 {
			limit = 100
		} else {
			limit = total
		}

		oplogs := mongodb.GetAll(skip, limit)
		go sync(oplogs)

		page++
		total -= limit
	}

}
