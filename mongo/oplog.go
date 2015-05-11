package mongo

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

//base struct of Oplog
type Oplog struct {
	Ts bson.MongoTimestamp `bson:"ts"`
	H  int                 `bson:"h"`
	V  int                 `bson:"v"`
	Op string              `bson:"op"`
	Ns string              `bson:"ns"`
}

//struct of Oplog insert object
type OplogInsert struct {
	Oplog `bson:",inline"`
	O     bson.M `bson:"o"`
}

//struct of Oplog update object
type OplogUpdate struct {
	Oplog `bson:",inline"`
	O2    bson.ObjectId `bson:"o2>_id"`
	O     bson.M        `bson:"o"`
}

type OplogDelete struct {
	Oplog `bson:",inline"`
	B     bool          `bson:"b"`
	O     bson.ObjectId `bson:"o>_id"`
}

//return all inserted oplog objects
func GetOplogsInsert(session *mgo.Session, database, collection string) []OplogInsert {
	c := session.DB("local").C("oplog.rs")

	var logs []OplogInsert
	c.Find(bson.M{"op": "i", "ns": database + "." + collection, "ts": bson.M{"$type": 17}}).All(&logs)

	return logs
}

//return all updated oplog objects
func GetOplogsUpdate(session *mgo.Session, database, collection string) []OplogUpdate {
	c := session.DB("local").C("oplog.rs")

	var logs []OplogUpdate
	c.Find(bson.M{"op": "u", "ns": database + "." + collection, "ts": bson.M{"$type": 17}}).All(&logs)

	return logs
}

//return all deleted oplog objects
func GetOplogsDelete(session *mgo.Session, database, collection string) []OplogDelete {
	c := session.DB("local").C("oplog.rs")

	var logs []OplogDelete
	c.Find(bson.M{"op": "d", "ns": database + "." + collection, "ts": bson.M{"$type": 17}}).All(&logs)

	return logs
}
