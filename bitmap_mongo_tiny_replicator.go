package memory_storage

//
//import (
//	"bytes"
//	"context"
//	"errors"
//	"fmt"

//	"time"
//
//	"go.mongodb.org/mongo-driver/bson"
//	"go.mongodb.org/mongo-driver/mongo/options"
//

//)
//
//type BitmapMongoTinyReplicator struct {
//	forStorage  string
//}
//
//func NewBitmapMongoTinyReplicator(cfg *mongoConfig.Config, forStorage string) MemorySetStorageReplicator {
//	return &BitmapMongoTinyReplicator{
//		mongoConfig: cfg,
//		forStorage:  forStorage,
//	}
//}
//
//type replicationDoc struct {
//	ID        string    `bson:"_id"`
//	Data      []byte    `bson:"data"`
//	UpdatedAt time.Time `bson:"updatedAt"`
//}
//
//func (r *BitmapMongoTinyReplicator) Replicate(ctx context.Context, storage MemorySetStorage, replicationKey string, ttl time.Duration) error {
//	conn := mongodb.NewDBConn(*r.mongoConfig)
//	db, err := conn.ConnectToDB(ctx)
//	if err != nil {
//		return err
//	}
//	defer conn.DisconnectDB(ctx)
//
//	bitmapBytes, err := storage.GetBytesFromBitmap()
//	if err != nil {
//		return err
//	}
//
//	if bitmapBytes == nil {
//		return errors.New(fmt.Sprintf("[%s] bitmap is empty for replicate", r.forStorage))
//	}
//
//	filter := bson.M{"_id": replicationKey}
//	update := bson.M{
//		"$set": bson.M{
//			"data":      bitmapBytes,
//			"updatedAt": time.Now(),
//		},
//	}
//	opts := options.Update().SetUpsert(true)
//
//	_, err = db.Collection(mlp_models.CollectionBitmapReplicator).UpdateOne(ctx, filter, update, opts)
//	if err != nil {
//		return err
//	}
//
//	return nil
//}
//
//func (r *BitmapMongoTinyReplicator) Recover(ctx context.Context, storage MemorySetStorage, replicationKey string) error {
//	conn := mongodb.NewDBConn(*r.mongoConfig)
//	db, err := conn.ConnectToDB(ctx)
//	if err != nil {
//		return err
//	}
//	defer conn.DisconnectDB(ctx)
//
//	filter := bson.M{"_id": replicationKey}
//	var doc replicationDoc
//	err = db.Collection(mlp_models.CollectionBitmapReplicator).FindOne(ctx, filter).Decode(&doc)
//	if err != nil {
//		return fmt.Errorf("%s: %w", fmt.Sprintf("[%s] no document found in MongoDB for replicationKey=%s", r.forStorage, replicationKey), err)
//	}
//
//	if len(doc.Data) == 0 {
//		return errors.New(fmt.Sprintf("[%s] bitmap data is empty in MongoDB for replicationKey=%s", r.forStorage, replicationKey))
//	}
//
//	storage.Clear()
//	buffer := bytes.NewBuffer(doc.Data)
//	_, err = storage.ReadFromBuffer(ctx, buffer)
//	if err != nil {
//		return err
//	}
//
//	return nil
//}
//
//func (r *BitmapMongoTinyReplicator) DropReplicationKey(ctx context.Context, replicationKey string) error {
//	conn := mongodb.NewDBConn(*r.mongoConfig)
//	db, err := conn.ConnectToDB(ctx)
//	if err != nil {
//		return err
//	}
//	defer conn.DisconnectDB(ctx)
//
//	filter := bson.M{"_id": replicationKey}
//	_, err = db.Collection(mlp_models.CollectionBitmapReplicator).DeleteOne(ctx, filter)
//	if err != nil {
//		return err
//	}
//
//	return nil
//}
