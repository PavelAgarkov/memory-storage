package memory_storage

//
//import (
//	"bytes"
//	"context"
//	"errors"
//	"fmt"

//	"io"
//	"time"
//
//
//	"go.mongodb.org/mongo-driver/bson"
//	"go.mongodb.org/mongo-driver/bson/primitive"
//	"go.mongodb.org/mongo-driver/mongo"
//	"go.mongodb.org/mongo-driver/mongo/gridfs"
//	"go.mongodb.org/mongo-driver/mongo/options"
//

//)
//
//type BitmapGridFSReplicator struct {
//	forStorage  string
//}
//
//func NewBitmapGridFSReplicator(cfg *mongoConfig.Config, forStorage string) MemorySetStorageReplicator {
//	return &BitmapGridFSReplicator{
//		mongoConfig: cfg,
//		forStorage:  forStorage,
//	}
//}
//
//type gridFSDoc struct {
//	ID        string             `bson:"_id"`
//	FileID    primitive.ObjectID `bson:"fileId"`
//	UpdatedAt time.Time          `bson:"updatedAt"`
//}
//
//func (r *BitmapGridFSReplicator) Replicate(ctx context.Context, storage MemorySetStorage, replicationKey string, ttl time.Duration) error {
//	conn := mongodb.NewDBConn(*r.mongoConfig)
//	db, err := conn.ConnectToDB(ctx)
//	if err != nil {
//		return err
//	}
//	defer conn.DisconnectDB(ctx)
//
//	ps := "fs"
//	bucket, err := gridfs.NewBucket(
//		db,
//		&options.BucketOptions{
//			Name: &ps,
//		},
//	)
//	if err != nil {
//		return err
//	}
//
//	collection := db.Collection(mlp_models.CollectionBitmapReplicator)
//
//	var existing gridFSDoc
//	err = collection.FindOne(ctx, bson.M{"_id": replicationKey}).Decode(&existing)
//
//	// любая другая ошибка нас не беспокоит
//	if err != nil && err != mongo.ErrNoDocuments {
//		return err
//	}
//
//	if err == nil {
//		errDel := bucket.Delete(existing.FileID)
//		if errDel != nil && errDel != mongo.ErrNoDocuments {
//		}
//	}
//
//	bitmapBytes, err := storage.GetBytesFromBitmap()
//	if err != nil {
//		return err
//	}
//	if bitmapBytes == nil {
//		return errors.New(fmt.Sprintf("[%s] bitmap is empty for replicate", r.forStorage))
//	}
//
//	uploadStream, err := bucket.OpenUploadStream(
//		replicationKey,
//	)
//	if err != nil {
//		return err
//	}
//	defer uploadStream.Close()
//
//	_, err = uploadStream.Write(bitmapBytes)
//	if err != nil {
//
//		return err
//	}
//	newFileID := uploadStream.FileID.(primitive.ObjectID)
//
//	filter := bson.M{"_id": replicationKey}
//	update := bson.M{
//		"$set": bson.M{
//			"fileId":    newFileID,
//			"updatedAt": time.Now(),
//		},
//	}
//	opts := options.Update().SetUpsert(true)
//	_, err = collection.UpdateOne(ctx, filter, update, opts)
//	if err != nil {
//		return err
//	}
//
//	return nil
//}
//
//func (r *BitmapGridFSReplicator) Recover(ctx context.Context, storage MemorySetStorage, replicationKey string) error {
//	conn := mongodb.NewDBConn(*r.mongoConfig)
//	db, err := conn.ConnectToDB(ctx)
//	if err != nil {
//		return err
//	}
//	defer conn.DisconnectDB(ctx)
//
//	coll := db.Collection(mlp_models.CollectionBitmapReplicator)
//	var doc gridFSDoc
//	err = coll.FindOne(ctx, bson.M{"_id": replicationKey}).Decode(&doc)
//	if err != nil {
//		return err
//	}
//
//	if doc.FileID.IsZero() {
//		return errors.New(fmt.Sprintf("[%s] GridFS: fileId is empty for key=%s", r.forStorage, replicationKey))
//	}
//
//	bucket, err := gridfs.NewBucket(db)
//	if err != nil {
//		return err
//	}
//
//	downloadStream, err := bucket.OpenDownloadStream(doc.FileID)
//	if err != nil {
//		return err
//	}
//	defer downloadStream.Close()
//
//	var buf bytes.Buffer
//	_, err = io.Copy(&buf, downloadStream)
//	if err != nil {
//		return err
//	}
//
//	storage.Clear()
//	_, err = storage.ReadFromBuffer(ctx, &buf)
//	if err != nil {
//		return err
//	}
//
//	return nil
//}
//
//func (r *BitmapGridFSReplicator) DropReplicationKey(ctx context.Context, replicationKey string) error {
//	conn := mongodb.NewDBConn(*r.mongoConfig)
//	db, err := conn.ConnectToDB(ctx)
//	if err != nil {
//		return err
//	}
//	defer conn.DisconnectDB(ctx)
//
//	coll := db.Collection(mlp_models.CollectionBitmapReplicator)
//	var doc gridFSDoc
//	err = coll.FindOne(ctx, bson.M{"_id": replicationKey}).Decode(&doc)
//	if err != nil {
//		if err == mongo.ErrNoDocuments {
//			// если нед документа нет, то просто выходим
//			return nil
//		}
//		return err
//	}
//
//	bucket, err := gridfs.NewBucket(db)
//	if err != nil {
//		return err
//	}
//
//	err = bucket.Delete(doc.FileID)
//	if err != nil && err != mongo.ErrNoDocuments {
//	}
//
//	_, err = coll.DeleteOne(ctx, bson.M{"_id": replicationKey})
//
//	return err
//}
