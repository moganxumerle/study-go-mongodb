package databases

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoDB struct {
	client   *mongo.Client
	dataBase *mongo.Database
}

func NewMongoDB(uriMongoAtlas string, ctx context.Context) *MongoDB {

	client, err := mongo.NewClient(options.Client().ApplyURI(uriMongoAtlas))
	if err != nil {
		log.Fatal(err)
	}

	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatal(err)
	}

	databases, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(databases)

	return &MongoDB{
		client: client,
	}
}

func (m *MongoDB) CreateDatabase(databaseName string) {
	m.dataBase = m.client.Database(databaseName)
}

func (m *MongoDB) GetCollection(collection string) *mongo.Collection {
	return m.dataBase.Collection(collection)
}

func (m *MongoDB) Disconnect(ctx context.Context) {
	defer m.client.Disconnect(ctx)
}
