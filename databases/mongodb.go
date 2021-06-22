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

func (m *MongoDB) ReadAllCollection(ctx context.Context, collection *mongo.Collection) {

	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}

	var documents []bson.M
	if err = cursor.All(ctx, &documents); err != nil {
		log.Fatal(err)
	}

	fmt.Println(documents)
}

func (m *MongoDB) ReadAllOneByOneCollection(ctx context.Context, collection *mongo.Collection) {

	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {

		var document bson.M

		if err = cursor.Decode(&document); err != nil {
			log.Fatal()
		}

		fmt.Println(document)
	}
}

func (m *MongoDB) FindEpisodesByDuration(ctx context.Context, collection *mongo.Collection, duration int) {

	filterCursor, err := collection.Find(ctx, bson.M{"duration": duration})
	if err != nil {
		log.Fatal(err)
	}

	var episodesFiltered []bson.M
	if err = filterCursor.All(ctx, &episodesFiltered); err != nil {
		log.Fatal(err)
	}

	fmt.Println(episodesFiltered)
}

func (m *MongoDB) FindEpisodesLongerThanDurationSortByDurationDesc(ctx context.Context, collection *mongo.Collection, duration int) {

	opts := options.Find()
	opts.SetSort(bson.D{{"duration", -1}})

	filterSortCursor, err := collection.Find(ctx, bson.D{{"duration", bson.D{{"$gt", duration}}}}, opts)
	if err != nil {
		log.Fatal(err)
	}

	var episodesSorted []bson.M
	if err = filterSortCursor.All(ctx, &episodesSorted); err != nil {
		log.Fatal(err)
	}

	fmt.Println(episodesSorted)
}
