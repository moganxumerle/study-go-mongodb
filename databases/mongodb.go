package databases

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoDB struct {
	ctx      context.Context
	client   *mongo.Client
	dataBase *mongo.Database
}

func NewMongoDB(uriMongoAtlas string) *MongoDB {

	ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)

	client, err := mongo.NewClient(options.Client().ApplyURI(uriMongoAtlas))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connecting MongoDB...")
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("MongoDB connected!")

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatal(err)
	}

	/* List Databases Name
	databases, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(databases)
	*/

	return &MongoDB{
		ctx:    ctx,
		client: client,
	}
}

func (m *MongoDB) CreateDatabase(databaseName string) {
	m.dataBase = m.client.Database(databaseName)
}

func (m *MongoDB) GetCollection(collection string) *mongo.Collection {
	return m.dataBase.Collection(collection)
}

func (m *MongoDB) Disconnect() {
	defer m.client.Disconnect(m.ctx)
}

func (m *MongoDB) ReadAllCollection(collection *mongo.Collection) {

	cursor, err := collection.Find(m.ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}

	var documents []bson.M
	if err = cursor.All(m.ctx, &documents); err != nil {
		log.Fatal(err)
	}

	fmt.Println(documents)
}

func (m *MongoDB) ReadAllOneByOneCollection(collection *mongo.Collection) {

	cursor, err := collection.Find(m.ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(m.ctx)

	for cursor.Next(m.ctx) {

		var document bson.M

		if err = cursor.Decode(&document); err != nil {
			log.Fatal()
		}

		fmt.Println(document)
	}
}

func (m *MongoDB) FindEpisodesByDuration(collection *mongo.Collection, duration int) {

	filterCursor, err := collection.Find(m.ctx, bson.M{"duration": duration})
	if err != nil {
		log.Fatal(err)
	}

	var episodesFiltered []bson.M
	if err = filterCursor.All(m.ctx, &episodesFiltered); err != nil {
		log.Fatal(err)
	}

	fmt.Println(episodesFiltered)
}

func (m *MongoDB) FindEpisodesLongerThanDurationSortByDurationDesc(collection *mongo.Collection, duration int) {

	opts := options.Find()
	opts.SetSort(bson.D{{"duration", -1}})

	filterSortCursor, err := collection.Find(m.ctx, bson.D{{"duration", bson.D{{"$gt", duration}}}}, opts)
	if err != nil {
		log.Fatal(err)
	}

	var episodesSorted []bson.M
	if err = filterSortCursor.All(m.ctx, &episodesSorted); err != nil {
		log.Fatal(err)
	}

	fmt.Println(episodesSorted)
}

func (m *MongoDB) InsertDocuments(podcastsCollection, episodesCollection *mongo.Collection) {

	//Insert into collections
	podcastResult, err := podcastsCollection.InsertOne(m.ctx, bson.D{
		{"title", "Mogancast"},
		{"author", "Mogan Xumerle"},
		{"tags", bson.A{"development", "programming", "coding", "finance"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Id PodCast Inserted: %v\n", podcastResult)

	episodeResult, err := episodesCollection.InsertMany(m.ctx, []interface{}{
		bson.D{
			{"podcast", podcastResult.InsertedID},
			{"title", "New Studying MongoDB with Golang"},
			{"description", "Learning MongoDB"},
			{"duration", 35},
		},
		bson.D{
			{"podcast", podcastResult.InsertedID},
			{"title", "New Studying Kafka with C#"},
			{"description", "Learning Kafka"},
			{"duration", 43},
		},
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Inserted %v documents into episode collection!\n", len(episodeResult.InsertedIDs))
}

func (m *MongoDB) UpdatePodCastDocument(podcastsCollection *mongo.Collection, id, author string) bool {

	podcastId, _ := primitive.ObjectIDFromHex(id)

	result, err := podcastsCollection.UpdateOne(
		m.ctx,
		bson.M{"_id": podcastId},
		bson.D{
			{"$set", bson.D{{"author", author}}},
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Updated %v Documents!\n", result.ModifiedCount)

	return result.ModifiedCount > 0
}

func (m *MongoDB) DeletePodCastDocuments(collection *mongo.Collection, author string) bool {

	result, err := collection.DeleteMany(m.ctx, bson.M{"author": author})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("DeleteMany removed %v document(s)\n", result.DeletedCount)

	return result.DeletedCount > 0
}
