package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"study-go-mongodb/databases"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const DATA_BASE_NAME = "quickstart"
const PODCASTS_COLLECTION = "podcasts"
const EPISODES_COLLECTION = "episodes"

var URI_MONGODB_ATLAS = os.Getenv("URI_MONGODB_ATLAS")

var podcastsCollection *mongo.Collection
var episodesCollection *mongo.Collection

func main() {

	ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)

	mongoDB := databases.NewMongoDB(URI_MONGODB_ATLAS, ctx)
	defer mongoDB.Disconnect(ctx)

	//Create and connection on new Database and Collections
	mongoDB.CreateDatabase(DATA_BASE_NAME)
	podcastsCollection = mongoDB.GetCollection(PODCASTS_COLLECTION)
	episodesCollection = mongoDB.GetCollection(EPISODES_COLLECTION)

	//insert
	//insertDocuments(ctx, mongoDB)

	//query
	//mongoDB.ReadAllOneByOneCollection(ctx, podcastsCollection)
	//mongoDB.ReadAllCollection(ctx, episodesCollection)
	//mongoDB.FindEpisodesByDuration(ctx, episodesCollection, 35)
	mongoDB.FindEpisodesLongerThanDurationSortByDurationDesc(ctx, episodesCollection, 55)
}

func insertDocuments(ctx context.Context, mongoDB *databases.MongoDB) {

	//Insert into collections
	podcastResult, err := podcastsCollection.InsertOne(ctx, bson.D{
		{"title", "Mogancast"},
		{"author", "Mogan Xumerle"},
		{"tags", bson.A{"development", "programming", "coding", "finance"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Id PodCast Inserted: %v\n", podcastResult)

	episodeResult, err := episodesCollection.InsertMany(ctx, []interface{}{
		bson.D{
			{"podcast", podcastResult.InsertedID},
			{"title", "Studying MongoDB with Golang"},
			{"description", "Learning MongoDB"},
			{"duration", 35},
		},
		bson.D{
			{"podcast", podcastResult.InsertedID},
			{"title", "Studying Kafka with C#"},
			{"description", "Learning Kafka"},
			{"duration", 43},
		},
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Inserted %v documents into episode collection!\n", len(episodeResult.InsertedIDs))
}
