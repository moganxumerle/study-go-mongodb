package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const URI_MONGODB_ATLAS = ""

func main() {

	client, ctx := connectMongoDB()

	//Create and connection on new Database and Collections
	quickstartDatabase := client.Database("quickstart")
	podcastsCollection := quickstartDatabase.Collection("podcasts")
	episodesCollection := quickstartDatabase.Collection("episodes")

	//Insert into collections
	podcastResult, err := podcastsCollection.InsertOne(ctx, bson.D{
		{"title", "Mogancast"},
		{"author", "Mogan Xumerle"},
		{"tags", bson.A{"development", "programming", "coding", "finance"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Id PodCast Inserted: %v", podcastResult)

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

func connectMongoDB() (*mongo.Client, context.Context) {

	client, err := mongo.NewClient(options.Client().ApplyURI(URI_MONGODB_ATLAS))
	if err != nil {
		log.Fatal(err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatal(err)
	}

	databases, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(databases)

	return client, ctx
}
