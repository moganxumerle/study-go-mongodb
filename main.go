package main

import (
	"fmt"
	"os"
	"study-go-mongodb/databases"

	"go.mongodb.org/mongo-driver/mongo"
)

const DATA_BASE_NAME = "quickstart"
const PODCASTS_COLLECTION = "podcasts"
const EPISODES_COLLECTION = "episodes"

var URI_MONGODB_ATLAS = os.Getenv("URI_MONGODB_ATLAS")

var podcastsCollection *mongo.Collection
var episodesCollection *mongo.Collection

func main() {

	mongoDB := databases.NewMongoDB(URI_MONGODB_ATLAS)
	defer func() {
		fmt.Println("Closing MongoDB connection")
		mongoDB.Disconnect()
	}()

	//Create and connection on new Database and Collections
	mongoDB.CreateDatabase(DATA_BASE_NAME)
	podcastsCollection = mongoDB.GetCollection(PODCASTS_COLLECTION)
	episodesCollection = mongoDB.GetCollection(EPISODES_COLLECTION)

	//insert
	mongoDB.InsertDocuments(podcastsCollection, episodesCollection)

	//query
	mongoDB.ReadAllOneByOneCollection(podcastsCollection)
	//mongoDB.ReadAllCollection(ctx, episodesCollection)
	//mongoDB.FindEpisodesByDuration(ctx, episodesCollection, 35)
	//mongoDB.FindEpisodesLongerThanDurationSortByDurationDesc(episodesCollection, 25)
}
