import pymongo
import json
import plotly.express as px
import pandas as pd


class MongoConnector():
    """
    A class to connect to a MongoDB server and perform CRUD operations.

    Attributes:
        host (str): The hostname of the MongoDB server.
        port (int): The port number of the MongoDB server.
        db_name (str): The name of the MongoDB database.

    """

    def __init__(self, host: str, port: int, db_name: str) -> None:
        """
        Constructs a new MongoConnector object.

        Args:
            host (str): The hostname of the MongoDB server.
            port (int): The port number of the MongoDB server.
            db_name (str): The name of the MongoDB database.

        """
        self.host: str = host
        self.port: int = port
        self.db_name: str = db_name
        self.client = None
        self.db = None

    def connect(self) -> None:
        """
        Connects to the MongoDB server.
        """
        self.client: pymongo.MongoClient = pymongo.MongoClient(host=self.host, port=self.port)
        self.db = self.client[self.db_name]
        print(self.db)

    def disconnect(self) -> None:
        """
        Disconnects from the MongoDB server.
        """
        self.client.close()
    
    def flush_collection(self, collection_name: str) -> None:
        """
        Flushes a given collection on the established database on the MongoDB server.
        
        Args:
            collection_name (str): The collection name to flush.
        """
        collection= self.db[collection_name]

        # Delete all documents in the collection
        result = collection.delete_many({})
        print(f"Deleted {result.deleted_count} documents from {collection.name}")

    def remove_collection(self, collection_name: str) -> None:
        """
        Removes a given collection on the established database on the MongoDB server.

        Args:
            collection_name (str): The collection name to remove.
        """
        collection = self.db[collection_name]

        # Drop the collection
        collection.drop()
        print(f"Dropped {collection.name} from {self.db.name}")
    
    def create_collection(self, collection_name: str) -> None:
        """
        Creates a given collection on the established database on the MongoDB server.

        Args:
            collection_name (str): The collection name to create.
        """
        self.db.create_collection(collection_name)
        print(f"Created collecton {collection_name} in {self.db.name}")
    
    def collection_size(self, collection_name: str) -> int:
        """
        Checks the size of a given collection on the established database on the MongoDB server.

        Args:
            collection_name (str): The collection name to create.
        """
        return len(list(self.db[collection_name].find({})))

    def insert_data(self, collection_name: str, json_file: str, clear=False) -> None:
        """
        Inserts data from a JSON file into a MongoDB collection.

        Args:
            collection_name (str): The name of the MongoDB collection.
            json_file (str): The path to the JSON file.
            clear (bool): Whether to clear the collection of existing data.
        """
        try:
            collection = self.db.create_collection(collection_name)
        except:
            collection = self.db[collection_name]

        if clear:
            self.flush_collection(collection_name)

        with open(json_file) as f:
            # data: list = json.load(f)
            data: str = f.read()

            # Split the data into individual JSON objects
            objects: list[str] = data.strip().split('\n')

            # Wrap the objects in an array
            json_data: str = '[' + ','.join(objects) + ']'

            # Load the JSON data as a Python object
            data = json.loads(json_data)

            collection.insert_many(list(data))

        print(f"{len(data)} documents inserted into collection {collection_name} in the {self.db.name} database.")
    
    def search_query(self, collection_name: str, qu: dict, proj:dict, lim=10) -> list:
        """
        Method to execute queries on a given collection on the established database on the MongoDB server.

        Args:
            collection_name (str): The collection name to create.
            query (dict): The collection query to execute.
            projection (dict): The projection to map.
            lim (int): The limit on the number of objects to return.
        """

        try:
            collection = self.db.create_collection(collection_name)
        except:
            collection = self.db[collection_name]
        
        documents = collection.find(qu, proj).limit(lim)

        result: list = []

        # print each document
        for document in documents:
            print(document)
            result.append(document)

        return result


    def aggregate_query(self, collection_name: str, query) -> list:
        """
        Method to execute aggregate queries on a given collection on the established database on the MongoDB server.

        Args:
            collection_name (str): The collection name to create.
            query (dict | list): The collection query to execute.
        """

        try:
            collection = self.db.create_collection(collection_name)
        except:
            collection = self.db[collection_name]

        documents = collection.aggregate(query)

        # print each document
        result: list = []
        for document in documents:
            print(document)
            result.append(document)

        return result


if __name__ == '__main__':
    mongo: MongoConnector = MongoConnector('localhost', 27017, 'restaurants')
    mongo.connect()

    if mongo.collection_size("restaurants_collection") == 0:
        mongo.insert_data('restaurants_collection', 'data/restaurants.json', clear=False)

    print("\n Number of McDonald's in NYC: \n")
    mongo.aggregate_query("restaurants_collection", [
                                                    {
                                                        "$match": {
                                                                    "name":"Mcdonald'S"
                                                                    }
                                                    },
                                                    {"$count":"totalMcDonalds"}
                                                    ])

    print("\n Number of restaurants in each bourough: \n")
    mongo.aggregate_query("restaurants_collection", [
                                                    {
                                                        '$group': {
                                                            '_id': '$borough',
                                                            'count': {'$sum': 1}
                                                        }
                                                    }
                                                ])

    print("\n Boroughs with the highest number of Chinese restaurants --> give the number of Chinese restaurants in each boroughough: \n")
    mongo.aggregate_query("restaurants_collection", [
                                                    { "$match": { "cuisine": "Chinese" } },
                                                    {
                                                        "$group": {
                                                        "_id": "$borough",
                                                        "count": { "$sum": 1 }
                                                        }
                                                    },
                                                    { "$sort": { "count": -1 } }
                                                    ])
    
    print("\n Top 10 restaurants with the highest average score, sorted by average score (min 5 reviews): \n")
    mongo.aggregate_query("restaurants_collection", 
                                                       [{"$match": {"grades": {"$size": 5}}},
                                                        {"$project": {
                                                            "name": 1,
                                                            "borough": 1,
                                                            "cuisine": 1,
                                                            "grades": 1,
                                                            "avgScore": {"$avg": "$grades.score"}
                                                        }},
                                                        {"$sort": {"avgScore": -1}},
                                                        {"$limit": 10}
                                                    ])
    
    print("\n Restaurants which have a zipcode that starts with '10' and they are of either Italian or Chinese cuisine and have been graded 'A' in their latest grade: \n")
    mongo.search_query(collection_name="restaurants_collection", qu = {
                                                            "address.zipcode": { "$regex": "^10" },
                                                            "$or": [
                                                                { "cuisine": "Italian" },
                                                                { "cuisine": "Chinese" }
                                                            ],
                                                            "grades.0.grade": "A"
                                                        },
                                                proj = {
                                                    "name": 1,
                                                    "borough": 1,
                                                    "cuisine": 1,
                                                    "grades.date": 1,
                                                    "grades.grade": 1,
                                                    "grades.score": 1
                                                })
    
    print("\n All restaurants that are located in the Bronx borough and have an 'American' cuisine: \n")
    mongo.search_query(collection_name="restaurants_collection", qu = {"borough": "Bronx", "cuisine": "American"}, proj={ "name": 1,
                                                                                                                         "borough": 1,
                                                                                                                         "cuisine": 1}, lim=5)

    print("\n All restaurants that have a 'Pizza' cuisine and a 'B' grade in their latest inspection: \n")
    mongo.search_query(collection_name="restaurants_collection",     qu={
                                                                        "cuisine": "Pizza",
                                                                        "grades.0.grade": "B"
                                                                    },
                                                                    proj={
                                                                        "name": 1,
                                                                        "borough": 1,
                                                                        "cuisine": 1,
                                                                        "grades": {
                                                                            "$elemMatch": {"grade": "B"}
                                                                        }
                                                                    }, lim=5)

    print("\n Top 5 restaurants with the highest average score for their grades, and only show their name, cuisine, and average score: \n")
    mongo.search_query(collection_name="restaurants_collection",qu={}, proj={"name": 1, "cuisine": 1, "avgScore": {"$avg": "$grades.score"}, "_id": 0}, lim=5)


    print("\n Visualization of the distribution of restaurants across different cuisines in the NYC boroughs: \n")
    res: list = mongo.aggregate_query("restaurants_collection", [{"$group": {"_id": {"borough": "$borough", "cuisine": "$cuisine"}, "count": {"$sum": 1}}},
                                                                    {"$project": {"borough": "$_id.borough", "cuisine": "$_id.cuisine", "count": "$count", "_id": 0}}
                                                                    ])


    df: pd.DataFrame = pd.DataFrame(res)
    fig = px.bar(df, x="borough", y="count", color="cuisine", title="Restaurant counts by cuisine in NYC boroughs")
    fig.show()

    # mongo.flush_collection("restaurants_collection")
    mongo.disconnect()
