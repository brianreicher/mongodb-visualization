import pymongo
import json
import plotly.express as px
import plotly.io as pio
import pandas as pd


class MongoDriver():
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
    
    def search_query(self, collection_name: str, qu: dict, proj:dict, lim=10, show=True) -> list:
        """
        Method to execute queries on a given collection on the established database on the MongoDB server.

        Args:
            collection_name (str): The collection name to create.
            query (dict): The collection query to execute.
            projection (dict): The projection to map.
            lim (int): The limit on the number of objects to return.
            show (bool): Whether to display the given query.
        """

        # set the collection name, create a new collection if necessary
        try:
            collection = self.db.create_collection(collection_name)
        except:
            collection = self.db[collection_name]
        
        # execute the find() query with given query, projection, and limit
        documents = collection.find(qu, proj).limit(lim)

        result: list = []

        # append each doc to the result, printing if necessary
        for document in documents:
            if show:
                print(document)
            result.append(document)

        return result


    def aggregate_query(self, collection_name: str, query, show=True) -> list:
        """
        Method to execute aggregate queries on a given collection on the established database on the MongoDB server.

        Args:
            collection_name (str): The collection name to create.
            query (dict | list): The collection query to execute.
            show (bool): Whether to display the given query.
        """

        # set the collection name, create a new collection if necessary
        try:
            collection = self.db.create_collection(collection_name)
        except:
            collection = self.db[collection_name]

        # execute the aggregate() query
        documents = collection.aggregate(query)

        # append each doc to the result, printing if necessary
        result: list = []
        for document in documents:
            if show:
                print(document)
            result.append(document)

        return result
    
    @staticmethod
    def plot_query( mongo_responnse: list, 
                    x_var: str, 
                    y_var: str, 
                    color_on:str, 
                    plot_title:str, 
                    save_as=None) -> None:
        """
        Method to plot visualizations from a given MongoDB query response.

        Args:
            mongo_responnse (list): The query response.
            x_var (str): The query variable to plot on the x-axis.
            y_var (str): The query variable to plot on the y-axis.
            color_on (str): The query variable to color the plot on.
            plot_title (str): Display title for the visualization.
            save_as (None | str): Defines where and how to save the resulting plot.
        """

        # write the query output to a dataframe
        df: pd.DataFrame = pd.DataFrame(mongo_responnse)

        # create the figure given the passed x_var, y_var, color, and title
        fig = px.bar(df, x=x_var, y=y_var, color=color_on, title=plot_title)

        # display the figure and save if desired
        fig.show()
        if save_as is not None:
            pio.write_image(fig, save_as)


if __name__ == '__main__':
    mongo: MongoDriver = MongoDriver('localhost', 27017, 'restaurants')
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
                                                       [{"$match": {"grades":{"$gt": 5}}},
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


    print("\n All restaurants that have a 'Chinese' or 'Japanese' cuisine, and are located in the 'Queens' borough, and have a grade 'A' in their latest inspection. \n")
    mongo.search_query(collection_name="restaurants_collection",     qu={
                                                                        "borough": "Queens", 
                                                                        "cuisine": {"$in": ["Chinese", "Japanese"]}, 
                                                                        "grades.0.grade": "A"
                                                                    }, 
                                                                    proj={
                                                                        "name": 1, 
                                                                        "borough": 1, 
                                                                        "cuisine": 1, 
                                                                        "grades": 1,
                                                                        "_id": 0
                                                                    },
                                                                    lim=5)

    print("\n Visualization of the distribution of restaurants across different cuisines in the NYC boroughs: \n")
    res: list = mongo.aggregate_query("restaurants_collection", [{"$group": {"_id": {"borough": "$borough", "cuisine": "$cuisine"}, "count": {"$sum": 1}}},
                                                                    {"$project": {"borough": "$_id.borough", "cuisine": "$_id.cuisine", "count": "$count", "_id": 0}}]
                                                                    , show=False)

    # plot the previous query
    mongo.plot_query(res, x_var="borough", y_var="count", color_on="cuisine", plot_title="Resturant Count by Cusiene in NYC Boroughs", save_as='./data/mongo_visualization.png')

    # mongo.flush_collection("restaurants_collection")
    mongo.disconnect()
