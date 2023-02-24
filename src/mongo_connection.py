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
        try:
            self.client: pymongo.MongoClient = pymongo.MongoClient(host=self.host, port=self.port)
            self.db = self.client[self.db_name]
            print(self.db)
        except pymongo.errors.ConnectionFailure as e:
            print(f"Failed to connect to MongoDB server: {e}")

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
    
    def search_query(self, collection_name: str, qu: dict, proj:dict, lim=10, show=False) -> list:
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


    def aggregate_query(self, collection_name: str, query, show=False) -> list:
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
