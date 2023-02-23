import pymongo
import json


class MongoConnector:
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
        collection = self.db[collection_name]

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
    
    def search_query(self, collection_name: str, query) -> None:
        """
        Method to execute queries on a given collection on the established database on the MongoDB server.

        Args:
            collection_name (str): The collection name to create.
            query (dict | list): The collection query to execute.
        """

        try:
            collection = self.db.create_collection(collection_name)
        except:
            collection = self.db[collection_name]

        documents = collection.find(query)

        # print each document
        for document in documents:
            print(document)

    def aggregate_query(self, collection_name: str, query) -> None:
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

        print(collection)

        documents = collection.aggregate(query)

        # print each document
        for document in documents:
            print(document)


if __name__ == '__main__':
    mongo: MongoConnector = MongoConnector('localhost', 27017, 'restaurants')
    mongo.connect()

    if mongo.collection_size("resturants_collection") == 0:
        mongo.insert_data('resturants_collection', 'data/restaurants.json', clear=False)

    mongo.aggregate_query("resturants_collection", [{"$match":{"name":"Mcdonald'S"}},{"$count":"totalMcDonalds"}])

    mongo.flush_collection("resturants_collection")
    mongo.disconnect()
