import pymongo
import json
from typing import List, Dict


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
    
    def flush_collection(self, collection_name) -> None:
        db = self.client[self.db_name]
        collection = self.db[collection_name]

        # Delete all documents in the collection
        result = collection.delete_many({})
        print(f"Deleted {result.deleted_count} documents from {collection.name}")

    def remove_collection(self, collection_name) -> None:
        db = self.client[self.db_name]
        collection = self.db[collection_name]

        # Drop the collection
        collection.drop()
    
    def create_collection(self, collection_name) -> None:
        self.db.create_collection(collection_name)

    def insert_data(self, collection_name: str, json_file: str, clear=False) -> None:
        """
        Inserts data from a JSON file into a MongoDB collection.

        Args:
            collection_name (str): The name of the MongoDB collection.
            json_file (str): The path to the JSON file.

        """
        try:
            collection = self.db.create_collection(collection_name)
        except:
            collection = self.db[collection_name]

        if clear:
            self.flush_collection(collection_name)


        db_names: List[str] = self.client.list_database_names()

        for name in db_names:
            print(name)

        with open(json_file) as f:
            data = [json.load(f)]
            print(data)
            # collection.insert_many(data)

        # print(f"{len(data)} documents inserted into collection {collection_name}.")


if __name__ == '__main__':
    mongo: MongoConnector = MongoConnector('localhost', 27017, 'restaurants')
    mongo.connect()
    mongo.insert_data('restaurants_collection', '/Users/brianreicher/Downloads/restaurants.json')
    mongo.disconnect()
