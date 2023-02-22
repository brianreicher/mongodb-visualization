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
    
    def flush_collection(self, collection_name) -> None:
        collection = self.db[collection_name]

        # Delete all documents in the collection
        result = collection.delete_many({})
        print(f"Deleted {result.deleted_count} documents from {collection.name}")

    def remove_collection(self, collection_name) -> None:
        collection = self.db[collection_name]

        # Drop the collection
        collection.drop()
        print(f"Dropped {collection.name} from {self.db.name}")
    
    def create_collection(self, collection_name) -> None:
        self.db.create_collection(collection_name)
        print(f"Created collecton {collection_name} in {self.db.name}")

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

        with open(json_file) as f:
            data: list = [json.load(f)]
            print(data)
            collection.insert_many(data)

        print(f"{len(data)} documents inserted into collection {collection_name} in {self.db.name}.")
    
    def query(self, collection_name: str) -> None:
        # find documents where the 'status' field is 'active'
        try:
            collection = self.db.create_collection(collection_name)
        except:
            collection = self.db[collection_name]

        print(collection)

        documents = collection.find()

        # print each document
        for document in documents:
            print(document)



if __name__ == '__main__':
    mongo: MongoConnector = MongoConnector('localhost', 27017, 'sample')
    mongo.connect()
    mongo.insert_data('sample_coll', 'data/sample.json', clear=False)
    mongo.query('sample_coll')

    mongo.flush_collection("sample_coll")
    mongo.disconnect()
