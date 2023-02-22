import matplotlib.pyplot as plt
from pymongo import MongoClient
import os 

def connect_to_client():
    """
    Returns an instance of a mongo client by getting user information from the system.
    """
    username = os.getenv("MONGO_USER")
    password = os.getenv("MONGO_PASS")
    return MongoClient(f"mongodb+srv://{username}:{password}@cluster0.bslew.mongodb.net/test")


client = connect_to_client()
client.close()
