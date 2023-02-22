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

def get_cuisine_ratings(client):
    """
    Queries Mongo for the distinct cuisine types, 
    then queries the restaurants for each cuisine, calculating the average score. 
    """
    db = client["Restaurants"]
    collection = db["New York"]
    cuisines = collection.distinct("cuisine")
    cuisine_scores = []
    for c in cuisines:
        restaurants = collection.find({"cuisine": c})
        scores = []
        for restaurant in restaurants:
            scores += [review["score"] for review in restaurant["grades"] if review["score"] is not None]
        cuisine_scores.append(0 if len(scores) == 0 else sum(scores) / len(scores))
    return cuisines, cuisine_scores


def make_graph(cuisines, scores):
    """
    Creates the graph given the cuisine and score lists (Scores on the Y Axis, for Cuisine Type Bars)
    """
    plt.bar(cuisines, scores, align='center', alpha=0.5)
    plt.ylabel('Average Scores')
    plt.xticks(rotation=90)
    plt.title('Average Review Scores by Cuisines in NYC')
    plt.show()


client = connect_to_client()
cuisines, cuisine_scores = get_cuisine_ratings(client)
make_graph(cuisines, cuisine_scores)
client.close()
