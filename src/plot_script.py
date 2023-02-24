from mongo_connection import MongoDriver


if __name__ == '__main__':
    mongo: MongoDriver = MongoDriver('localhost', 27017, 'restaurants')
    mongo.connect()

    if mongo.collection_size("restaurants_collection") == 0:
        mongo.insert_data('restaurants_collection', 'data/restaurants.json', clear=False)
        print("\n Query to make a visualization of the distribution of restaurants across different cuisines in the NYC boroughs: \n")
    res: list = mongo.aggregate_query("restaurants_collection", [{"$group": {"_id": {"borough": "$borough", "cuisine": "$cuisine"}, "count": {"$sum": 1}}},
                                                                    {"$project": {"borough": "$_id.borough", "cuisine": "$_id.cuisine", "count": "$count", "_id": 0}}]
                                                                    , show=False)
    for item in res[:10]:
        print(item)

    mongo.plot_query(res, x_var="borough", y_var="count", color_on="cuisine", plot_title="Resturant Count by Cuisine in NYC Boroughs", save_as='../data/mongo_visualization.png')
    mongo.disconnect()