from pymongo import MongoClient
import random

# MongoDB connection URI
uri = os.getenv('uri')


def add_watch_lists():
    """
    Connects to the MongoDB cluster, accesses the sample_mflix database,
    and adds a 'watch_movies_list' array to each user in the users collection.
    The array contains between 10 and 50 movie _ids from the embedded_movies collection.
    """
    client = None  # Initialize client to None
    try:
        # Create a new client and connect to the server
        client = MongoClient(uri)

        # Access the sample_mflix database
        db = client.sample_mflix

        # Access the users and embedded_movies collections
        users_collection = db.users
        embedded_movies_collection = db.embedded_movies

        # Get all movie IDs from the embedded_movies collection
        all_movie_ids = [movie['_id'] for movie in embedded_movies_collection.find({"plot_embedding": {"$exists": True}}, {'_id': 1})]

        # Iterate through each user in the users collection
        for user in users_collection.find():
            # Generate a random number of movies to add to the watch list
            num_movies = random.randint(10, 50)

            # Randomly select movie IDs without replacement, handle edge case of fewer movies than num_movies
            watch_movies_list = random.sample(all_movie_ids, min(num_movies, len(all_movie_ids)))

            # Update the user document to add the watch_movies_list array
            users_collection.update_one(
                {'_id': user['_id']},
                {'$set': {'watch_movies_list': watch_movies_list}}
            )
            print(f"Updated user: {user['_id']} with {len(watch_movies_list)} movies") #Added print

        print("Successfully added 'watch_movies_list' to all users.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the MongoDB client
        if client:
            client.close()

if __name__ == "__main__":
    add_watch_lists()