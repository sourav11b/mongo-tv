from pymongo import MongoClient
import random
import os
from dotenv import load_dotenv
import openai  # Import the openai library
import time


# Load environment variables from .env file
load_dotenv()
# Replace with your actual MongoDB URI
FIREWORK_API_KEY = os.getenv('FIREWORK_API_KEY')


# MongoDB connection URI
uri = os.getenv('uri')

def add_user_embeddings():
    """
    Connects to the MongoDB cluster, accesses the sample_mflix database,
    fetches user data, generates embeddings using Fireworks AI (via openai client),
    and stores the embeddings in a new 'embedding' field in the users collection,
    only if an embedding does not already exist.  Handles 429 errors with a retry.
    """
    client = None
    os.environ['FIREWORKS_API_KEY'] = 'fw_3Ze2AYvUXRmiDNypLqxbB3Nw'
    print(os.environ['FIREWORKS_API_KEY'])
    try:
        # Create a new client and connect to the server
        client = MongoClient(uri)

        # Access the sample_mflix database
        db = client.sample_mflix

        # Access the users collection
        users_collection = db.users

        # Get Fireworks AI API key from environment variable
        fireworks_api_key = os.environ.get("FIREWORKS_API_KEY")
        if not fireworks_api_key:
            raise ValueError("FIREWORKS_API_KEY is not set in the environment variables.")

        # Initialize OpenAI client for Fireworks AI
        openai_client = openai.OpenAI(
            base_url="https://api.fireworks.ai/inference/v1",
            api_key=fireworks_api_key,
        )

        # Iterate through each user in the users collection
        for user in users_collection.find():
            # Check if the user already has an embedding
            if 'embedding' in user:
                print(f"User {user['_id']} already has an embedding, skipping.")
                continue

            # Create a text string from user data.  Include fields relevant to demographics.
            user_text = f"Gender: {user.get('gender', '')}, Age: {user.get('age', '')}, Country: {user.get('country', '')}, State: {user.get('state', '')}"

            # Generate the embedding for the user text using the openai client, with retry logic
            max_retries = 3  # Number of times to retry
            for attempt in range(max_retries):
                try:
                    response = openai_client.embeddings.create(
                        model="nomic-ai/nomic-embed-text-v1.5",
                        input=f"search_document: {user_text}",
                    )
                    embedding = response.data[0].embedding
                    break  # Exit the retry loop if successful
                except openai.APIError as e:
                    if e.status_code == 429:
                        print(f"Rate limit exceeded for user {user['_id']}.  Waiting 60 seconds before retry...")
                        time.sleep(60)
                    else:
                        raise  # Re-raise other errors
                except Exception as e:
                    print(f"Error generating embedding for user {user['_id']}: {e}")
                    break # Exit loop for other errors
            else: # only executed if the loop completes without a break
                print(f"Failed to get embedding for user {user['_id']} after {max_retries} attempts.")
                continue # go to the next user

            # Update the user document to add the embedding
            users_collection.update_one(
                {'_id': user['_id']},
                {'$set': {'embedding': embedding}}
            )
            print(f"Added embedding to user: {user['_id']}")

        print("Finished adding embeddings to users.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the MongoDB client
        if client:
            client.close()

if __name__ == "__main__":
    add_user_embeddings()
