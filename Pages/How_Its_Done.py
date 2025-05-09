import pymongo
import streamlit as st
from streamlit_image_select import image_select
from st_clickable_images import clickable_images
from streamlit_carousel import carousel
from pymongo import MongoClient
from bson.objectid import ObjectId  # Import ObjectId
import os
from dotenv import load_dotenv
import random
import requests
from fireworks.client import Fireworks

def do_stuff_on_page_load():
    st.set_page_config(layout="wide")
page_element="""
<style>
[data-testid="stAppViewContainer"]{
background-image: url("https://images.unsplash.com/photo-1638184984605-af1f05249a56?q=80&w=3132&auto=format&fit=crop&ixlib=rb-4.1.0&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D");
background-size: cover;
}
</style>
"""
st.markdown(page_element, unsafe_allow_html=True)

### Get recommended images.
st.logo("images/MongoTV.png")


# Load environment variables from .env file
load_dotenv()


# MongoDB connection URI
uri = "mongodb+srv://sling-admin:password1!@movies-cluster.wgugf5o.mongodb.net/?retryWrites=true&w=majority&appName=movies-cluster"
# Replace with your actual MongoDB URI
def find_similar_users_and_movies(target_user_id):
    """
    Connects to the MongoDB cluster, accesses the sample_mflix database,
    finds similar users and movies using MongoDB's $vectorSearch operator.

    Args:
        target_user_id: The _id of the user to find similar users for.  Assumed to be a string representation of an ObjectId.
    """
    client = None
    try:
        # Create a new client and connect to the server
        client = MongoClient(uri)

        # Access the sample_mflix database
        db = client.sample_mflix

        # Access the users and embedded_movies collections
        users_collection = db.users
        embedded_movies_collection = db.embedded_movies

        # 1. Find the target user's embedding
        # Convert target_user_id to ObjectId
        try:
            target_object_id = ObjectId(target_user_id)
        except Exception:
            print(f"Invalid target_user_id '{target_user_id}'.  Must be a valid ObjectId string.")
            return

        target_user = users_collection.find_one({"_id": target_object_id}, {"embedding": 1,"watch_movies_list" :1})
        if not target_user or "embedding" not in target_user:
            print(f"Target user with _id '{target_user_id}' not found or has no embedding.")
            return
        target_embedding = target_user["embedding"]
        user_watched_movies = target_user["watch_movies_list"]

        # 2. Find similar users using $vectorSearch
        pipeline = [
            {
                "$vectorSearch": {
                    "index" : "user_dem_vector_index",  # Replace with the name of your vector search index on the users collection
                    "path": "embedding",
                    "queryVector": target_embedding,
                    "numCandidates": 185,  # Number of candidate documents to consider
                    "limit": 5 # Number of similar users to return

                }},
                {"$project": {  # Include the fields you want to return
                        "_id": 1,
                        "watch_movies_list": 1,
                        "name" :1,
                        "similarity": {"$meta": "vectorSearchScore"}  # Get the similarity score
                    }
            }
        ]

        similar_users = list(users_collection.aggregate(pipeline))
        if not similar_users:
            print("No similar users found.")
            return

        print(f"Found similar users: {[user['name'] for user in similar_users]}")
        st.write(f"Similar users: {[user['name'] for user in similar_users]}")
        #st.write(similar_user)
        # 3. Collect movies from similar users' watchlists
        all_movie_ids = []
        for similar_user in similar_users:
            if "watch_movies_list" in similar_user:
                all_movie_ids.extend(random.sample(similar_user["watch_movies_list"],5))

        if not all_movie_ids:
            print("No movies found in similar users' watchlists.")
            return

        # Remove duplicate movie IDs
        all_movie_ids = list(set(all_movie_ids))

        # 4. Find similar movies from embedded_movies using $vectorSearch
        if not all_movie_ids:
            print("No movie ids to search")
            return

        similar_movies = []
        for movie_id in all_movie_ids:

            # Find the movie document by _id
            movie_document = embedded_movies_collection.find_one(
                {"_id": ObjectId(movie_id)}, {"plot_embedding": 1}
            )

            if movie_document and "plot_embedding" in movie_document:
                movie_embedding = movie_document["plot_embedding"]
            else:
                print(f"Movie with _id '{movie_id}' not found or has no 'plot_embedding' field.")



            pipeline = [
               {
                "$vectorSearch": {
                    "index": "movies_plot_vector_index",
                    "path": "plot_embedding",  # Changed to plot_embedding
                    "queryVector": movie_embedding,
                    "numCandidates": 50,
                    "limit": 5,

                 }
               },
               {"$project": {
                        "_id": 1,
                        "title": 1,
                        "genres": 1,
                        "poster" : 1,
                        "similarity": {"$meta": "vectorSearchScore"}
                    }
               },
               {"$match" : { "_id" : { "$nin" : user_watched_movies},"similarity" : { "$exists" : "true"},"poster" : { "$exists" : "true"}}},
               {
                  "$sort": {"similarity": -1}
               }

               ]
            similar_movies += list(embedded_movies_collection.aggregate(pipeline))

        # unique_similar_movies = list(set(similar_movies))

        if not similar_movies:
            print("No similar movies found.")
            return

        # Deduplicate movies based on title
        unique_movies = []
        seen_titles = set()
        for movie in similar_movies:
            if movie["title"] not in seen_titles:
                unique_movies.append(movie)
                seen_titles.add(movie["title"])

        if not unique_movies:
            print("No similar movies found.")
            return

        print("Similar movies:")
        for movie in unique_movies:
            print(
                f"  Title: {movie['title']}, Genres: {movie['genres']}, Similarity: {movie['similarity']:.4f}, Poster: {movie['poster']}")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the MongoDB client
        if client:
            client.close()
        return similar_movies

# Run everything on the page
#Show prep image for what we did to prep the database
st.image("images/mongoPrep1.png")
st.image("images/MongoTV-QueryProcessImage.png")
col1, col2, col3 = st.columns(3)
with col1:
     st.write()

with col2:
     st.write()

with col3:
    target_user_id = "59b99db4cfa9a34dcd7885b6"  # Replace with a valid user _id from your database
    recommendedMovies = find_similar_users_and_movies(st.selectbox("Impersonate User",("59b99db4cfa9a34dcd7885b6","59b99db4cfa9a34dcd7885b7","59b99db5cfa9a34dcd7885b8","59b99db5cfa9a34dcd7885b9","59b99db6cfa9a34dcd7885ba")))
# st.write(recommendedMovies)
# Write Slideshow

getUsersCode = """
        target_embedding = target_user["embedding"]
        user_watched_movies = target_user["watch_movies_list"]

        # 2. Find similar users using $vectorSearch
        pipeline = [
            {
                "$vectorSearch": {
                    "index" : "user_dem_vector_index",  # Replace with the name of your vector search index on the users collection
                    "path": "embedding",
                    "queryVector": target_embedding,
                    "numCandidates": 185,  # Number of candidate documents to consider
                    "limit": 5 # Number of similar users to return

                }},
                {"$project": {  # Include the fields you want to return
                        "_id": 1,
                        "watch_movies_list": 1,
                        "name" :1,
                        "similarity": {"$meta": "vectorSearchScore"}  # Get the similarity score
                    }
            }
        ]

"""
st.write("Sample code for getting users like current user")
st.code(getUsersCode,language="python")

getMoviesCode = """
       similar_movies = []
        for movie_id in all_movie_ids:

            # Find the movie document by _id
            movie_document = embedded_movies_collection.find_one(
                {"_id": ObjectId(movie_id)}, {"plot_embedding": 1}
            )

            if movie_document and "plot_embedding" in movie_document:
                movie_embedding = movie_document["plot_embedding"]
            else:
                print(f"Movie with _id '{movie_id}' not found or has no 'plot_embedding' field.")



            pipeline = [
               {
                "$vectorSearch": {
                    "index": "movies_plot_vector_index",
                    "path": "plot_embedding",  # Changed to plot_embedding
                    "queryVector": movie_embedding,
                    "numCandidates": 50,
                    "limit": 5,

                 }
               },
               {"$project": {
                        "_id": 1,
                        "title": 1,
                        "genres": 1,
                        "poster" : 1,
                        "similarity": {"$meta": "vectorSearchScore"}
                    }
               },
               {"$match" : { "_id" : { "$nin" : user_watched_movies},"similarity" : { "$exists" : "true"},"poster" : { "$exists" : "true"}}},
               {
                  "$sort": {"similarity": -1}
               }

               ]
            similar_movies += list(embedded_movies_collection.aggregate(pipeline))

"""
st.write("#### Get Movies Code")
st.code(getMoviesCode, language="python")
st.write("#### Sample Vector Query")
vectorSearchQueryExample = """
                {
                "$vectorSearch": {
                    "index": "movies_plot_vector_index",
                    "path": "plot_embedding",  # Changed to plot_embedding
                    "queryVector": movie_embedding,
                    "numCandidates": 50,
                    "limit": 5,

                 }

"""
st.code(vectorSearchQueryExample, language="python")
st.write("#### Vector Search Output Example")
showExamplePipelineOutput = """
                {"$project": {
                        "_id": 1,
                        "title": 1,
                        "genres": 1,
                        "poster" : 1,
                        "similarity": {"$meta": "vectorSearchScore"}
                    }
               },
"""
st.code(showExamplePipelineOutput, language="python")

st.write("## Recommended Movies")
carousellist = []
for i, movie in enumerate(recommendedMovies):
    if i<5:
        response = requests.get(movie['poster'], stream=True, timeout=10)
        if response.status_code == 200:
            dict(
                title=movie['title'],
                text=movie['title'],
                img=movie['poster'],
                link=movie['poster']
            )
        carousellist.append (dict(title=movie['title'], text=movie['genres'], img=movie['poster'], link=movie['poster']))
# st.write(carousellist)
carousel(carousellist)




#replace the FIREWORKS_API_KEY with the key copied in the above step.

def getFunnyTitle(input_string):
    client = Fireworks(api_key='fw_3Ze2AYvUXRmiDNypLqxbB3Nw')
    model_name = "accounts/fireworks/models/llama-v3p1-405b-instruct"
    response = client.chat.completions.create(
        model=model_name,
        messages=[{
            "role": "user",
            "content": "Generate a funny and witty title for a section of an online video on demand website that is classified as " + input_string + ". Give me only the title that is less than 15 words long, without any other text.",
        }],
    )
    return response.choices[0].message.content


st.write(getFunnyTitle("horror"))

# show additional movies that someone might like based on their profile and other profiles like theirs
st.write("#### Feeling Adventurous? More Movies You Might Like")
st.write("Movies that have a lower similarity (top 50 but not top 10)")
images = list()
for i, movie in enumerate(recommendedMovies):
        if i<50 and i>30:
            response = requests.get(movie['poster'], stream=True, timeout=10)
            if response.status_code == 200:
                images.append(movie['poster'])
#st.write(images)
clicked = clickable_images(
    images,
    titles=[f"Image #{str(i)}" for i in range(5)],
    div_style={"display": "flex", "justify-content": "center", "flex-wrap": "wrap"},
    img_style={"margin": "5px", "height": "200px"},
)
#st.markdown(f"Image #{clicked} clicked" if clicked > -1 else "No image clicked")        
#imgSel = image_select("Additional Recommended Titles", images)


# Start building a graph of people like me
st.write("## More About People Like Me")
st.write("Example of the distribution of Genres shown in similarity. Utilized to build dyanmic categories")
largeGenreList = []
for i, movie in enumerate(recommendedMovies):
        # Check if 'Title' and 'Genres' keys exist to avoid errors
        title = movie.get("Title", f"Unknown Movie {i+1}")
        genres = movie.get("genres")
        if genres is not None and isinstance(genres, list):
            if genres:
                for genre in genres:
                    largeGenreList.append(genre)
                    # st.write(f"- {genre}")

#set up bar chart data
uniqueGenre = list(set(largeGenreList))
genreData = []
for item in uniqueGenre:
    addedGenre = {}
    addedGenre['genre'] = item
    addedGenre['amount'] = largeGenreList.count(item)
    #st.write(addedGenre)
    genreData.append(addedGenre)
#st.write(genreData)
sortedGenres = sorted(genreData, key=lambda item: item["amount"], reverse=True)
st.bar_chart(data=sortedGenres, x="genre", y="amount")

# Set Section 1
st.write("### " + getFunnyTitle(sortedGenres[0]["genre"]))
st.write("More " + sortedGenres[0]["genre"] + " movies we think you'll Like")
# for i, movie in enumerate(recommendedMovies):
movies_compile1 = [
    movie for movie in recommendedMovies
    if 'genres' in movie and sortedGenres[0]["genre"] in movie['genres']
]
#st.write(movies_compile1)
movieCompileImages1 = list()
for i, movie in enumerate(movies_compile1):
        if i<10:
            response = requests.get(movie['poster'], stream=True, timeout=10)
            if response.status_code == 200:
                movieCompileImages1.append(movie['poster'])
#st.write(images)
clickedMovie1 = clickable_images(
    movieCompileImages1,
    titles=[f"Image #{str(i)}" for i in range(5)],
    div_style={"display": "flex", "justify-content": "center", "flex-wrap": "wrap"},
    img_style={"margin": "5px", "height": "200px"},
)

# Set Section 2
st.write("### " + getFunnyTitle(sortedGenres[1]["genre"]))
st.write("More " + sortedGenres[1]["genre"] + " movies we think you'll Like")
movies_compile2 = [
    movie for movie in recommendedMovies
    if 'genres' in movie and sortedGenres[1]["genre"] in movie['genres']
]

movieCompileImages2 = list()
for i, movie in enumerate(movies_compile2):
        if i<10:
            response = requests.get(movie['poster'], stream=True, timeout=10)
            if response.status_code == 200:
                movieCompileImages2.append(movie['poster'])
#st.write(images)
clickedMovie2 = clickable_images(
    movieCompileImages2,
    titles=[f"Image #{str(i)}" for i in range(5)],
    div_style={"display": "flex", "justify-content": "center", "flex-wrap": "wrap"},
    img_style={"margin": "5px", "height": "200px"},
)


# Set Section 4
st.write("### " + getFunnyTitle(sortedGenres[-8]["genre"]))
st.write("Automatically builds this provile getting the 7th to last genre in the listings")
movies_compile4 = [
    movie for movie in recommendedMovies
    if 'genres' in movie and sortedGenres[-7]["genre"] in movie['genres']
]
movieCompileImages4 = list()
for i, movie in enumerate(movies_compile4):
        if i<10:
            response = requests.get(movie['poster'], stream=True, timeout=10)
            if response.status_code == 200:
                movieCompileImages4.append(movie['poster'])
#st.write(images)
clickedMovie4 = clickable_images(
    movieCompileImages4,
    titles=[f"Image #{str(i)}" for i in range(5)],
    div_style={"display": "flex", "justify-content": "center", "flex-wrap": "wrap"},
    img_style={"margin": "5px", "height": "200px"},
)