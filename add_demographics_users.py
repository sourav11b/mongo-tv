from pymongo import MongoClient
import random

# MongoDB connection URI
uri = os.getenv('uri')

def add_demographics_to_users():
    """
    Connects to the MongoDB cluster, accesses the sample_mflix database,
    and adds demographic information to each user in the users collection.
    This version ensures all users are USA-based and adds state and zip code.
    """
    client = None
    try:
        # Create a new client and connect to the server
        client = MongoClient(uri)

        # Access the sample_mflix database
        db = client.sample_mflix

        # Access the users collection
        users_collection = db.users

        # Define possible values for demographic fields
        genders = ['Male', 'Female', 'Other']
        ages = list(range(18, 66))  # Ages from 18 to 65
        # states and zip codes
        us_states = {
            "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
            "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
            "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
            "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
            "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
            "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
            "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
            "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
            "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
            "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
            "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
            "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
            "WI": "Wisconsin", "WY": "Wyoming"
        }
        # Iterate through each user in the users collection
        for user in users_collection.find():
            # Generate random demographic data
            state_code = random.choice(list(us_states.keys()))
            demographics = {
                'gender': random.choice(genders),
                'age': random.choice(ages),
                'country': 'USA',  # Ensure USA
                'state': us_states[state_code],
                'zip_code': str(random.randint(10000, 99999))  # Generate a 5-digit zip code
            }

            # Update the user document to add the demographics information
            users_collection.update_one(
                {'_id': user['_id']},
                {'$set': demographics}
            )
            print(f"Updated user: {user['_id']} with demographics: {demographics}") #Added print

        print("Successfully added demographic information to all users.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the MongoDB client
        if client:
            client.close()

if __name__ == "__main__":
    add_demographics_to_users()