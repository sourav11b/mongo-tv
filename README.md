1. pip install -r .\requirements.txt

2.  On the file `.env` in root folder put the following two lines with proper values 

```
uri="<<MONGODB_URI>>"
FIREWORK_API_KEY='<<FIREWORK_API_KEY>>'
```
This demo is based on [Sample Mflix Dataset](https://www.mongodb.com/docs/atlas/sample-data/sample-mflix/) 

3. create watched movies list for users - `add_movies_users.py`
4. add demographic information to users - `add_demographics_users.py`
5. create demographics based vector embeddings for users - `create_user_dem_vectors.py`
6. Run the application - ` python -m streamlit run .\Home.py`