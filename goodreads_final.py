from flask import Flask, request, jsonify, send_from_directory, Response
import pandas as pd
import threading
from time import sleep
import os
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
import json
import numpy as np
from pymongo import MongoClient
from bson import ObjectId, errors

# MongoDB setup
def get_mongo_uri():
    """Loads and returns the MongoDB URI from the .env file."""
    load_dotenv(".env")  # Load environment variables from .env
    mongo_uri = os.getenv("MONGO_URI")  # Retrieve the URI

    if not mongo_uri:
        raise ValueError("MONGO_URI not found in .env file!")

    return mongo_uri  # Return the URI as a string

def place_book_in_mongo(books_df, mongo_uri=None, db_name="test", collection_name="goodreads_books"):
    """Inserts a Pandas DataFrame (single or multiple rows) into MongoDB."""
    if not isinstance(books_df, pd.DataFrame):
        raise ValueError("Expected a Pandas DataFrame as input")

    # Ensure fields for text index are strings
    books_df['authors'] = books_df['authors'].apply(lambda x: [str(author) for author in x] if isinstance(x, list) else [])
    books_df['publisher'] = books_df['publisher'].astype(str)
    books_df['title'] = books_df['title'].astype(str)

    # Additional debugging: Print data types
    print(books_df.dtypes)

    books_list = books_df.to_dict(orient="records")
    mongo_uri = mongo_uri or get_mongo_uri()
    with MongoClient(mongo_uri) as client:
        db = client[db_name]
        collection = db[collection_name]

        # Drop existing indexes before creating a new one
        collection.drop_indexes()

        result = collection.insert_many(books_list)
        print(f":white_check_mark: {len(result.inserted_ids)} book(s) added.")

        # Create text index
        collection.create_index([
            ("authors", "text"),
            ("publisher", "text"),
            ("title", "text")
        ])

def old_place_book_in_mongo(books_df, mongo_uri=None, db_name="test", collection_name="goodreads_books"):
    """Inserts a Pandas DataFrame (single or multiple rows) into MongoDB."""
    if not isinstance(books_df, pd.DataFrame):
        raise ValueError("Expected a Pandas DataFrame as input")

    books_list = books_df.to_dict(orient="records")
    mongo_uri = mongo_uri or get_mongo_uri()

    with MongoClient(mongo_uri) as client:
        db = client[db_name]
        collection = db[collection_name]
        result = collection.insert_many(books_list)
        print(f":white_check_mark: {len(result.inserted_ids)} book(s) added.")

        collection.create_index([
            ("authors", "text"),
            ("publisher", "text"),
            ("title", "text")
        ])

def scrape_books_by_genre(genre):
    url = f"https://www.goodreads.com/shelf/show/{genre}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return {"error": f"Failed to fetch data for genre {genre}. Status code: {response.status_code}"}
    soup = BeautifulSoup(response.text, "html.parser")
    books = []
    for book in soup.select(".bookTitle"):
        title = book.get_text(strip=True)
        link = "https://www.goodreads.com" + book['href']
        book_details = fetch_book_details(link, genre)
        books.append(book_details)
    return books

def fetch_book_details(book_url, genre):
    response = requests.get(book_url)
    if response.status_code != 200:
        return {"error": f"Failed to fetch book details. Status code: {response.status_code}"}
    soup = BeautifulSoup(response.text, "html.parser")
    script_tag = soup.find("script", type="application/ld+json")
    if script_tag:
        json_data = json.loads(script_tag.string)
        book_details = {
            "title": json_data.get("name", "N/A"),
            "authors": [author["name"] for author in json_data.get("author", [])],
            "publisher": json_data.get("publisher", "N/A"),
            "page_count": json_data.get("numberOfPages", "N/A"),
            "language": json_data.get("inLanguage", "N/A"),
            "category": json_data.get("genre", genre),
            "thumbnail": json_data.get("image", "N/A"),
            "isbn": json_data.get("isbn", "N/A"),
            "link": book_url
        }
        return book_details
    else:
        return {"error": f"Failed to find script tag with JSON data for {book_url}"}

# Function to import data from MongoDB
def import_from_mongo(mongo_uri):
    client = MongoClient(mongo_uri)
    db = client["test"]
    collection = db["goodreads_books"]
    data = list(collection.find())
    df = pd.DataFrame(data)
    return df

# Function to remove a book from MongoDB by ID
def remove_selection_from_mongo(mongo_ID, mongo_uri, db_name, collection_name):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    result = collection.delete_one({"_id": ObjectId(mongo_ID)})
    return result.deleted_count > 0

def check_correct_mongo_ID(mongo_ID):
    if not mongo_ID:
        return None
    try:
        return ObjectId(mongo_ID)  # Convert safely
    except errors.InvalidId:
        return None

# Load environment variables from .env file
#load_dotenv(".env")

app = Flask(__name__)

@app.route('/')
def home():
    return "Welcome to the Book API! Use /scrape_books, /get_selected_books, /remove_by_ID."

@app.route('/favicon.ico')
def favicon():
    return send_from_directory('static', 'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/scrape_books', methods=['POST'])
def scrape_books():
    try:
        genres = request.json["genres"]
    except KeyError:
        return jsonify({"error": "Genres are required in the input JSON"}), 400
    if not genres:
        return jsonify({"error": "The genres list cannot be empty"}), 400
    message = {"message": "Books are being fetched. Please wait..."}

    def fetch_and_store_books():
        all_books = []
        for genre in genres:
            print(f"Fetching books for genre: {genre}")
            books = scrape_books_by_genre(genre)
            if "error" not in books:
                all_books.extend(books)
            else:
                print(f"Error fetching books for {genre}: {books['error']}")
            sleep(2)
        if all_books:
            books_df = pd.DataFrame(all_books)
            place_book_in_mongo(books_df)

    threading.Thread(target=fetch_and_store_books, daemon=True).start()
    return jsonify(message), 202

@app.route('/get_selected_books', methods=['GET'])
def get_selected_books():
    """Fetch all selected books from MongoDB and return as JSON."""
    mongo_uri = get_mongo_uri()
    df = import_from_mongo(mongo_uri)  # Fetch from MongoDB

    # Convert ObjectId to string
    if '_id' in df.columns:
        df['_id'] = df['_id'].apply(lambda x: str(x))

    return jsonify(df.to_dict(orient="records"))

@app.route('/remove_by_ID', methods=['POST'])
def remove_by_ID():
    data = request.get_json()
    mongo_ID = data.get("_id")
    mongo_ID = check_correct_mongo_ID(mongo_ID)
    if mongo_ID is None:
        return jsonify({"message": "Wrong ID, nothing happened!"})
    mongo_uri = get_mongo_uri()
    book_removed = remove_selection_from_mongo(mongo_ID, mongo_uri, db_name="test", collection_name="goodreads_books")
    if book_removed:
        return jsonify({"message": "Book removed"})
    return jsonify({"message": "Wrong ID, nothing happened!"})

if __name__ == '__main__':
    app.run(debug=True, threaded=True)
