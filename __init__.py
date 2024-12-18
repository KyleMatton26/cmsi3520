from flask import Flask, request, jsonify
from flask_pydantic import validate
from model import HockeyTeam
from objectid import PydanticObjectId
from pymongo.errors import DuplicateKeyError
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
from dotenv import load_dotenv
import os
from pydantic import ValidationError
from pathlib import Path

env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

app = Flask(__name__)

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("No MongoDB connection string found in environment variables.")

client = MongoClient(MONGO_URI)
db = client["hockey_db"]
collection = db["hockey_teams"]

duplicates = db.hockey_teams.aggregate([
    {"$group": {
        "_id": {"Team_Name": "$Team_Name", "Year": "$Year"},
        "count": {"$sum": 1}
    }},
    {"$match": {"count": {"$gt": 1}}}
])

for dup in duplicates:
    team_name = dup["_id"]["Team_Name"]
    year = dup["_id"]["Year"]
    docs = list(collection.find({"Team_Name": team_name, "Year": year}))
    if len(docs) > 1:
        doc_to_keep = docs[0]["_id"]
        ids_to_delete = [d["_id"] for d in docs if d["_id"] != doc_to_keep]
        if ids_to_delete:
            collection.delete_many({"_id": {"$in": ids_to_delete}})

collection.create_index([("Team_Name", 1), ("Year", 1)], unique=True)

@app.errorhandler(404)
def resource_not_found(e):
    return jsonify(error=str(e)), 404

@app.errorhandler(DuplicateKeyError)
def handle_duplicate_key_error(e):
    return jsonify(error="Team already exists."), 409

@app.errorhandler(ValidationError)
def handle_validation_error(e):
    return jsonify(error=e.errors()), 400

@app.route('/insert', methods=['POST'])
@validate()
def insert_team(body: HockeyTeam):
    team_dict = body.dict(by_alias=True)
    team_dict['date_added'] = datetime.utcnow()
    team_dict['date_updated'] = datetime.utcnow()
    try:
        result = collection.insert_one(team_dict)
        team_dict['_id'] = str(result.inserted_id)
        return jsonify(HockeyTeam(**team_dict).dict()), 201
    except DuplicateKeyError:
        return jsonify({"error": "Team already exists."}), 409
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/delete', methods=['DELETE'])
def delete_team():
    team_name = request.args.get('team_name')
    year = request.args.get('year', type=int)

    if not team_name or not year:
        return jsonify({"error": "Please provide both 'team_name' and 'year' as query parameters."}), 400

    try:
        result = collection.delete_one({"Team_Name": team_name, "Year": year})
        if result.deleted_count == 1:
            return jsonify({"message": "Team deleted successfully."}), 200
        else:
            return jsonify({"message": "No matching team found."}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/search', methods=['GET'])
def search_teams():
    team_name = request.args.get('team_name')
    year = request.args.get('year', type=int)

    query = {}
    if team_name:
        query['Team_Name'] = team_name
    if year is not None:
        query['Year'] = year

    try:
        results = list(collection.find(query, {"_id": 1, "Team_Name": 1, "Year": 1, "Wins": 1, "Losses": 1, "OT_Losses": 1, "Win_Percentage": 1, "Goals_For_GF": 1, "Goals_Against_GA": 1, "+____": 1, "date_added": 1, "date_updated": 1}))
        
        for doc in results:
            doc["_id"] = str(doc["_id"])
            if "Win_Percentage" not in doc:
                doc["Win_Percentage"] = 0.0 

        validated_results = [HockeyTeam(**doc).dict() for doc in results]
        return jsonify(validated_results), 200
    except ValidationError as e:
        return jsonify({"error": e.errors()}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/teams', methods=['GET'])
def get_teams():
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 10))
    except ValueError:
        return jsonify({"error": "'page' and 'per_page' must be integers."}), 400

    if page < 1 or per_page < 1:
        return jsonify({"error": "'page' and 'per_page' must be positive integers."}), 400

    skips = per_page * (page - 1)
    cursor = collection.find().sort("Team_Name").skip(skips).limit(per_page)
    team_count = collection.count_documents({})

    links = {
        "self": {
            "href": request.url_root.rstrip('/') + f"/teams?page={page}&per_page={per_page}"
        },
        "last": {
            "href": request.url_root.rstrip('/') + f"/teams?page={(team_count // per_page) + 1}&per_page={per_page}"
        }
    }

    if page > 1:
        links["prev"] = {
            "href": request.url_root.rstrip('/') + f"/teams?page={page - 1}&per_page={per_page}"
        }

    if page < (team_count // per_page) + 1:
        links["next"] = {
            "href": request.url_root.rstrip('/') + f"/teams?page={page + 1}&per_page={per_page}"
        }

    try:
        results = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            if "Win_Percentage" not in doc:
                doc["Win_Percentage"] = 0.0
            results.append(HockeyTeam(**doc).dict())

        return jsonify({
            "recipes": results,
            "_links": links
        }), 200
    except ValidationError as e:
        return jsonify({"error": e.errors()}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
