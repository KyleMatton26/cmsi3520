import pandas as pd
from pymongo import MongoClient

df = pd.read_csv('HockeyTeams.csv')

df['OT Losses'] = df['OT Losses'].where(pd.notnull(df['OT Losses']), None)

df.columns = (
    df.columns
    .str.strip()
    .str.replace(' ', '_', regex=True)
    .str.replace('(', '', regex=True)
    .str.replace(')', '', regex=True)
    .str.replace('/', '_', regex=True)
    .str.replace('-', '_', regex=True)
    .str.replace('%', 'Percentage', regex=True) 
)

df.rename(columns={'Win_%': 'Win_Percentage'}, inplace=True)

data = df.to_dict(orient='records')

client = MongoClient('mongodb://localhost:27017/') 
db = client['hockey_db'] 
collection = db['hockey_teams'] 

try:
    collection.insert_many(data, ordered=False)
    print("Data successfully imported into MongoDB!")
except Exception as e:
    print(f"An error occurred during data import: {e}")
