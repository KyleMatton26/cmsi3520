import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData, Enum, BigInteger, SmallInteger
from sqlalchemy.dialects.mysql import YEAR
import pymysql
import Config

MYSQL_USER = Config.MYSQL_USER
MYSQL_PASSWORD = Config.MYSQL_PASSWORD
MYSQL_HOST = Config.MYSQL_HOST
MYSQL_PORT = Config.MYSQL_PORT
MYSQL_DB = Config.MYSQL_DATABASE

connection_string = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}'
engine = create_engine(connection_string)
metadata = MetaData()

popular_spotify_songs = Table(
    'popular_spotify_songs',
    metadata,
    Column('track_name', String(255)),
    Column('artists_name', String(255)),
    Column('artist_count', Integer),
    Column('released_year', YEAR),
    Column('released_month', SmallInteger),
    Column('released_day', SmallInteger),
    Column('in_spotify_playlists', Integer),
    Column('in_spotify_charts', Integer),
    Column('streams', BigInteger),
    Column('in_apple_playlists', Integer),
    Column('in_apple_charts', Integer),
    Column('in_deezer_playlists', Integer),
    Column('in_deezer_charts', Integer),
    Column('in_shazam_charts', BigInteger),
    Column('bpm', Float),
    Column('key', String(10)),
    Column('mode', Enum('Major', 'Minor')),
    Column('danceability', Float),
    Column('valence', Float),
    Column('energy', Float),
    Column('acousticness', Float),
    Column('instrumentalness', Float),
    Column('liveness', Float),
    Column('speechiness', Float)
)

spotify_dataset = Table(
    'spotify_dataset',
    metadata,
    Column('user_id', String(255)),
    Column('artistname', String(255)),
    Column('trackname', String(255)),
    Column('playlistname', String(255))
)

metadata.drop_all(engine, tables=[popular_spotify_songs, spotify_dataset])
metadata.create_all(engine)

popular_songs_df = pd.read_csv('Popular_Spotify_Songs.csv', encoding='latin1')
popular_songs_df.columns = popular_songs_df.columns.str.replace(r'[()\%]', '', regex=True).str.replace(' ', '_').str.lower()

popular_songs_df.rename(columns={
    'artist(s)_name': 'artists_name',
    'danceability_': 'danceability',
    'valence_': 'valence',
    'energy_': 'energy',
    'acousticness_': 'acousticness',
    'instrumentalness_': 'instrumentalness',
    'liveness_': 'liveness',
    'speechiness_': 'speechiness'
}, inplace=True)

numeric_columns = ['streams', 'in_deezer_playlists', 'in_shazam_charts']

for col in numeric_columns:
    popular_songs_df[col] = pd.to_numeric(popular_songs_df[col], errors='coerce')

popular_songs_df[numeric_columns] = popular_songs_df[numeric_columns].fillna(0).astype(int)
popular_songs_df.to_sql('popular_spotify_songs', con=engine, if_exists='append', index=False)

spotify_dataset_df = pd.read_csv('spotify_dataset.csv', encoding='latin1', on_bad_lines='skip')

spotify_dataset_df.columns = spotify_dataset_df.columns.str.replace(r'[\"\s]', '', regex=True).str.lower()
spotify_dataset_df.replace({'': None, 'N/A': None}, inplace=True)

spotify_dataset_df['playlistname'] = spotify_dataset_df['playlistname'].astype(str).str.slice(0, 255)
spotify_dataset_df['trackname'] = spotify_dataset_df['trackname'].astype(str).str.slice(0, 255)

spotify_dataset_df.to_sql('spotify_dataset', con=engine, if_exists='append', index=False)

print("Data import process completed.")

query_top_streams = """
SELECT track_name, artists_name, streams
FROM popular_spotify_songs
ORDER BY streams DESC
LIMIT 10;
"""

query_tracks_per_artist = """
SELECT artists_name, COUNT(track_name) AS number_of_tracks
FROM popular_spotify_songs
GROUP BY artists_name
ORDER BY number_of_tracks DESC;
"""

query_join = """
SELECT sd.user_id, sd.artistname, sd.trackname, sd.playlistname, pss.streams
FROM spotify_dataset sd
JOIN popular_spotify_songs pss
ON LOWER(TRIM(sd.trackname)) = LOWER(TRIM(pss.track_name))
   AND LOWER(TRIM(sd.artistname)) = LOWER(TRIM(pss.artists_name));
"""

top_streams_df = pd.read_sql(query_top_streams, con=engine)
print("\nTop 10 Tracks by Streams:")
print(top_streams_df)

tracks_per_artist_df = pd.read_sql(query_tracks_per_artist, con=engine)
print("\nNumber of Tracks per Artist:")
print(tracks_per_artist_df)

joined_df = pd.read_sql(query_join, con=engine)
print("\nUser Playlists with Popular Song Streams:")
print(joined_df)
