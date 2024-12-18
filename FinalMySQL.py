import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Text, Boolean
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.engine.url import URL
from pathlib import Path
from datetime import datetime
import Config2

DB_CONFIG = {
    'drivername': 'mysql+pymysql',
    'username': Config2.MYSQL_USER,       
    'password': Config2.MYSQL_PASSWORD,  
    'host': Config2.MYSQL_HOST,           
    'port': Config2.MYSQL_PORT,          
    'database': Config2.MYSQL_DATABASE   
}

def create_database_if_not_exists():
    try:
        db_config_no_db = DB_CONFIG.copy()
        db_config_no_db.pop('database')
        url_no_db = URL.create(**db_config_no_db)
        engine_no_db = create_engine(url_no_db)
        with engine_no_db.connect() as connection:
            connection.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        print(f"Database '{DB_CONFIG['database']}' is ready.")
    except SQLAlchemyError as e:
        print(f"Error creating database '{DB_CONFIG['database']}': {e}")
        raise

def get_engine():
    try:
        engine = create_engine(URL.create(**DB_CONFIG), echo=False)
        with engine.connect() as connection:
            pass
        print(f"Successfully connected to database '{DB_CONFIG['database']}'.")
        return engine
    except SQLAlchemyError as e:
        print(f"Error connecting to database '{DB_CONFIG['database']}': {e}")
        raise

metadata = MetaData()

team_info = Table(
    "team_info", metadata,
    Column("team_id", Integer, primary_key=True),
    Column("franchiseId", Integer),
    Column("shortName", String(50)),
    Column("teamName", String(100)),
    Column("Abbreviation", String(10)),
    Column("link", String(255)),
    Column("date_added", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')),
    Column("date_updated", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
)

player_info = Table(
    "player_info", metadata,
    Column("player_id", String(50), primary_key=True),
    Column("firstName", String(100)),
    Column("lastName", String(100)),
    Column("nationality", String(50)),
    Column("birthCity", String(100)),
    Column("primaryPosition", String(50)),
    Column("birthDate", String(30)),               
    Column("birthStateProvince", String(50)),      
    Column("height", String(10)),
    Column("height_cm", Float),
    Column("weight", Float),                        
    Column("shootsCatches", String(10)),           
    Column("date_added", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')),
    Column("date_updated", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
)

game = Table(
    "game", metadata,
    Column("game_id", String(50), primary_key=True),
    Column("season", String(50)),
    Column("type", String(50)),
    Column("date_time", String(30)),               
    Column("date_time_GMT", String(30)),           
    Column("away_team_id", Integer),
    Column("home_team_id", Integer),
    Column("away_goals", Integer),
    Column("home_goals", Integer),
    Column("outcome", String(50)),
    Column("home_rink_side_start", String(10)),
    Column("venue", String(100)),
    Column("venue_link", String(255)),
    Column("venue_time_zone_id", String(50)),
    Column("venue_time_zone_offset", String(10)),
    Column("venue_time_zone_tz", String(50)),
    Column("date_added", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')),
    Column("date_updated", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
)

game_plays = Table(
    "game_plays", metadata,
    Column("play_id", String(50), primary_key=True),  
    Column("game_id", String(50)),
    Column("team_id_for", Integer),
    Column("team_id_against", Integer),
    Column("event", String(255)),
    Column("secondaryType", String(100)),
    Column("X", Float),
    Column("Y", Float),
    Column("Period", Integer),
    Column("PeriodType", String(50)),
    Column("PeriodTime", Integer),                  
    Column("PeriodTimeRemaining", Float),         
    Column("dateTime", String(30)),                
    Column("Goals_away", Integer),
    Column("Goals_home", Integer),
    Column("Description", Text),
    Column("St_x", Float),
    Column("St_y", Float),
    Column("date_added", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')),
    Column("date_updated", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
)

game_teams_stats = Table(
    "game_teams_stats", metadata,
    Column("game_id", String(50), primary_key=True),    
    Column("team_id", Integer, primary_key=True),
    Column("HoA", String(10)),
    Column("Won", Boolean),
    Column("Settled_in", String(10)),  
    Column("head_coach", String(100)),
    Column("Goals", Integer),
    Column("Shots", Integer),
    Column("Hits", Integer),
    Column("Pim", Integer),
    Column("powerPlayOpportunities", Integer),
    Column("powerPlayGoals", Integer),
    Column("faceOffWinPercentage", Float),
    Column("Giveaways", Integer),
    Column("Takeaways", Integer),
    Column("Blocked", Integer),
    Column("startRinkSide", String(10)),
    Column("date_added", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')),
    Column("date_updated", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
)

game_officials = Table(
    "game_officials", metadata,
    Column("game_id", String(50), primary_key=True),   
    Column("official_name", String(100), primary_key=True),
    Column("official_type", String(50)),
    Column("date_added", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')),
    Column("date_updated", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
)

game_goals = Table(
    "game_goals", metadata,
    Column("play_id", String(50), primary_key=True),  
    Column("Strength", String(50)),
    Column("gameWinningGoal", Boolean),
    Column("emptyNet", Boolean),
    Column("date_added", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')),
    Column("date_updated", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
)

game_penalties = Table(
    "game_penalties", metadata,
    Column("play_id", String(50), primary_key=True), 
    Column("penaltySeverity", String(50)),
    Column("penaltyMinutes", Integer),
    Column("date_added", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')),
    Column("date_updated", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
)

game_scratches = Table(
    "game_scratches", metadata,
    Column("game_id", String(50), primary_key=True),  
    Column("team_id", Integer, primary_key=True),
    Column("player_id", String(50), primary_key=True),
    Column("date_added", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')),
    Column("date_updated", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
)

game_plays_players = Table(
    "game_plays_players", metadata,
    Column("play_id", String(50), primary_key=True),   
    Column("game_id", String(50), primary_key=True),   
    Column("player_id", String(50), primary_key=True), 
    Column("playerType", String(50)),
    Column("date_added", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')),
    Column("date_updated", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
)

game_shifts = Table(
    "game_shifts", metadata,
    Column("game_id", String(50), primary_key=True),    
    Column("player_id", String(50), primary_key=True),  
    Column("Period", Integer, primary_key=True),
    Column("Shift_start", String(10), primary_key=True),
    Column("Shift_end", String(10)),
    Column("date_added", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')),
    Column("date_updated", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
)

game_skater_stats = Table(
    "game_skater_stats", metadata,
    Column("game_id", String(50), primary_key=True),    
    Column("player_id", String(50), primary_key=True),  
    Column("team_id", Integer),
    Column("timeOnIce", String(10)),
    Column("Assists", Integer),
    Column("Goals", Integer),
    Column("Shots", Integer),
    Column("Hits", Integer),
    Column("powerPlayGoals", Integer),
    Column("powerPlayAssists", Integer),
    Column("penaltyMinutes", Integer),
    Column("faceOffwins", Integer),
    Column("faceoffTaken", Integer),
    Column("Takeaways", Integer),
    Column("Giveaways", Integer),
    Column("shortHandedGoals", Integer),
    Column("shortHandedAssists", Integer),
    Column("Blocked", Integer),
    Column("plusMinus", Integer),
    Column("evenTimeOnIce", String(10)),
    Column("shortHandedTimeOnIce", String(10)),
    Column("powerPlayTimeOnIce", String(10)),
    Column("date_added", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')),
    Column("date_updated", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
)

# 13. game_goalie_stats (Child table)
game_goalie_stats = Table(
    "game_goalie_stats", metadata,
    Column("game_id", String(50), primary_key=True),    
    Column("player_id", String(50), primary_key=True),  
    Column("team_id", Integer),
    Column("timeOnIce", String(10)),
    Column("Assists", Integer),
    Column("Goals", Integer),
    Column("pim", Integer),
    Column("shots", Integer),
    Column("saves", Integer),
    Column("powerPlaySaves", Integer),
    Column("shortHandedSaves", Integer),
    Column("evenSaves", Integer),
    Column("shortHandedShotsAgainst", Integer),
    Column("evenShotsAgainst", Integer),
    Column("powerPlayShotsAgainst", Integer),
    Column("decision", String(50)),
    Column("savePercentage", Float),
    Column("powerPlaySavePercentage", Float),
    Column("evenStrengthSavePercentage", Float),
    Column("date_added", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')),
    Column("date_updated", String(30), default=lambda: datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
)

def create_tables(engine, metadata):
    try:
        metadata.drop_all(engine)  
        metadata.create_all(engine)  
        print("All tables created successfully.")
    except SQLAlchemyError as e:
        print(f"Error creating tables: {e}")
        raise


def import_csv_to_table(csv_path, table, engine):
    try:
        if not csv_path.exists():
            print(f"CSV file for table '{table.name}' not found at path: {csv_path}")
            return

        df = pd.read_csv(csv_path)

        if 'brithStateProvince' in df.columns:
            df.rename(columns={'brithStateProvince': 'birthStateProvince'}, inplace=True)
            print(f"Renamed column 'brithStateProvince' to 'birthStateProvince' in table '{table.name}'.")

        df = df.where(pd.notnull(df), None)

        primary_keys = [col.name for col in table.primary_key.columns]

        if primary_keys:
            duplicate_count = df.duplicated(subset=primary_keys, keep='first').sum()
            if duplicate_count > 0:
                print(f"Found {duplicate_count} duplicate rows in table '{table.name}'. Dropping duplicates.")
                df = df.drop_duplicates(subset=primary_keys, keep='first')

        with engine.connect() as connection:
            trans = connection.begin()
            try:
                df.to_sql(table.name, con=connection, if_exists="append", index=False)
                trans.commit()
                print(f"Imported {len(df)} rows into table '{table.name}'.")
            except IntegrityError as ie:
                trans.rollback()
                print(f"Integrity Error inserting data into table '{table.name}': {ie}")
            except SQLAlchemyError as e:
                trans.rollback()
                print(f"SQLAlchemy Error inserting data into table '{table.name}': {e}")
            except Exception as e:
                trans.rollback()
                print(f"Unexpected error inserting data into table '{table.name}': {e}")
    except FileNotFoundError:
        print(f"CSV file for table '{table.name}' not found at path: {csv_path}")
    except SQLAlchemyError as e:
        print(f"SQLAlchemy Error processing table '{table.name}': {e}")
    except Exception as e:
        print(f"Unexpected error processing table '{table.name}': {e}")

def main():
    create_database_if_not_exists()

    engine = get_engine()

    create_tables(engine, metadata)

    import_order = [
        "team_info",
        "player_info",
        "game",
        "game_plays",
        "game_teams_stats",
        "game_officials",
        "game_goals",
        "game_penalties",
        "game_scratches",
        "game_plays_players",
        "game_shifts",
        "game_skater_stats",
        "game_goalie_stats"
    ]

    data_folder = Path(__file__).parent / "NHLGameData"

    for table_name in import_order:
        csv_file = data_folder / f"{table_name}.csv" 
        if csv_file.exists():
            table = metadata.tables[table_name]
            print(f"\nStarting import for table '{table_name}' from '{csv_file}'.")
            import_csv_to_table(csv_file, table, engine)
        else:
            print(f"\nCSV file for table '{table_name}' does not exist at path: {csv_file}")

    print("\nAll tables attempted to be imported into MySQL.")

if __name__ == "__main__":
    main()
