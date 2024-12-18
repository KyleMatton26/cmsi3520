import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError
import Config2


DB_CONFIG = {
    'drivername': 'mysql+pymysql',
    'username': Config2.MYSQL_USER,       
    'password': Config2.MYSQL_PASSWORD,   
    'host': Config2.MYSQL_HOST,           
    'port': Config2.MYSQL_PORT,           
    'database': Config2.MYSQL_DATABASE    
}


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

def execute_and_display_query(engine, query, description):
    try:
        df = pd.read_sql_query(query, con=engine)
        print(f"\n--- {description} ---")
        print(df.head())
    except SQLAlchemyError as e:
        print(f"Error executing query '{description}': {e}")

def create_view(engine, view_name, view_query):
    try:
        with engine.connect() as connection:
            connection.execute(f"DROP VIEW IF EXISTS {view_name}")
            connection.execute(f"CREATE VIEW {view_name} AS {view_query}")
        print(f"View '{view_name}' created successfully.")
    except SQLAlchemyError as e:
        print(f"Error creating view '{view_name}': {e}")

def query_view(engine, view_name, description):
    try:
        df = pd.read_sql_query(f"SELECT * FROM {view_name}", con=engine)
        print(f"\n--- {description} (View: {view_name}) ---")
        print(df.head())
    except SQLAlchemyError as e:
        print(f"Error querying view '{view_name}': {e}")

def main():
    engine = get_engine()
    
    simple_query1 = """
        SELECT player_id, firstName, lastName, nationality, primaryPosition
        FROM player_info
        LIMIT 10;
    """
    execute_and_display_query(engine, simple_query1, "Simple Query 1: List of Players")

    simple_query2 = """
        SELECT team_id, teamName, Abbreviation, shortName
        FROM team_info
        LIMIT 10;
    """
    execute_and_display_query(engine, simple_query2, "Simple Query 2: List of Teams")
    
    complex_query = """
        SELECT 
            p.player_id, 
            p.firstName, 
            p.lastName, 
            SUM(gs.Goals) AS total_goals, 
            SUM(gs.Assists) AS total_assists
        FROM 
            player_info p
        JOIN 
            game_skater_stats gs ON p.player_id = gs.player_id
        GROUP BY 
            p.player_id, p.firstName, p.lastName
        ORDER BY 
            total_goals DESC
        LIMIT 10;
    """
    execute_and_display_query(engine, complex_query, "Complex Query: Top 10 Players by Goals")
    
    player_career_stats_query = """
        SELECT 
            p.player_id, 
            p.firstName, 
            p.lastName, 
            SUM(gs.Goals) AS total_goals, 
            SUM(gs.Assists) AS total_assists,
            SUM(gs.Shots) AS total_shots,
            SUM(gs.Hits) AS total_hits
        FROM 
            player_info p
        JOIN 
            game_skater_stats gs ON p.player_id = gs.player_id
        GROUP BY 
            p.player_id, p.firstName, p.lastName
    """
    create_view(engine, "player_career_stats", player_career_stats_query)
    
    team_performance_stats_query = """
        SELECT 
            t.team_id, 
            t.teamName, 
            COUNT(g.game_id) AS total_games,
            SUM(gt.Won) AS total_wins,
            SUM(gt.Goals) AS total_goals,
            AVG(gt.faceOffWinPercentage) AS avg_face_off_win_percentage
        FROM 
            team_info t
        JOIN 
            game_teams_stats gt ON t.team_id = gt.team_id
        JOIN 
            game g ON gt.game_id = g.game_id
        GROUP BY 
            t.team_id, t.teamName
    """
    create_view(engine, "team_performance_stats", team_performance_stats_query)

    top_goalies_query = """
        SELECT 
            p.player_id, 
            p.firstName, 
            p.lastName, 
            gg.savePercentage,
            gg.saves
        FROM 
            player_info p
        JOIN 
            game_goalie_stats gg ON p.player_id = gg.player_id
        WHERE 
            gg.savePercentage IS NOT NULL
        ORDER BY 
            gg.savePercentage DESC
        """
    create_view(engine, "top_goalies", top_goalies_query)

    player_performance_over_season_query = """
        SELECT 
            p.player_id, 
            p.firstName, 
            p.lastName, 
            g.season,
            SUM(gs.Goals) AS season_goals,
            SUM(gs.Assists) AS season_assists,
            SUM(gs.Shots) AS season_shots,
            SUM(gs.Hits) AS season_hits
        FROM 
            player_info p
        JOIN 
            game_skater_stats gs ON p.player_id = gs.player_id
        JOIN 
            game g ON gs.game_id = g.game_id
        GROUP BY 
            p.player_id, p.firstName, p.lastName, g.season
        ORDER BY 
            p.player_id, g.season;
        """
    create_view(engine, "player_performance_over_season", player_performance_over_season_query)

    query_view(engine, "player_career_stats", "View Query: Player Career Stats")
    query_view(engine, "team_performance_stats", "View Query: Team Performance Stats")
    query_view(engine, "top_goalies", "View Query: Top Goalies")
    query_view(engine, "player_performance_over_season", "View Query: Player Performance Over Season")

    engine.dispose()
    print("\nAll queries and views executed successfully.")

if __name__ == "__main__":
    main()
