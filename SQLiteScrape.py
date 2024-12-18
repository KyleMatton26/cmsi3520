import mechanicalsoup
import pandas as pd
import sqlite3
import re

url = "https://en.wikipedia.org/wiki/List_of_Stanley_Cup_champions"
browser = mechanicalsoup.StatefulBrowser()
browser.open(url)

tables = browser.page.find_all("table", {"class": "wikitable"})
if len(tables) < 3:
    raise Exception("Less than 3 tables found on the page.")

table = tables[2] 
rows = table.find_all("tr")

years = []
winning_teams = []
winning_coaches = []
losing_teams = []
losing_coaches = []

remaining_winning_coach_rowspan = 0
current_winning_coach = ""

remaining_losing_coach_rowspan = 0
current_losing_coach = ""

for row in rows[1:]:
    cells = row.find_all(["th", "td"])
    cell_text = [cell.get_text(strip=True) for cell in cells]
    
    if len(cell_text) > 0 and cell_text[0] == "2005":
        continue
    
    idx = 0
    
    if remaining_winning_coach_rowspan > 0 or remaining_losing_coach_rowspan > 0:
        if idx < len(cell_text):
            year = cell_text[idx]
            years.append(year)
            idx += 1
        else:
            years.append(None)
    else:
        if idx < len(cell_text):
            year = cell_text[idx]
            years.append(year)
            idx += 1
        else:
            years.append(None)
    
    if idx < len(cell_text):
        winning_team = cell_text[idx]
        winning_teams.append(winning_team)
        idx += 1
    else:
        winning_teams.append(None)
    
    if remaining_winning_coach_rowspan > 0:
        winning_coach = current_winning_coach
        winning_coaches.append(winning_coach)
        remaining_winning_coach_rowspan -= 1
    else:
        if idx < len(cells):
            winning_coach_cell = cells[idx]
            rowspan = int(winning_coach_cell.get("rowspan", 1))
            winning_coach = winning_coach_cell.get_text(strip=True)
            current_winning_coach = winning_coach
            winning_coaches.append(winning_coach)
            remaining_winning_coach_rowspan = rowspan - 1
            idx += 1
        else:
            winning_coaches.append(None)
    
    if idx < len(cell_text):
        losing_team = cell_text[idx]
        losing_teams.append(losing_team)
        idx += 1
    else:
        losing_teams.append(None)
    
    if remaining_losing_coach_rowspan > 0:
        losing_coach = current_losing_coach
        losing_coaches.append(losing_coach)
        remaining_losing_coach_rowspan -= 1
    else:
        if idx < len(cells):
            losing_coach_cell = cells[idx]
            rowspan = int(losing_coach_cell.get("rowspan", 1))
            losing_coach = losing_coach_cell.get_text(strip=True)
            current_losing_coach = losing_coach
            losing_coaches.append(losing_coach)
            remaining_losing_coach_rowspan = rowspan - 1
            idx += 1
        else:
            losing_coaches.append(None)

dictionary = {
    "year": years,
    "winning_team": winning_teams,
    "winning_coach": winning_coaches,
    "losing_team": losing_teams,
    "losing_coach": losing_coaches
}

df = pd.DataFrame(data=dictionary)
print("First 5 records:")
print(df.head())
print("\nLast 5 records:")
print(df.tail())

connection = sqlite3.connect("stanley_cup.db")
cursor = connection.cursor()
cursor.execute("DROP TABLE IF EXISTS stanley_cup;")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS stanley_cup (
        year TEXT PRIMARY KEY,
        winning_team TEXT,
        winning_coach TEXT,
        losing_team TEXT,
        losing_coach TEXT
    )
""")

for i in range(len(df)):
    cursor.execute("INSERT OR IGNORE INTO stanley_cup VALUES (?,?,?,?,?)", tuple(df.iloc[i]))

connection.commit()
connection.close()
