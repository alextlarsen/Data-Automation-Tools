import requests
import pandas as pd
import mysql.connector 
from mysql.connector import Error
import os 
import datetime

from datetime import datetime, timedelta
os.getcwd()
#os.chdir("Betting scraper")
date1 = (datetime.now()).strftime('%Y-%m-%d')
date2 = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
#date = '2024-04-12'
import requests
payload = ""
headers = {"User-Agent": "insomnia/8.6.1"}


# Request two URLs in one query
url1 = f"https://api.sofascore.com/api/v1/sport/tennis/scheduled-events/{date1}"
url2 = f"https://api.sofascore.com/api/v1/sport/tennis/scheduled-events/{date2}"
response1 = requests.request("GET",url1, data=payload, headers=headers)
response2 = requests.request("GET", url2, data=payload, headers=headers)

jsondata1 = response1.json()
jsondata2 = response2.json()

def extract_data(df):
  player_names = []
  results = []
  match_dates = []
  for match in df['events']:
    # Extract player names
    name1 = match['homeTeam']['name']
    name2 = match['awayTeam']['name']
    player_names.append(name1)
    player_names.append(name2)
    
    #Extract match date
    start_timestamp = match['startTimestamp']
    match_date = datetime.fromtimestamp(start_timestamp).strftime('%Y-%m-%d')
    match_dates.append(match_date)
    match_dates.append(match_date)
    
    # Extract match results
    if len(match['homeScore']) == 0 :
        result = None 
        results.append(result)
    if len(match['awayScore']) == 0:
        result = None
        results.append(result)
    else:
     results1 = match['homeScore'].get('current', None)
     results2 = match['awayScore'].get('current', None)
     results.append(results1)
     results.append(results2)
  return pd.DataFrame({'player': player_names, 'results': results, 'match_dates': match_dates})


df1 = extract_data(jsondata1)
df2 = extract_data(jsondata2)
data = pd.concat([df1, df2], ignore_index=True)
df_reshaped = pd.DataFrame()  

for i in range(0, len(data), 2): 
    df_reshaped = pd.concat([df_reshaped, pd.DataFrame({'index': data.index[i] // 2,
                                                         'player1': data['player'][i],
                                                         'player2': data['player'][i+1],
                                                         'result1': data['results'][i],
                                                         'result2': data['results'][i+1],
                                                         'match_date': data['match_dates'][i],
                                                          }, index=[0])], ignore_index=True)  

from datetime import datetime

# Get the current date and time
current_datetime = datetime.now()

time = current_datetime.strftime('%Y-%m-%d %H:%M')

# Add to df_reshaped
df_reshaped['Time'] = time

df_reshaped['result1'] = pd.to_numeric(df_reshaped['result1'], errors='coerce')  # Convert to numeric, invalid parsing will be set as NaN
df_reshaped['result2'] = pd.to_numeric(df_reshaped['result2'], errors='coerce')  # Repeat for result2 if needed
df_reshaped['result1'] = df_reshaped['result1'].where(pd.notnull(df_reshaped['result1']), None)
df_reshaped['result2'] = df_reshaped['result2'].where(pd.notnull(df_reshaped['result2']), None)

df_reshaped = df_reshaped.sort_values(by = ['player1','player2','match_date','Time'], ascending = [False, False, False, False]) #Sort by time of data extaction
df_reshaped = df_reshaped.drop_duplicates(subset = ['player1','player2','match_date'],keep='first') #Extract only most updated status of match (first row)

# Get connection to current table in mySQL database
connection = mysql.connector.connect(
    host = "localhost",
    user = "root",
    passwd = "",
    database = "tennis_db"
)

mycursor = connection.cursor()

#Append new data to existing mysql table

for i, row in df_reshaped.iterrows():
    row = [None if pd.isna(val) else val for val in row]
    insert_row_query =  f"""
            INSERT INTO Sofascore (`index`, player1, player2, result1, result2, `time`, match_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
    mycursor.execute(insert_row_query, tuple(row))

connection.commit()
# Delete duplicate records
mycursor.execute("DROP TABLE Sofascore_sorted")
connection.commit()
mycursor.execute("""
    CREATE TABLE Sofascore_sorted AS
    SELECT *
    FROM sofascore
    ORDER BY player1 DESC, player2 DESC, match_date DESC, Time DESC
""")
connection.commit()
# Step 2: Remove duplicates based on specified columns, keeping the first occurrence
mycursor.execute("""
    WITH CTE AS(
   SELECT [col1], [col2], [col3], [col4], [col5], [col6], [col7],
       RN = ROW_NUMBER()OVER(PARTITION BY col1 ORDER BY col1)
   FROM dbo.Table1
)
DELETE FROM CTE WHERE RN > 1
""")
connection.commit()
# Drop the temporary tables
mycursor.execute("DROP TABLE Sofascore_sorted")

connection.commit()
mycursor.close()
connection.close()








print("Sofascore file has been updated")