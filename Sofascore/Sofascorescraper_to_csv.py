import requests
import json 
import os 
from datetime import datetime
import pandas as pd


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
#response1 = response1.text
jsondata1 = response1.json()
#response2 = response2.text
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
        result = "Not started"
        results.append(result)
    if len(match['awayScore']) == 0:
        result = "Not started"
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


#df_reshaped.to_csv('Sofascore.csv', index=False)
#df_reshaped.to_excel('Sofascore.xlsx', index=False)

# Get the current working directory
current_dir = os.getcwd()
# Construct the relative path
relative_path = os.path.join(current_dir, "Sofascore.csv")

# Read the current data from the CSV file
current_data = pd.read_csv(relative_path, sep=",")

# Append to existing file

updated_data = pd.concat([current_data, df_reshaped], ignore_index=True)
updated_data = updated_data.sort_values(by = ['player1','player2','match_date','Time'], ascending = [False, False, False, False]) #Sort by time of data extaction

updated_data = updated_data.drop_duplicates(subset = ['player1','player2','match_date'],keep='first') #Extract only most updated status of match (first row)

# Save the updated data to csv and excel
import os
updated_data.to_csv("Sofascore.csv", index=False)
updated_data.to_excel('Sofascore.xlsx', index=False)

#os.chdir('..')
print("Sofascore file has been updated")