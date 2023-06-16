import csv
import requests

api_url = 'https://api.unidadeditorial.es/sports/v1/classifications/current/?site=2&type=10&tournament=0152'

response = requests.get(api_url)
data = response.json()

# Specify the path and name for the CSV file
csv_file = 'classification_data.csv'

# Extract the relevant data from the response and prepare it for writing to the CSV file
rows = []
for rank in data['data'][0]['rank']:
    fullName = rank['fullName']
    position = rank['standing']['position']
    drawn = rank['standing']['drawn']
    lost = rank['standing']['lost']
    won = rank['standing']['won']
    pts = rank['standing']['points']
    played = rank['standing']['played']
    rows.append([fullName, position, drawn, lost, won, pts, played])

# Write the data to the CSV file
with open(csv_file, 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['fullName', 'position', 'drawn', 'lost', 'won', 'pts', 'played'])
    writer.writerows(rows)

print(f"Data saved to {csv_file}.")
