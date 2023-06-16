import csv
import requests

api_url = 'https://api.unidadeditorial.es/sports/v1/classifications/current/?site=2&type=10&tournament=0152'

response = requests.get(api_url)

data = response.json()

# Specify the path and name for the CSV file
csv_file = 'classification_data.csv'

# Extract the relevant data from the response and prepare it for writing to the CSV file
rows = []

for i in range(len(data['data'][0]['rank'])):
    fullName = data['data'][0]['rank'][i]['fullName']
    position = data['data'][0]['rank'][i]['standing']['position']
    drawn = data['data'][0]['rank'][i]['standing']['drawn']
    lost = data['data'][0]['rank'][i]['standing']['lost']
    won = data['data'][0]['rank'][i]['standing']['won']
    pts = data['data'][0]['rank'][i]['standing']['points']
    played = data['data'][0]['rank'][i]['standing']['played']
    print(position + ' ' + fullName +' - pts: '+pts +',partidos jugados: '+played+', empates: '+drawn+', perdidos: '+lost+', won: '+won)
    
