import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import csv
import requests


class GenerateFile(beam.DoFn):
    def process(self, element):
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

        yield csv_file


def run_pipeline():
    # Specify the GCS output file path
    output_path = 'gs://your-bucket/output.txt'  # Replace with your desired GCS output file path

    # Create the pipeline options with GCP credentials and other necessary options
    options = PipelineOptions()

    with beam.Pipeline(options=options) as pipeline:
        # Generate the file content
        file_content = (
            pipeline
            | 'Create' >> beam.Create([None])
            | 'GenerateFile' >> beam.ParDo(GenerateFile())
        )

        # Write the file content to GCS
        file_content | 'WriteToGCS' >> beam.io.WriteToText(output_path, windowed=True)

if __name__ == '__main__':
    run_pipeline()
