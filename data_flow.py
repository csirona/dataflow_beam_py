import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
import requests

class UploadFileToGCS(beam.DoFn):
    def process(self, element):
        import requests
        import csv
        import apache_beam as beam
        from apache_beam.options.pipeline_options import PipelineOptions

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

        # Specify the GCS output file path
        output_path = 'gs://your-bucket/classification_data.csv'  # Replace with your desired GCS output file path

        # Upload the file to GCS
        with beam.io.gcsio.GcsIO().open(output_path, 'wb') as gcs_file:
            with open(csv_file, 'rb') as local_file:
                gcs_file.write(local_file.read())

        yield output_path


def run_pipeline():
    # Create the pipeline options with GCP credentials and other necessary options
    options = PipelineOptions(
        runner='DataflowRunner',
        project='flowapiprimera',
        staging_location='gs://dataflow-staging-us-central1-5f1e024c90d7f8cfd09240d1be6169ed/staging',
        temp_location='gs://dataflow-staging-us-central1-5f1e024c90d7f8cfd09240d1be6169ed/temp',
        region='us-central1',
    )

    with beam.Pipeline(options=options) as pipeline:
        # Create a dummy element
        dummy_element = [None]

        # Upload the file to GCS
        uploaded_file = (
            pipeline
            | 'Create' >> beam.Create(dummy_element)
            | 'UploadFileToGCS' >> beam.ParDo(UploadFileToGCS())
        )

        # Print the GCS output file path
        uploaded_file | 'PrintOutputPath' >> beam.Map(print)


if __name__ == '__main__':
    run_pipeline()
