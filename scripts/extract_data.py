import xml.etree.ElementTree as ET
import boto3
import io
import csv
import time
import logging
from io import BytesIO

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

s3 = boto3.client('s3')

#Writes the csv files to buckets, take bucket name, key name, row names, and row generator with specific filter as input
def stream_write(bucket, key, header, row_generator):
    logging.info(f"Writing to S3: {key}")
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(header)

    count = 0
    for row in row_generator:
        writer.writerow(row)
        count += 1
        if count % 50000 == 0:
            logging.info(f"{key} - written {count} rows...")

    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    logging.info(f"Done writing {count} rows to {key}")

#parse the xml file returned from the s3 has inner functions called generate row and row builder for each type - heart, steps,
#respiratory etc...

def parse_and_write(streaming_body):
    logging.info("Starting XML parsing using ElementTree...")

    #Generate row records based on the filter
    def generate_rows(record_type_filter, row_builder):
        context = ET.iterparse(streaming_body, events=('start',))
        for event, element in context:
            if element.tag != 'Record':
                continue
            if element.attrib.get('type') == record_type_filter:
                try:
                    yield row_builder(element)
                except Exception as e:
                    logging.warning(f"Skipped malformed record: {e}")
            element.clear()

    def heart_row(e):
        return (
            e.attrib.get('creationDate', ''),
            e.attrib.get('value', '0')
        )

    def sleep_row(e):
        return (
            e.attrib.get('creationDate', ''),
            e.attrib.get('startDate', ''),
            e.attrib.get('endDate', '')
        )

    def step_row(e):
        return (
            e.attrib.get('creationDate', ''),
            e.attrib.get('value', '0')
        )

    def resp_row(e):
        return (
            e.attrib.get('creationDate', ''),
            e.attrib.get('value', '0')
        )

    stream_write('iwatch-healthdata-csv', 'processed/Heart_Data.csv',
                 ('created_at', 'value'), generate_rows('HKQuantityTypeIdentifierHeartRate', heart_row))

    streaming_body.seek(0) #Resets file pointer to the beginning so it can be parsed again

    stream_write('iwatch-healthdata-csv', 'processed/Sleep_Data.csv',
                 ('created_at', 'start_date', 'end_date'), generate_rows('HKCategoryTypeIdentifierSleepAnalysis', sleep_row))

    streaming_body.seek(0)

    stream_write('iwatch-healthdata-csv', 'processed/Step_Data.csv',
                 ('created_at', 'count'), generate_rows('HKQuantityTypeIdentifierStepCount', step_row))

    streaming_body.seek(0)

    stream_write('iwatch-healthdata-csv', 'processed/Resp_Data.csv',
                 ('created_at', 'count'), generate_rows('HKQuantityTypeIdentifierRespiratoryRate', resp_row))

    logging.info("All CSVs written using ElementTree.")

def run():
    logging.info("Starting with ElementTree version...")
    start = time.time()

    #Fetches the s3 object from the bucket
    response = s3.get_object(
        Bucket='iwatch-healthdata-raw',
        Key='iwatch_health_export/export.xml'
    )

    logging.info("Reading the whole memory file from the streaming object might take some time")
    #Reads the whole memory file from the streaming object
    xml_bytes = response['Body'].read()
    #wraps the file into a seekable to allow multiple passes
    parse_and_write(BytesIO(xml_bytes))

    elapsed = round(time.time() - start, 2)
    logging.info(f"Job complete in {elapsed} seconds.")


if __name__ == '__main__':
    run()
