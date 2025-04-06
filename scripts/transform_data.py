import pandas as pd
import boto3
import io
import time
import logging

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

s3 = boto3.client('s3')
source_bucket = 'iwatch-healthdata-csv'
target_bucket = 'iwatch-healthdatatransform-parquet'


def read_csv_from_s3(key):
    logging.info(f"Read File: {key}")
    source_key = f'processed/{key}'
    obj = s3.get_object(Bucket=source_bucket,Key = source_key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    logging.info(f"Loaded{len(df):,} rows from {key}")
    return df

def write_parquet_tos3(df,key):
    logging.info(f"Writing {len(df):,} records to {key}")
    target_key = f'transformed_parquet/{key}/{key}.parquet'
    out_buffer = io.BytesIO() #Creating a buffer stream
    df.to_parquet(out_buffer,index=False) #writing the df to the buffer stream
    s3.put_object(Bucket= target_bucket, Key = target_key, Body=out_buffer.getvalue())
    logging.info(f"Upload completed for {target_key}")

"""
Heart Data Tasks:
Convert created_at to datetime
Filter out heart rates outside 30–220 bpm
Aggregate daily average heart rate
Add year, month, day columns for partitioning
"""
def transform_heart_data(df):
    logging.info("Transforming Heart Data")
    df['created_at'] = pd.to_datetime(df['created_at'],errors ='coerce')
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df = df[(df['value']>=30) & (df['value']<=220)]
    df = df.dropna(subset=['created_at'])

    heart_daily = (
        df.groupby(df['created_at'].dt.date)
        .agg(avg_heart_rate = ('value','mean'))
        .reset_index()
    )

    heart_daily['created_at'] = pd.to_datetime(heart_daily['created_at'])
    heart_daily['year'] = heart_daily['created_at'].dt.year
    heart_daily['month'] = heart_daily['created_at'].dt.month
    heart_daily['day'] = heart_daily['created_at'].dt.day

    return heart_daily

"""
Respiration Data Tasks:
Convert created_at to datetime
Filter column count outside 8–40 bpm
Aggregate daily average respiratory rate
Add year, month, day columns
"""

def transform_resp_data(df):
    logging.info("Transforming Respiratory Rate Data")
    df['created_at'] = pd.to_datetime(df['created_at'],errors='coerce')
    df['count'] = pd.to_numeric(df['count'], errors='coerce')
    df=df[(df['count']>= 8) & (df['count']<=40)]
    df = df.dropna(subset=['created_at'])

    resp_daily = (
        df.groupby(df['created_at'].dt.date)
        .agg(avg_resp_rate=('count', 'mean'))
        .reset_index()
    )
    resp_daily['created_at'] = pd.to_datetime(resp_daily['created_at'])
    resp_daily['year'] = resp_daily['created_at'].dt.year
    resp_daily['month'] = resp_daily['created_at'].dt.month
    resp_daily['day'] = resp_daily['created_at'].dt.day
    return resp_daily

"""
Sleep Data Tasks:
Convert start_date and end_date to datetime
Drop sessions where end < start
Compute sleep duration in minutes
Aggregate daily sleep duration (per created_at)
Add year, month, day
"""
def transform_sleep_data(df):
    logging.info("Transforming Sleep Data")
    df['start_date'] = pd.to_datetime(df['start_date'],errors='coerce')
    df['end_date'] = pd.to_datetime(df['end_date'],errors='coerce')
    df['created_at'] = pd.to_datetime(df['created_at'],errors='coerce')
    df = df.dropna(subset=['created_at', 'start_date', 'end_date'])
    df = df[df['end_date'] > df['start_date']]

    df['duration_mins'] = (df['end_date'] - df['start_date']).dt.total_seconds() / 60

    sleep_daily = (
        df.groupby(df['created_at'].dt.date)
        .agg(total_sleep_minutes=('duration_mins', 'sum'))
        .reset_index()
    )
    sleep_daily['created_at'] = pd.to_datetime(sleep_daily['created_at'])
    sleep_daily['year'] = sleep_daily['created_at'].dt.year
    sleep_daily['month'] = sleep_daily['created_at'].dt.month
    sleep_daily['day'] = sleep_daily['created_at'].dt.day
    return sleep_daily

"""
Step Data Tasks:
Convert created_at to datetime
Remove rows with step counts > 100,000
Aggregate daily total steps
Add year, month, day
"""

def transform_step_data(df):
    logging.info("Transforming Step Count data")
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df = df[df['count'] <= 100000]
    df = df.dropna(subset=['created_at'])

    step_daily = (
        df.groupby(df['created_at'].dt.date)
        .agg(total_steps=('count', 'sum'))
        .reset_index()
    )
    step_daily['created_at'] = pd.to_datetime(step_daily['created_at'])
    step_daily['year'] = step_daily['created_at'].dt.year
    step_daily['month'] = step_daily['created_at'].dt.month
    step_daily['day'] = step_daily['created_at'].dt.day
    return step_daily


"""
Driver Function
"""

def main():

    start = time.time()
    logging.info("Starting Health Transformation ETL Job")

    try:
        heart_df = read_csv_from_s3('Heart_Data.csv')
        heart_tf = transform_heart_data(heart_df)
        write_parquet_tos3(heart_tf,'heart')

        resp_df = read_csv_from_s3('Resp_Data.csv')
        resp_tf = transform_resp_data(resp_df)
        write_parquet_tos3(resp_tf,'resp')

        sleep_df = read_csv_from_s3('Sleep_Data.csv')
        sleep_tf = transform_sleep_data(sleep_df)
        write_parquet_tos3(sleep_tf,'sleep')

        step_df = read_csv_from_s3('Step_Data.csv')
        step_tf = transform_step_data(step_df)
        write_parquet_tos3(step_tf,'step')

        logging.info("Health Transformation ETL Job completed")

        print(f"Time taken {round(time.time()-start,2)} seconds")
    
    except Exception as e:
        logging.error(f"ETL Job failed {e}",exc_info=True)


if __name__ == '__main__':
    main()