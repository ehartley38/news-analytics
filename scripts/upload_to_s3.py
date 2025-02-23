from newsapi import NewsApiClient
from datetime import datetime, timedelta
import pandas as pd
import boto3
import os

SOURCE_BATCH_SIZE = 20
S3_BUCKET = 'news-analytics-ed'

def upload_to_s3():
    newsapi = NewsApiClient(api_key='930d0e46b185481c83c1eff5e6a07065')


    # Get Sources
    sources = newsapi.get_sources(language='en', category='general')

    source_ids = [source['id'] for source in sources.get('sources', [])]

    source_batches = [source_ids[i:i + SOURCE_BATCH_SIZE] for i in range(0, len(source_ids), SOURCE_BATCH_SIZE)]

    from_date = datetime.now() - timedelta(days=2)
    formatted_from_date = from_date.strftime("%Y-%m-%dT%H:%M:%S")

    # Extract all articles in batches due to source limit request of 20
    all_articles = []
    for batch in source_batches:
        sources_joined = ','.join(batch)
        articles = newsapi.get_everything(sources=sources_joined, from_param=formatted_from_date)['articles']
        all_articles.extend(articles)

    # Ensure the directory exists
    os.makedirs("./data", exist_ok=True)

    # Upload to S3
    formatted_from_date = formatted_from_date.replace(":", "-")
    dt = datetime.strptime(formatted_from_date, "%Y-%m-%dT%H-%M-%S")
    date_only = dt.date()


    df = pd.DataFrame(all_articles)
    filename = f"articles-{date_only}"
    pq_path = f"./data/{filename}.parquet"
    df.to_parquet(pq_path, engine='pyarrow')


    s3 = boto3.client('s3')
    s3.upload_file(pq_path, S3_BUCKET, filename)

    return filename, date_only






