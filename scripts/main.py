from upload_to_s3 import upload_to_s3
from process_article_data import process_article_data


file_name, file_date = upload_to_s3()

process_article_data(file_name, file_date)


