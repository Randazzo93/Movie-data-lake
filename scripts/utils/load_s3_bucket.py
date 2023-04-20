import boto3
import os
from pathlib import Path
from dotenv import load_dotenv

dotenv_path = Path('env/.env')
load_dotenv(dotenv_path=dotenv_path)

#get enviroment variables
aws_key = os.getenv('AWS_KEY')
aws_secret = os.getenv('AWS_SECRET')

def load_s3_bucket(csv_file):
	#upload to s3 bucket
	s3 = boto3.client('s3',
			aws_access_key_id=aws_key,
			aws_secret_access_key= aws_secret)
		
	with open(csv_file, "rb") as file:
		s3.upload_fileobj(file, "myproject-imdb", csv_file)