import csv
import requests
from bs4 import BeautifulSoup

# SOURCE_URL = 'https://bitbucket.org/cityhive/jobs/src/master/integration-eng/integration-entryfile.html'
SOURCE_URL = 'https://github.com/florenciacomuzzi/integration-eng-exercise-python/blob/main/integration-entryfile.html'

# 1. Using requests or any other HTTP library, grab the file HTML from: https://bitbucket.org/cityhive/jobs/src/master/integration-eng/integration-entryfile.html
# 2. Then, parse the URL for the csv file located in the S3 bucket (as part of the script, not by hand)
# 3. Make a GET request to Amazon's S3 with the details from #2 and save the to `local_file_path`
initial_html_file="url-to-html-file"
local_file_path = "path-to-exported-file"


def process_line(line):
  if len(line) > 10:
    return {
      'upc': line[0],
      'price': line[4],
      'quantity': line[5]
    }

def get_html(url):
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for bad status codes
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Extract S3 bucket information
    bucket_div = soup.find(id='bucket-value')
    region_div = soup.find(id='region-value')
    object_div = soup.find(id='object-value')
    
    if not all([bucket_div, region_div, object_div]):
        raise ValueError("Could not find all required S3 information in the HTML")
    
    bucket = bucket_div.text.strip()
    region_code = region_div.get('data-region', '')  # Get the actual AWS region code
    object_path = object_div.text.strip()
    
    # Clean up the object path by removing the path separators and extra whitespace
    object_path = ' '.join(object_path.split())
    
    return {
        'bucket': bucket,
        'region_code': region_code,
        'object_path': object_path
    }


if __name__ == '__main__':
  s3_details = get_html(SOURCE_URL)
  # with open(local_file_path, 'r') as in_file:
  #   reader = csv.reader(in_file, delimiter='|')
  #   for line in reader:
  #     l = process_line(line)
  #     if l: print(l)

  print(s3_details)