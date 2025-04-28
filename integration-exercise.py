import csv

from integration_eng_exercise_python.inventory_utils import extract_inventory_object_details, \
    process_line
from integration_eng_exercise_python.s3_helper import S3Helper


# 1. Using requests or any other HTTP library, grab the file HTML from: https://bitbucket.org/cityhive/jobs/src/master/integration-eng/integration-entryfile.html
# 2. Then, parse the URL for the csv file located in the S3 bucket (as part of the script, not by hand)
# 3. Make a GET request to Amazon's S3 with the details from #2 and save the to `local_file_path`
initial_html_file = 'https://bitbucket.org/cityhive/jobs/src/master/integration-eng/integration-entryfile.html'
local_file_path = 'inventory_export.csv'


if __name__ == '__main__':
    s3_details = extract_inventory_object_details(initial_html_file)
    bucket = S3Helper(s3_details['bucket'], s3_details['region_code'])
    bucket.download_key_with_presigned_url(
        s3_details['object_path'], local_file_path)

    with open(local_file_path, 'r') as in_file:
        reader = csv.reader(in_file, delimiter='|')
        for line in reader:
            l = process_line(line)
            if l:
                print(l)
