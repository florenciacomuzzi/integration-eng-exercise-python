import requests
from bs4 import BeautifulSoup

from integration_eng_exercise_python.utils import transform_url_to_raw


def process_line(line):
  if len(line) > 10:
    return {
      'upc': line[0],
      'price': line[4],
      'quantity': line[5]
    }

def extract_inventory_object_details(url):
    url = transform_url_to_raw(url)
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

    # Process path segments and separators
    path_parts = []
    elements = object_div.find_all('span')

    i = 0
    while i < len(elements):
        if 'path-sep' in elements[i].get('class', []):
            # Found a separator
            separator = elements[i].text.strip()
            path_parts.append(separator if separator else '/')
            i += 1
        elif 'path' in elements[i].get('class', []):
            # Found a path
            path_part = elements[i].text.strip()
            if i + 1 < len(elements) and 'path-sep' in elements[i + 1].get('class', []):
                # Path has a separator following it
                path_parts.append(path_part)
                separator = elements[i + 1].text.strip()
                path_parts.append(separator if separator else '/')
                i += 2
            elif i == len(elements) - 1:
                # Last path without separator, append it
                path_parts.append(path_part)
                i += 1
            else:
                # Path without separator and not last, discard it
                i += 1
        else:
            i += 1

    # Join all parts together
    object_path = ''.join(path_parts)

    return {
        'bucket': bucket,
        'region_code': region_code,
        'object_path': object_path
    }