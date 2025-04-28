import re


def transform_url_to_raw(url):
    """
    Transforms a Bitbucket HTML view URL to its raw view equivalent.
    Example:
    Input: https://bitbucket.org/your-workspace/your-repo/src/main/path/to/file.csv
    Output: https://bitbucket.org/your-workspace/your-repo/raw/main/path/to/file.csv
    """
    if not url:
        return None

    # Use regex to find and replace 'src' with 'raw'
    pattern = r'(https?://[^/]+/[^/]+/[^/]+/)src(/.*)'
    replacement = r'\1raw\2'

    transformed_url = re.sub(pattern, replacement, url)

    # If the URL wasn't transformed (no 'src' found), return None
    if transformed_url == url:
        return None

    return transformed_url
