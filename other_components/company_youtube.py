import os
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
from dotenv import load_dotenv
from utils import iterate_func
from input import crypto_fintech_companies
import pandas as pd

# Search for 'video_count' number of youtube videos which contain 'keyword'
# Requires 'video_count' > 0
def search_company_video(keyword, video_count, youtube):
    request = youtube.search().list(
        part="snippet", q=keyword, type="video", maxResults=video_count
    )
    response = request.execute()
    result = {keyword: []}
    for item in response["items"]:
        result[keyword].append(item["snippet"]["title"])
    return result

# Initial setup of connection to Google Cloud and setup of youtube api
def main():
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = (
        "client_secret_420575062793-0ol65db83sdldp702vp4esb4p5du1d6o.json"
    )

    # Get credentials and create an API client
    flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
        client_secrets_file, scopes
    )
    credentials = flow.run_local_server()
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=API_KEY
    )
    return youtube


if __name__ == "__main__":
    load_dotenv()
    API_KEY = os.getenv("YOUTUBE_API_KEY")
    scopes = ["https://www.googleapis.com/auth/youtube.force-ssl"]

    youtube = main()
    result = iterate_func(search_company_video, crypto_fintech_companies, 20, youtube)
    pd.DataFrame.from_dict(result).to_csv("data/company_youtube.csv")
