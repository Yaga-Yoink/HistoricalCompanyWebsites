import asyncio
from twscrape import API, gather
from twscrape.logger import set_log_level
from dotenv import load_dotenv
import os
from utils import iterate_func
from input import crypto_fintech_companies
import pandas as pd


# Return 'post_count' number of posts that have content matching 'keyword'
# Requires post_count > 0
async def search_company_tweets(keyword, post_count):
    api = API()  # or API("path-to.db") - default is `accounts.db`
    data = {keyword: []}
    async for tweet in api.search(keyword, limit=post_count):
        data[keyword].append(tweet.rawContent)
        # Use the code below for {content : url}
        # For filtering foreign language and unkown characters
        # if tweet.rawContent.isascii():
        #     data[tweet.rawContent] = tweet.url
    # change log level, default info
    set_log_level("DEBUG")
    return data


# Helper function for iteratively calling 'search_company_tweets'
def call_search_company_tweets(keyword, post_count):
    data = asyncio.run(search_company_tweets(keyword, post_count))
    return data


if __name__ == "__main__":
    # Login with CLI before using
    load_dotenv()
    password = os.getenv("TWITTER_PASSWORD")
    result = iterate_func(call_search_company_tweets, crypto_fintech_companies, 1)
    pd.DataFrame.from_dict(result, orient="index").transpose().to_csv(
        "data/company_tweets.csv"
    )
