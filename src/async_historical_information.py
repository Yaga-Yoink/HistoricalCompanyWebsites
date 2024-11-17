from requests_html import AsyncHTMLSession
from bs4 import BeautifulSoup
import re
import pandas as pd
import os
import datetime
import asyncio
from sklearn.feature_extraction.text import TfidfVectorizer
import time
from rate_limiter import RateLimiter

from bs4.builder import XMLParsedAsHTMLWarning
import warnings

import logging

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
pd.options.display.max_rows = 1000

pd.options.mode.chained_assignment = None

date = datetime.datetime.now().strftime("%m_%d_%H_%M_%S")
web_url = "https://web.archive.org"

logger = logging.getLogger(__name__)

number_of_workers = 10

# Wayback machine cdx rate limiter
cdx_rate_limiter = RateLimiter(1, 0.25)
# Wayback Machine snapshot rate limiter
other_rate_limiter = RateLimiter(1, 2)

# Number of seconds before server will accept more get requests
cdx_restart_time = 150
other_restart_time = 80

# base_output_path = "/share/hariharan/rmf253"
base_output_path = "data/"

input_name_url_csv = "data/input_data/text_ready_name_url_10_07_02_49_38.csv"


# TODO: cleanup and modularize this better
# TODO: add driver


# Returns the column names of the output CSV file with 'n' historical versions.
def headers(n):
    headers = ["CompanyName", "URL", "probability", "Description"]
    for i in range(1, n + 1, 1):
        headers.append(f"text_version_{i}")
        headers.append(f"text_version_{i}_similarity_score")
    headers = ",".join(headers)
    return headers


# Returns the timestamps for up to 'n' versions of the 'company_url' website.
async def get_timestamps(session, company_url, n):
    await cdx_rate_limiter.api_limit()
    # A retry policy which tries to get it the first time, and if it encounters a rate limit will sleep untill it can hopefully successfully request again. Will only retry twice for links that do not return data.
    for _ in range(2):
        try:
            r = await session.get(
                f"{web_url}/cdx/search/cdx",
                params={
                    "url": company_url,
                    "output": "json",
                    "fl": ["timestamp"],
                    "limit": -n,
                    "filter": ["statuscode:200", "!mimetype:application/octet-stream"],
                },
            )
            response_json = r.json()
            return response_json[1:]
        except Exception as e:
            if "Errno 61" in str(e):
                logger.debug(f"Timestamp Rate Limitied: {e}")
                time.sleep(cdx_restart_time)
            else:
                logger.debug(f"Non Ratelimiting Timestamp Error: {e}")
                


# Get the historical text from a 'company_url' at a time 'timestamp' and save the text to 'file_path'.
async def get_historical_text(session, company_url, timestamp, file_path):
    with open(file_path, "a+") as text_file:
        home_page_text, linked_pages = await get_webpage_text(
            session, f"{web_url}/web/{timestamp}/{company_url}", 100000
        )
        text_file.write(home_page_text)
        if linked_pages:
            linked_pages = list(linked_pages)
            modified_linked_pages = linked_pages[:20]
            num_linked_pages = len(modified_linked_pages)
            # Limit of characters for tthe entire version of the company website
            character_limit = 200000
            per_page_limit = character_limit // num_linked_pages
            for page in modified_linked_pages:
                if not re.search(r".*\.(?i:pdf|jpg|gif|png|bmp)", page) and re.match(
                    "http", page
                ):
                    linked_page_text, _ = await get_webpage_text(
                        session, page, per_page_limit
                    )
                    text_file.write(linked_page_text)
        if home_page_text:
            return True
    return False


# Returns the webpage text for the website at 'get_url' and limits the amount of text to 'per_page_limit'. Requires:
async def get_webpage_text(session, get_url, per_page_limit):
    for _ in range(2):
        try:
            await other_rate_limiter.api_limit()
            # Websites that will return data normally return within 10 seconds. 20 seconds is used to prevent long timeouts slowing the program.
            r = await session.get(get_url, timeout=20)
            soup = BeautifulSoup(r.content, "html.parser")
            page_text = " ".join(soup.text.split())
            page_text = page_text[:per_page_limit]
            linked_pages = r.html.absolute_links
            del soup
            del r
            return page_text, linked_pages
        # The case where the url is not formed properly or the website can't be reached for whatever reason
        except Exception as e:
            # Errno 61 is the rate limiting exception
            if "Errno 61" in str(e):
                logger.debug(f"WaybackMachine Webpage Rate Limited: {e}")
                # Enough time to reset the rate limiting.
                await asyncio.sleep(other_restart_time)
            # If it wasn't rate limiting, retry the get requests immediately
            else:
                logger.debug(f"WacybackMachine Webpage Other Error: {e}")
    return "", None


# Helper function for handling a single company's website text. Gets 'n' timestamps of 'company' and saves the text to the corresponding file.
async def handle_company(session, company, n):
    if not pd.isna(company["URL"]):
        logger.info(f"{company["URL"]}: started")
        timestamps = await get_timestamps(session, company["URL"], n)
        if timestamps:
            company_dir = (
                f"{base_output_path}/website_text/{date}/{company['CompanyName']}"
            )
            if timestamps and not pd.isnull(company["URL"]):
                os.makedirs(company_dir, exist_ok=True)

                tasks = []
                tasks.append(
                    save_company_data(session, company, timestamps, company_dir)
                )
                files = await asyncio.gather(*tasks)
                return files
    else:
        logger.info(f"{company["CompanyName"]}: failed (no url)")
    return [{company.name: []}]


# Returns the similarity score of 'text' to the 'company_description'.
async def get_similarity_score(text, company_description):
    documents = [text, company_description]
    tfidf = TfidfVectorizer().fit_transform(documents)
    # No need to normalize, since Vectorizer will return normalized tf-idf
    similarity_score = tfidf * tfidf.T
    return similarity_score[0, 1]


# Returns the list of filenames created for each timestamp in 'timestamps' of the 'company' website. Saves the website text to the corresponding file in 'company_dir'.
async def save_company_data(session, company, timestamps, company_dir):
    files = {company["CompanyID"]: []}
    for timestamp in timestamps:
        timestamp = timestamp[0]
        company_name = company["CompanyName"]
        company_name = re.sub("/", "_", company_name)
        file_path = f"{company_dir}/{company_name}_{timestamp}.txt"
        saved_text_bool = await get_historical_text(
            session, company["URL"], timestamp, file_path
        )
        if saved_text_bool:
            files[company["CompanyID"]].append(file_path)
    return files


# Add the new timestamp columns from 'headers' for the company in 'company_df' as well as similarity scores for each file in 'files'.
async def add_timestamp_columns(company_df, files, headers):
    company_df = pd.DataFrame(company_df).transpose()
    new_col_id = 0
    # Get the company_id from the dict inside the list
    company_id = next(iter(files[0]))
    for index, filepath in enumerate(files[0][company_id]):
        # Routine for adding the new column
        if index == new_col_id:
            company_df[f"text_version_{new_col_id + 1}"] = ""
            company_df[f"text_version_{new_col_id + 1}_similarity_score"] = ""
        company_df[f"text_version_{index + 1}"] = filepath
        text = open(filepath, "r").read()
        company_df[f"text_version_{index + 1}_similarity_score"] = (
            await get_similarity_score(text, company_df["Description"].values[0])
        )
        new_col_id += 1
    for header in headers:
        if header not in list(company_df.columns.values):
            company_df[header] = ""
    company_df = company_df.set_index("CompanyID")
    company_df.to_csv(
        f"{base_output_path}/historical_versions/test_async_historical_{date}.csv",
        mode="a",
        index=False,
        header=False,
    )


# Iterate through companies in 'company_df', gather 'n' versions of the website text, save the website text to text file, and save the CSV which documents what companies and their timestamps were collected.
async def iterate_historical(company_df, n):
    session = AsyncHTMLSession(workers=number_of_workers)
    os.makedirs(f"{base_output_path}/website_text/{date}", exist_ok=True)
    # Wayback Machine can handle 14 parallel requests
    semaphore = asyncio.Semaphore(14)
    header = headers(n)

    async def semaphore_handle_company(session, company, n, sem):
        async with sem:
            files = await handle_company(session, company, n)
            await add_timestamp_columns(company, files, header)

    tasks = []
    for _, company in company_df.iterrows():
        tasks.append(semaphore_handle_company(session, company, n, semaphore))
    await asyncio.gather(*tasks, return_exceptions=False)


# Async entry point
def async_historical_entry(company_df, n):
    asyncio.run(iterate_historical(company_df, n))


# Initializes a historical_versions directory and outputs the columns into an initial CSV.
def output_header_csv(n):
    os.makedirs("historical_versions", exist_ok=True)
    header = headers(n)
    with open(
        f"{base_output_path}/historical_versions/test_async_historical_{date}.csv", "a+"
    ) as text_file:
        text_file.write(header)
        text_file.write("\n")

# Setup the logger by initializing the log directory if not made and create the new log file
def init_log():
    os.makedirs("src/logs/async_historical_information", exist_ok=True)
    logging.basicConfig(filename=f"src/logs/async_historical_information/async_historical_information_{date}.log", encoding='utf-8', level=logging.DEBUG)

if __name__ == "__main__":
    init_log()
    name_url_csv = input_name_url_csv
    company_df = pd.read_csv(name_url_csv)
    number_timestamps = 10
    output_header_csv(number_timestamps)
    async_historical_entry(company_df, 10)
