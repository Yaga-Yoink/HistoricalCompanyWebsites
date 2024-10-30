from requests_html import AsyncHTMLSession
from bs4 import BeautifulSoup
import re
import pandas as pd
import os
from time import perf_counter
import datetime
import asyncio
# from memory_profiler import profile


# from lingua import LanguageDetectorBuilder, Language
from sklearn.feature_extraction.text import TfidfVectorizer
import time

from bs4.builder import XMLParsedAsHTMLWarning
import warnings

warnings.filterwarnings('ignore', category=XMLParsedAsHTMLWarning)
pd.options.display.max_rows=1000

pd.options.mode.chained_assignment = None

date = datetime.datetime.now().strftime("%m_%d_%H_%M_%S")
web_url = "https://web.archive.org"

number_of_workers = 10

start_time = datetime.datetime.now()
api_call_count = 0


# # Return the language of "text"
# async def determine_language(text):
#     detector = LanguageDetectorBuilder.from_all_languages().build()
#     language = detector.detect_language_of(text)
#     return language


# # Block the program from continuing until time_period is passed with the desired number_of_calls
# async def api_limit(time_period, number_of_calls):
#     global api_call_count
#     global start_time
#     api_call_count += 1
#     if api_call_count >= number_of_calls:
#         end_time = datetime.datetime.now()
#         while (end_time - start_time).seconds < time_period:
#             print("Sleeping")
#             time.sleep(1)
#             end_time = datetime.datetime.now()
#         start_time = datetime.datetime.now()
#         api_call_count = 1
#     return

class RateLimiter:
    def __init__(self, time_period, number_of_calls):
        self.time_period = time_period  # Time period in seconds
        self.number_of_calls = number_of_calls  # Max API calls allowed in time period
        self.api_call_count = 0
        self.start_time = datetime.datetime.now()
        self.lock = asyncio.Lock()  # For thread safety

    async def api_limit(self):
        async with self.lock:  # Ensure only one coroutine can modify shared variables at a time
            self.api_call_count += 1
            
            if self.api_call_count >= self.number_of_calls:
                end_time = datetime.datetime.now()

                # Calculate the time that has passed since the start
                time_elapsed = (end_time - self.start_time).seconds

                # Sleep if we haven't completed the time period
                if time_elapsed < self.time_period:
                    # print("Rate limit reached. Sleeping...")
                    await asyncio.sleep(self.time_period - time_elapsed)
                
                # Reset the start time and API call count
                self.start_time = datetime.datetime.now()
                self.api_call_count = 1


# Adjust rate limiter to not hit limits
# Testing values
cdx_rate_limiter = RateLimiter(1, 1)
other_rate_limiter = RateLimiter(1, 2)

# Async function to get timestamps for up to 'n' versions of the website
async def get_timestamps(session, company_url, n):
    await cdx_rate_limiter.api_limit()
    flag = True
    while flag:
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
            flag = False
        except Exception as e:
            if "Errno 61" in str(e):
                # print(e)
                # print("CDX Server Rate Limit Sleeping")
                time.sleep(121)
            else:
                flag = False
    try:
        response_json = r.json()
        return response_json[1:]  # Remove the "timestamp" header
    # Exception for any issues with getting the timestamps
    except Exception as e:
        # print(f"Issue with collecting timestamp of {company_url} : {e}")
        return []


# Async function to get the historical text from a company URL and its linked pages
async def get_historical_text(session, company_url, timestamp, file_path):
    with open(file_path, "a+") as text_file:
        home_page_text, linked_pages = await get_webpage_text(
        session, f"{web_url}/web/{timestamp}/{company_url}", 100000
    )
        text_file.write(home_page_text)

        if linked_pages:
            # Find every linked page with the same domain
            # print(linked_pages)
            if len(linked_pages) != 0:
                counter = 0
                modified_linked_pages = []
                for page in linked_pages:
                    if counter >= 50:
                        break
                    modified_linked_pages.append(page)
                    counter += 1
                num_linked_pages = len(modified_linked_pages)
                character_limit = 100000
                per_page_limit = character_limit // num_linked_pages
            for page in modified_linked_pages:
                if not re.search(r".*\.(?i:pdf|jpg|gif|png|bmp)", page) and re.match(
                    "http", page
                ):
                    #TOD: This could be teh cause of larg ememory usage, lots of beautiful soup processes of millions of characters
                    linked_page_text, _ = await get_webpage_text(session, page, per_page_limit)
                    text_file.write(linked_page_text)
        if home_page_text:
            return True
    return False


# Async function to get the webpage text and response for a given URL
# @profile
async def get_webpage_text(session, get_url, per_page_limit):
    try:
        await other_rate_limiter.api_limit()
        r = await session.get(get_url, timeout = 10)
        soup = BeautifulSoup(r.content, "html.parser")
        page_text = " ".join(soup.text.split())
        page_text = page_text[:per_page_limit]
        linked_pages = r.html.absolute_links
        del soup
        del r
        return page_text, linked_pages
    # The case where the url is not formed properly or the website can't be reached for whatever reason
    except Exception as e:
        # print(type(e))
        # print(f"Unable to get webpage text of {get_url}: {e}")
        if "Errno61" in str(e):
            # print("Going to sleep for a minute untill get_webpage ratelimit finishes. ")
            time.sleep(61)
        # time.sleep(61)
        return "", None


# Async function to handle a single company's data
async def handle_company(session, company, n):
    timestamps = await get_timestamps(session, company["URL"], n)
    company_dir = f"website_text/{date}/{company['CompanyName']}"
    # print(company["URL"])
    
    if timestamps and not pd.isnull(company["URL"]):
        os.makedirs(company_dir, exist_ok=True)

        tasks = []
        tasks.append(save_company_data(session, company, timestamps, company_dir))
        website_start_time = perf_counter()
        files = await asyncio.gather(*tasks)
        # print(f"Time Taken Saving Website Text: {str(perf_counter() - website_start_time)}")
        return files
    return [{company.name: []}]


# Returns the similarity score of [text] to the company description
async def get_similarity_score(text, company_description):
    documents = [text, company_description]
    tfidf = TfidfVectorizer().fit_transform(documents)
    # no need to normalize, since Vectorizer will return normalized tf-idf
    similarity_score = tfidf * tfidf.T
    return similarity_score[0, 1]


# Async function to save company data
async def save_company_data(session, company, timestamps, company_dir):
    # print('save_company_data', company, company["CompanyName"])
    files = {company["CompanyID"]: []}
    for index, timestamp in enumerate(timestamps):
        timestamp = timestamp[0]
        company_name = company["CompanyName"]
        company_name = re.sub("/", "_", company_name)
        file_path = f"{company_dir}/{company_name}_{timestamp}.txt"
        
        saved_text_bool = await get_historical_text(session, company["URL"], timestamp, file_path)
        # if company["hq"] != "United States" and determine_language(text) != Language.ENGLISH:
        if saved_text_bool:
            files[company["CompanyID"]].append(file_path)
    return files


# Add the new timestamp columns as well as similarity scores for the text to the company_df
async def add_timestamp_columns(company_df, files):
    company_df = pd.DataFrame(company_df).transpose()
    # print('start_company_df', company_df)
    new_col_id = 0
    # for company_file_dict in files:
        # Get the company_id from the dict inside the list
    company_id = next(iter(files[0]))
    # print('company_id', company_id)
    for index, filepath in enumerate(files[0][company_id]):
        # Routine for adding the new column
        if index == new_col_id:
            company_df[f"text_version_{new_col_id + 1}"] = ""
            company_df[f"text_version_{new_col_id + 1}_similarity_score"] = (
                "" 
            )
        company_df[f"text_version_{index + 1}"] = filepath
        text = open(filepath, "r").read()
        # print('here', company_df)
        company_df[f"text_version_{index + 1}_similarity_score"] = (
            await get_similarity_score(
                text, company_df["Description"].values[0]
            )
        )
        new_col_id += 1

    headers = ['CompanyID', 'CompanyName', 'URL', 'probability', 'Description', 'text_version_1', 'text_version_1_similarity_score', 'text_version_2', 'text_version_2_similarity_score', 'text_version_3', 'text_version_3_similarity_score', 'text_version_4', 'text_version_4_similarity_score', 'text_version_5', 'text_version_5_similarity_score', 'text_version_6', 'text_version_6_similarity_score', 'text_version_7', 'text_version_7_similarity_score', 'text_version_8', 'text_version_8_similarity_score', 'text_version_9', 'text_version_9_similarity_score', 'text_version_10', 'text_version_10_similarity_score']
    # print("company_series_before", company_df)
    for header in headers:
        # print("companindexvallist" , list(company_df.columns.values))
        if header not in list(company_df.columns.values):
            company_df[header] = ""
    company_df = company_df.set_index("CompanyID")
    # print("final compydf", company_df)
    company_df.to_csv(
        f"historical_versions/test_async_historical_{date}.csv",
        mode="a",
        index=False,
        header=False,
    )

# Async function to iterate through companies and gather historical website text
async def iterate_historical(session, chunked_company_df, n):
    os.makedirs(f"website_text/{date}", exist_ok=True)

    for chunk in chunked_company_df:
        company_start_time = perf_counter()
        tasks = []
        for _, company in chunk.iterrows():
            # Await inside the append in order to get the text of the websites at the same time as getting the timestamps
            # Enables processing of texts while slowing down the amount of calls to the cdx
            tasks.append(handle_company(session, company, n))
                # Create tasks without awaiting
        files = await asyncio.gather(*tasks)
        # print('files', files)
        timestamp_start = perf_counter()
        timestamp_tasks = []
        print(chunk.head())
        for i, (_, company) in enumerate(chunk.iterrows()):
            # print('i', i)
            timestamp_tasks.append(add_timestamp_columns(company, files[i]))
        await asyncio.gather(*timestamp_tasks)
        # print(f"Time Taken Adding Timestamps: {perf_counter() - timestamp_start}")
        # print(f"Time Taken Processing {company["CompanyName"]} : {str(perf_counter() - company_start_time)}")


# Run the async function using AsyncHTMLSession to handle the event loop
async def async_main(chunked_company_df, n):
    session = AsyncHTMLSession(workers=number_of_workers)
    await iterate_historical(session, chunked_company_df, n)


def async_historical_entry(chunked_company_df, n):
    asyncio.run(async_main(chunked_company_df, n))


def output_header_csv(n):
    os.makedirs("historical_versions", exist_ok=True)
    header = headers()
    with open(
        f"historical_versions/test_async_historical_{date}.csv", "a+"
    ) as text_file:
        text_file.write(header)
        text_file.write("\n")


def headers():
    headers = ["CompanyName", "URL", "probability", "Description"]
    for i in range(1, 11, 1):
        headers.append(f"text_version_{i}")
        headers.append(f"text_version_{i}_similarity_score")
    headers = ",".join(headers)
    return headers


if __name__ == "__main__":
    # chunked_company_df = pd.read_csv(
    #     "text_ready_name_url_10_07_02_49_38.csv", chunksize=5
    # )
    chunked_company_df = pd.read_csv(
        "second_run.csv", chunksize=14
    )
    output_header_csv(10)
    async_historical_entry(chunked_company_df, 10)