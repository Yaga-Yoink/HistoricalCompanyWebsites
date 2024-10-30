from bs4 import BeautifulSoup
import pandas as pd
from time import perf_counter
import json
import datetime
import os
from requests_html import HTMLSession
import re

#TODO: Figure out why RobinHood one has no text, possibly because all of it is linked with js
#TODO: Improve website url matching using a different api, or figuring out whether its the right company
#   -could just like put the text into an llm and ask if it sounds like a crypto or vc fund
#TODO: Figure out how to speed up the process, currently would take like 30 hours for all 2969 companies
#   - reduce api calls (seems difficult), benchmark to see where the time is taken

pd.options.mode.chained_assignment = None

global web_url
web_url = "http://web.archive.org"
global session
session = HTMLSession()
global date
date = datetime.datetime.now().strftime("%m_%d_%H_%M_%S")
output_csv_path = f"company_websites/historical_versions/historical_version_{date}.csv"

# Returns the timestamps in yyyymmddhhmmss of up to 'n' versions of the company website at 'company_url)
def get_timestamps(company_url, n):
    # -n to get the most recent versions, really old versions are more likely to have a website run by other company
    response = session.get(
        f"{web_url}/cdx/search/cdx",
        params={
            "url": company_url,
            "output": "json",
            "fl": ["timestamp"],
            "limit": -n,
            "filter": ["statuscode:200", "!mimetype:application/octet-stream"]
        },
    )
    try:
        response = response.json()[1:]
    except json.decoder.JSONDecodeError:
        response = []
    return response


# Return a string containing all of the text from 'company_url' at time 'timestamp' with additional text from all other websites the homepage links to
def get_historical_text(company_url, timestamp):
    result_pages_text = ""
    print('url', f"{web_url}/web/{timestamp}/{company_url}")
    home_page_text, response = get_webpage_text(f"{web_url}/web/{timestamp}/{company_url}")
    result_pages_text += home_page_text
    if home_page_text != "":
        # Find every linked page with the same domain
        linked_pages = response.html.absolute_links
        for page in linked_pages:
            if not re.search(".*(?i:pdf|jpg|gif|png|bmp)", page):
                new_text, _ = get_webpage_text(page)
                result_pages_text += new_text
    return result_pages_text

# Return the text of the webpage at 'get_url' and the 'soup' instance
def get_webpage_text(get_url):
    response = session.get(get_url)
    soup = BeautifulSoup(response.text.encode('ascii',errors='ignore') , "html.parser")
    page_text = soup.text
    # Remove multiple whitespaces and newlines from html formatting
    page_text = " ".join(page_text.split())
    page_text = page_text.replace(",", " ")
    return page_text, response
    
# Iterate through the companies with urls and return the 'company_df' with up to n columns for the historical text and n timestamp columns
def iterate_historical(chunked_company_df, n):
    os.mkdir(f"company_websites/website_text/{date}")
    new_col_id = 1
    for company_df in chunked_company_df:
        company_df = company_df.set_index("CompanyID")
        for company_id, company in company_df.iterrows():
            timestamps = get_timestamps(company["URL"], n)
            if len(timestamps) != 0 and not pd.isnull(company["URL"]):
                os.mkdir(f"company_websites/website_text/{date}/{company["CompanyName"]}")
                for index, timestamp in enumerate(timestamps):
                    text = get_historical_text(company["URL"], timestamp[0])
                    # If the column doesn't exist, then create a new one for the data
                    if index == new_col_id - 1:
                        company_df[f"text_version_{new_col_id}"] = ""
                        company_df[f"timestamp_version_{new_col_id}(yyyymmddhhmmss)"] = ""
                        new_col_id += 1
                    with open(f"company_websites/website_text/{date}/{company["CompanyName"]}/{company['CompanyName']}_{timestamp[0]}.txt", "a+") as text_file:
                        text_file.write(text)
                    company_df.at[company_id, f"text_version_{index + 1}"] = f"website_text/{company["CompanyName"]}_{timestamp[0]}.txt"
                    company_df.at[company_id, f"timestamp_version_{index+1}(yyyymmddhhmmss)"] = timestamp[0]
        if company_df.columns.values
        company_df.to_csv(output_csv_path, mode = 'a', index=False, header=False)

def output_header_csv(n):
    os.makedirs("historical_versions", exist_ok=True)
    headers = ["CompanyName","URL","probability","Description"]
    for i in range(1, 11, 1):
        headers.append(f"text_version_{i}")
        headers.append(f"text_version_{i}_similarity_score")        
    headers = ",".join(headers)
    with open(f"company_websites/historical_versions/test_async_historical_{date}.csv", "a+") as text_file:
       text_file.write(headers)
       text_file.write("\n")

if __name__ == "__main__":
    start_time = perf_counter()    
    chunked_company_df = (pd.read_csv("text_ready_name_url_10_07_02_49_38.csv", chunksize=1))
    output_header_csv(10)
    iterate_historical(chunked_company_df, 10)
    print(str(perf_counter() - start_time))
