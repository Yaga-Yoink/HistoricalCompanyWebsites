import requests
import json
from dotenv import load_dotenv
import os
import pandas as pd
from company_websites.llm_request import send_request
import asyncio


async def async_send_request(role, message):
    url = "http://localhost:1234/v1/chat/completions"

    # Your request payload
    payload = {
        "messages": [
            {"role": "system", "content": role},
            {"role": "user", "content": message},
        ],
        "temperature": 0.7,
        "max_tokens": -1,
        "stream": False,
    }

    headers = {"Content-Type": "application/json"}

    # Send POST request
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    # print('llm_response', response.json())
    return response.json()["choices"][0]["message"]["content"]


async def get_website_url_llm_async(company_df):
    organizations_df = pd.read_csv("../odm/organizations.csv")
    organizations_df["probability"] = 0
    organizations_dict = organizations_df[["name", "short_description"]].to_dict("index")
    tasks = []
    for company_id, company in company_df.iterrows():
        tasks.append(async_send_request(f"Act as an analyst that only returns the one company name. Only return the name of the company, no additional text allowed.", f"Given a dictionary mapping company names to short descriptions about the companies, tell me the company name of the company that is most likely {company["CompanyName"]} which is related to finance, crypto, or venture capital: {organizations_dict}"
))
    await asyncio.gather(*tasks)

async def main():
    ######## TESTING
    # Load the data
    company_df = pd.read_csv("20240831_vc backed companies (1).csv").set_index(
        "CompanyID"
    )
    # print(company_df)

    # Setup resulting df
    company_df = company_df[["CompanyName"]]
    company_df["URL"] = ""

    # TESTING PURPOSES
    company_df = company_df[:5]
    await get_website_url_llm_async(company_df)


load_dotenv()
api_key = os.getenv("CRUNCHBASE_API_KEY")


asyncio.run(main())
