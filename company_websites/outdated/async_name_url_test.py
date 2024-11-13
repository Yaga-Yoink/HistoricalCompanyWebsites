import asyncio
import aiohttp
import aiofiles
import json
import os
import pandas as pd
import re
from time import perf_counter
import csv
import datetime
from dotenv import load_dotenv

# Asynchronous request using aiohttp
async def send_request_async(role, message):
    async with aiohttp.ClientSession() as session:
        url = "http://localhost:1234/v1/chat/completions"  # Replace with actual API URL
        payload = {
            "messages": [
                {"role": "system", "content": role},
                {"role": "user", "content": message}
            ],
            "temperature": 0.7,
            "max_tokens": -1,
            "stream": False
        }

        headers = {
            "Content-Type": "application/json"
        }

        async with session.post(url, headers=headers, json=payload) as response:
            result = await response.json()
            return result["choices"][0]["message"]["content"]

# Convert API response to dictionary {company name : website url}
def name_to_url(data):
    name_url_dict = {}
    count = len(data["entities"])
    for i in range(count):
        if "website_url" in data["entities"][i]["properties"]:
            name = data["entities"][i]["properties"]["identifier"]["value"]
            name_url_dict[name] = data["entities"][i]["properties"]["website_url"]
    return name_url_dict

# Search for companies with matching names
async def organization_subset(company, organizations_df):
    subset = organizations_df[organizations_df["name"] == company["CompanyName"]]
    if subset.shape[0] == 0:
        split_company_names = re.split(r'[().\s-]+', company["CompanyName"])
        subset = organizations_df.loc[organizations_df["name"].str.contains(split_company_names[0], na=False)]
        end_index = 1
        while subset.shape[0] != 0 and end_index <= len(split_company_names):
            search_string = "".join([f".*{word}" for word in split_company_names[0:end_index]])
            test_subset = organizations_df.loc[organizations_df["name"].str.contains(search_string, na=False)]
            if test_subset.shape[0] > 0:
                subset = test_subset
            else:
                if subset.shape[0] > 10:
                    return subset.iloc[0:10, :]
                else:
                    return subset
            end_index += 1
    return subset

# Asynchronously process company URLs
async def get_website_url_llm(company_df):
    organizations_df = pd.read_csv("company_websites/odm/organizations.csv")
    organizations_df["probability"] = 0

    async with aiofiles.open(f"company_websites/name_url/name_url_{datetime.datetime.now().strftime('%m_%d_%H_%M_%S')}.csv", mode="w") as csvfile:
        writer = csv.writer(csvfile)
        await writer.writerow(["CompanyID", "CompanyName", "URL"])

        async def helper_func(row):
            probability = await send_request_async(
                "Act as a statistician that only returns one floating point value. Only return one floating point value, no text allowed.", 
                f"Give me the probability that the following text description is describing something related to finance, crypto, or venture capital: {row['short_description']}"
            )
            row["probability"] = float(probability)
            return row

        counter = 0
        for company_id, company in company_df.iterrows():
            if counter % 100 == 0:
                print(f"{counter} URLs Processed")
            subset = await organization_subset(company, organizations_df)
            if subset.shape[0] > 1:
                subset = await asyncio.gather(*[helper_func(row._asdict()) for row in subset.itertuples()])
                subset = pd.DataFrame(subset)
                subset["probability"] = pd.to_numeric(subset["probability"])
                max_index_label = subset["probability"].idxmax()
                selected_row = subset.loc[max_index_label]
                await writer.writerow([company_id, company["CompanyName"], selected_row["homepage_url"]])
                company_df.at[company_id, "URL"] = selected_row["homepage_url"]
            elif subset.shape[0] == 1:
                selected_row = subset.iloc[0]
                await writer.writerow([company_id, company["CompanyName"], selected_row["homepage_url"]])
                company_df.at[company_id, "URL"] = selected_row["homepage_url"]
            else:
                await writer.writerow([company_id, company["CompanyName"], ""])
            counter += 1
        return company_df

# Find missing companies
def missing_companies(names, name_url_dict):
    return list(set(names) - set(name_url_dict.keys()))

# Get missing websites
def missing_websites_df(df):
    return df.loc[df["URL"].isna()]

# Main function to trigger the async process
async def main():
    load_dotenv()
    company_df = missing_websites_df(pd.read_csv("company_websites/name_url/company_df_testing_full_llm.csv")).set_index("CompanyID")
    company_df = company_df[:10]

    start_time = perf_counter()
    company_df = await get_website_url_llm(company_df)
    print(f"Time Taken Getting Website URLs: {perf_counter() - start_time}")

if __name__ == "__main__":
    asyncio.run(main())
