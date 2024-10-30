import requests
import json
from dotenv import load_dotenv
import os
import pandas as pd
import re
from time import perf_counter
import csv
import datetime
from llm_request import send_request

# big issue I believe is that multiple company urls are coming back for single company names, so this means that some valid company urls are not being searched for because of the limit
organizations_data_path = "company_websites/odm/organizations.csv"


# Search for corresponding names from list 'company_names' and their website urls
def get_website_url(company_names):
    # TODO: go for single equality, then if not return the top 5 links that contain pattern matched websites
    result_dict = {}
    curr_index = 0
    company_count = 200
    if len(company_names) > 1000:
        while curr_index + company_count < len(company_names):
            response = send_request(company_names_slice=company_names[curr_index:curr_index+company_count])
            result_dict.update(name_to_url(response))
            curr_index += company_count
    response = send_request(company_names_slice=company_names[curr_index:])
    result_dict.update(name_to_url(response))
    return result_dict

# Get a new dataframe containing the companies with the closest matching names
def organization_subset(company, organizations_df):
    subset = organizations_df[organizations_df["name"] == company["CompanyName"]]
    if subset.shape[0] == 0:
        split_company_names = company["CompanyName"].replace("(", "").replace(")", "").replace("-", " ").replace(".", " ")
        split_company_names = split_company_names.split()
        subset = organizations_df.loc[organizations_df["name"].str.contains(split_company_names[0], na=False)]
        end_index = 1
        while subset.shape[0] != 0 and end_index <= len(split_company_names):
            search_string = "".join([f".*{word}" for word in split_company_names[0:end_index]])
            test_subset = organizations_df.loc[organizations_df["name"].str.contains(search_string, na=False)]
            if test_subset.shape[0] > 0:
                subset = test_subset
            else:
                # if subset.shape[0] > 10:
                return subset.iloc[:]
                # else:
                #     return subset
            end_index += 1
    return subset
            
# Returns the formatted 'organizations_df'.   
def setup_organizations_df():
    organizations_df = pd.read_csv(organizations_data_path)
    organizations_df["probability"] = 0
    return organizations_df
    
# Search for the website urls of the names from the company names in "CompanyName" columnn of "company_df". 
# Returns "company_df" with a the "URL" column filled as much as possible. 
def get_website_url_llm(company_df):
    with open(f"company_websites/name_url/name_url_{datetime.datetime.now().strftime("%m_%d_%H_%M_%S")}.csv", "w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["CompanyID", "CompanyName", "URL", "probability"])
        
        def helper_func(row, company):
            #TODO: PUT IN THE SHORT DESCRIPTION FROM THE INPUT CSV AND THE ORGANIZATIONS CSV TO MATCH TOGETHER
            row["probability"] = float(send_request(f"Act as a statistician that only returns one floating point value. Only return one floating point value, not text allowed.", f"Give me the probability that the following descriptions are related to eachother or cryptpo/finance/business in general: ({company["Description"]}) : ({row["short_description"]})"))
            return row
        
        organizations_df = setup_organizations_df()
        counter = 0
        for company_id, company in company_df.iterrows():
            if counter % 100 == 0:
                print(f"{counter} Urls Processed")
            subset = organization_subset(company, organizations_df)
            # Added a max of 50 possibiltiies to check
            if subset.shape[0] > 1 and subset.shape[0] < 100:
                subset = subset.transform(lambda row : helper_func(row, company), axis = 1)
                subset["probability"] = pd.to_numeric(subset["probability"])
                # TODO: ADD A THRESHOLD FOR THE PROBABILITY BEFORE ACCEPTING
                max_index_label = subset["probability"].idxmax()
                subset = subset.loc[max_index_label]
                print(company["CompanyName"], subset["probability"])
                if subset["probability"] > .5:
                    writer.writerow([company_id, company["CompanyName"], subset["homepage_url"], subset["probability"]])
                    company_df.at[company_id, "URL"] = subset["homepage_url"]
                else:
                    writer.writerow([company_id, company["CompanyName"], "", ""])
            elif subset.shape[0] == 1:
                subset = subset.transform(lambda row : helper_func(row, company), axis = 1)
                print(company["CompanyName"], subset["probability"].values[0])
                # subset = subset.iloc[0]
                if subset["probability"].values[0] > .5:
                    writer.writerow([company_id, company["CompanyName"], subset["homepage_url"].values[0], subset["probability"].values[0]])
                    company_df.at[company_id, "URL"] = subset["homepage_url"]
                else:
                    writer.writerow([company_id, company["CompanyName"], "", ""])
            else:
                writer.writerow([company_id, company["CompanyName"], "", ""])
            counter += 1
        # company_df.join(name_url_dict, how="left", on="CompanyName")
        return company_df


# Convert crunchbase API response to dictionary {company name : website url}
def name_to_url(data):
    name_url_dict = {}
    # print(data)
    count = len(data["entities"])
    # print(data)
    for i in range(count):
        if "website_url" in data["entities"][i]["properties"]:
            name = data["entities"][i]["properties"]["identifier"]["value"]
            name_url_dict[name] = data["entities"][i]["properties"]["website_url"]
    return name_url_dict

# Find the company websites not found
def missing_companies(names, name_url_dict):
    return list(set(names) - set(name_url_dict.keys()))

def missing_websites_df(df):
    df = df.loc[df["URL"].isna()]
    return df

if __name__ == "__main__":
    company_df = missing_websites_df(pd.read_csv("company_websites/name_url/company_df_testing_full_llm.csv")).set_index("CompanyID")
    company_df = company_df[:5]
    print(company_df.head())
    print(company_df.shape)
    # # Get the website urls
    start_time1 = perf_counter()
    company_df = get_website_url_llm(company_df)
    print("Time Taken Getting Website Urls: " + str(perf_counter() - start_time1))
    


load_dotenv()
api_key = os.getenv("CRUNCHBASE_API_KEY")
