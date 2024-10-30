import pandas as pd
from name_to_url import missing_companies, get_website_url_llm, missing_websites_df
import datetime
from time import perf_counter
# from historical_information import historical_versions_main

name_url_path = 'company_websites/name_url/company_df_testing_full_llm.csv'
output_historical_versions_csv_path = f"company_websites/company_df_historical_llm_{datetime.datetime.now().strftime("%m_%d_%H_%M_%S")}.csv"
company_url_path = f"company_websites/name_url/name_url_{datetime.datetime.now().strftime("%m_%d_%H_%M_%S")}.csv"
input_csv = "company_websites/20240831_vc backed companies_hq.csv"

# Returns the initial company_df with "size" amount of companies
def initial_company_df(size):
    company_df = pd.read_csv(input_csv).set_index('CompanyID')
    company_df = company_df[["CompanyName", "hq", "Description"]]
    company_df["URL"] = ""
    company_df["foreign_language_flag"] = False
    company_df = company_df[:]
    return company_df

company_df = initial_company_df(10)


# company_df = missing_websites_df(pd.read_csv("company_websites/name_url/company_df_testing_full_llm.csv")).set_index("CompanyID")


# TESTING PURPOSES
company_df = company_df[:]
print(company_df.head())
print(company_df.shape)

# # Get the website urls
start_time1 = perf_counter()
company_df = get_website_url_llm(company_df)

print("Time Taken Getting Website Urls: " + str(perf_counter() - start_time1))

# # TODO: should probably fill NaN values
# company_df.to_csv(company_url_path)

# missing_companies = missing_companies(website_urls, website_urls_dict)
# # print(website_urls_dict)
# # print(missing_companies)


# company_df = pd.read_csv(name_url_path)
# company_df = company_df[1:2]
# print(company_df)
# # Get the historical data
# company_df = get_website_url_llm(company_df, 10)
# company_df.to_csv(output_historical_versions_csv_path, index=False)
