from name_to_url import missing_companies
import pandas as pd

llm_version = pd.read_csv("company_websites/name_url/name_url_10_05_18_14_19.csv")
origin_version = pd.read_csv("company_websites/name_url/company_df_testing_full.csv")
input_row_count = llm_version.shape[0]

llm_version = llm_version.dropna()
origin_version = origin_version.dropna()

llm_url_count = llm_version.shape[0]
origin_version = origin_version.shape[0]

print(f"LLM found {llm_url_count} URLS.")
print(f"Original found {origin_version} URLS.")
print(f"LLM Improvement =  {(llm_url_count / origin_version) * 100} URLS.")
print(f"Total Inputted Companies = {input_row_count}")
