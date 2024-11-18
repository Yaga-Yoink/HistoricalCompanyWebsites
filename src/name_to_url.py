import os
import pandas as pd
from time import perf_counter
import csv
import datetime
from llm_request import send_request
import logging

logger = logging.getLogger(__name__)

organizations_data_path = "data/input_data/odm/organizations.csv"

company_df_path = "data/input_data/20240831_vc backed companies_hq.csv"

output_base_dir = "data/name_url"

current_date = datetime.datetime.now().strftime("%m_%d_%H_%M_%S")


# Search for corresponding names from list 'company_names' and their website urls
def get_website_url(company_names):
    result_dict = {}
    curr_index = 0
    company_count = 200
    if len(company_names) > 1000:
        while curr_index + company_count < len(company_names):
            response = send_request(
                company_names_slice=company_names[
                    curr_index : curr_index + company_count
                ]
            )
            result_dict.update(name_to_url(response))
            curr_index += company_count
    response = send_request(company_names_slice=company_names[curr_index:])
    result_dict.update(name_to_url(response))
    return result_dict


# Return a new dataframe containing the companies from 'organizations_df' with the closest matching names to 'company'
def organization_subset(company, organizations_df):
    subset = organizations_df[organizations_df["name"] == company["CompanyName"]]
    if subset.shape[0] == 0:
        split_company_names = (
            company["CompanyName"]
            .replace("(", "")
            .replace(")", "")
            .replace("-", " ")
            .replace(".", " ")
        )
        split_company_names = split_company_names.split()
        subset = organizations_df.loc[
            organizations_df["name"].str.contains(split_company_names[0], na=False)
        ]
        end_index = 1
        while subset.shape[0] != 0 and end_index <= len(split_company_names):
            search_string = "".join(
                [f".*{word}" for word in split_company_names[0:end_index]]
            )
            test_subset = organizations_df.loc[
                organizations_df["name"].str.contains(search_string, na=False)
            ]
            if test_subset.shape[0] > 0:
                subset = test_subset
            else:
                return subset.iloc[:]
            end_index += 1
    return subset


# Returns the formatted 'organizations_df'.
def setup_organizations_df():
    organizations_df = pd.read_csv(organizations_data_path)
    organizations_df["probability"] = 0
    return organizations_df


# Search for the website urls of the names from the company names in "CompanyName" columnn of "company_df". Returns "company_df" with a the "URL" column filled as much as possible.
def get_website_url_llm(company_df):
    os.makedirs(f"{output_base_dir}", exist_ok=True)
    with open(
        f"{output_base_dir}/name_url_{current_date}.csv",
        "w",
    ) as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["CompanyID", "CompanyName", "URL", "probability"])

        def helper_func(row, company):
            row["probability"] = float(
                send_request(
                    f"Act as a statistician that only returns one floating point value. Only return one floating point value, not text allowed.",
                    f"Give me the probability that the following descriptions are related to eachother or cryptpo/finance/business in general: ({company["Description"]}) : ({row["short_description"]})",
                )
            )
            return row

        organizations_df = setup_organizations_df()
        counter = 0
        for company_id, company in company_df.iterrows():
            logger.info(f"Processing {company["CompanyName"]} : URL Number {counter}")
            subset = organization_subset(company, organizations_df)
            # Added a max of 100 possibiltiies to check
            if subset.shape[0] > 1 and subset.shape[0] < 100:
                subset = subset.transform(lambda row: helper_func(row, company), axis=1)
                subset["probability"] = pd.to_numeric(subset["probability"])
                max_index_label = subset["probability"].idxmax()
                subset = subset.loc[max_index_label]
                if subset["probability"] > 0.5:
                    writer.writerow(
                        [
                            company_id,
                            company["CompanyName"],
                            subset["homepage_url"],
                            subset["probability"],
                        ]
                    )
                    company_df.at[company_id, "URL"] = subset["homepage_url"]
                else:
                    writer.writerow([company_id, company["CompanyName"], "", ""])
            elif subset.shape[0] == 1:
                subset = subset.transform(lambda row: helper_func(row, company), axis=1)
                if subset["probability"].values[0] > 0.5:
                    writer.writerow(
                        [
                            company_id,
                            company["CompanyName"],
                            subset["homepage_url"].values[0],
                            subset["probability"].values[0],
                        ]
                    )
                    company_df.at[company_id, "URL"] = subset["homepage_url"]
                else:
                    writer.writerow([company_id, company["CompanyName"], "", ""])
            else:
                writer.writerow([company_id, company["CompanyName"], "", ""])
            counter += 1
        return company_df


# Convert crunchbase API response 'data' to dictionary {company name : website url}
def name_to_url(data):
    name_url_dict = {}
    count = len(data["entities"])
    for i in range(count):
        if "website_url" in data["entities"][i]["properties"]:
            name = data["entities"][i]["properties"]["identifier"]["value"]
            name_url_dict[name] = data["entities"][i]["properties"]["website_url"]
    return name_url_dict


# Setup the logger by initializing the log directory if not made and create the new log file
def init_log():
    os.makedirs("src/logs/name_url", exist_ok=True)
    logging.basicConfig(
        filename=f"src/logs/name_url/name_url_{current_date}.log",
        encoding="utf-8",
        level=logging.DEBUG,
    )


# Setup the initial company df
def setup_company_df():
    company_df = pd.read_csv(company_df_path).set_index("CompanyID")
    company_df["URL"] = ""
    return company_df


if __name__ == "__main__":
    init_log()
    company_df = setup_company_df()
    start_time1 = perf_counter()
    # Common error result = op.transform(), caused when lmstudio isn't activated and llm request isn't returning probabilities (make sure lmstudio is running and model is laoded)
    company_df = get_website_url_llm(company_df)
    logger.info(f"Time Taken Getting Website Urls {str(perf_counter() - start_time1)}")
