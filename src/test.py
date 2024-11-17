# from lingua import LanguageDetectorBuilder
# import time

# file = open("/Users/ryanfujii/FA_24_research/small_task/company_websites/website_text/10_01_20_23_12/Shangya Currency/Shangya Currency_20141218195624.txt", "r")
# text = file.read()

# start_time = time.perf_counter()
# start_2 = time.perf_counter()
# detector = LanguageDetectorBuilder.from_all_languages().build()
# print("Time Spent Making Detector: ", float(time.perf_counter() - start_2))
# language = detector.detect_language_of(text)
# print("Time Taken Detecting Language of One Website: ", float(time.perf_counter() - start_time))
# print(language)


# import pandas as pd
# data = {'index': [('a', 'b'), ('a', 'c')],
#         'columns': [('x', 1), ('y', 2)],
#         'data': [[1, 3], [2, 4]],
#         'index_names': ['n1', 'n2'],
#         'column_names': ['z1', 'z2']}
# print(pd.DataFrame.from_dict(data, orient='tight'))

# Script to add descriptions to name_url
# import pandas as pd

# name_url_df = (pd.read_csv("company_websites/name_url/name_url_10_07_02_49_38.csv")).set_index("CompanyID")
# company_df = pd.read_csv("company_websites/20240831_vc backed companies_hq.csv").set_index('CompanyID')
# company_df = company_df[["Description"]]
# print(company_df.head())
# # result_df = pd.concat([name_url_df, company_df])
# result_df = name_url_df.join(company_df, how="outer")
# result_df.to_csv("text_ready_name_url_10_07_02_49_38.csv")


# files  = [{'100102-51': ['company_websites/website_text/10_15_21_42_25/DigiByte/DigiByte_20230508042621.txt', 'company_websites/website_text/10_15_21_42_25/DigiByte/DigiByte_20240616025227.txt']}]
# # print(list(files[0].keys())[0])
# # print(iter(files[0]))
# print(next(iter(files[0])))


# for index, filepath in enumerate(files[0]['100102-51']):
#     print(filepath)
    
# import pandas as pd
    
# company_df = pd.read_csv("test_async_historical.csv")
# company_df.to_csv(f"company_websites/historical_versions/test_async_historical_.csv", index=False)

# import re

# print(re.sub("/", "_", "MultiChain (Business/Productivity Software)"))

# import pandas as pd

# company_df = (pd.read_csv("text_ready_name_url_10_07_02_49_38.csv")).set_index("CompanyID")
# print(company_df)

# import csv

# reader = csv.DictReader("text_ready_name_url_10_07_02_49_38.csv")
# for row in reader:
#     print(row)
import os
import pandas as pd
# date = "example_date"

# def create_output_csv(n):
#     os.makedirs("historical_versions", exist_ok=True)
#     headers = ["CompanyName","URL","probability","Description"]
#     for i in range(1, 11, 1):
#         headers.append(f"text_version_{i}")
#         headers.append(f"text_version_{i}_similarity_score")        
#     headers = ",".join(headers)
#     with open(f"company_websites/historical_versions/test_async_historical_{date}.csv", "a+") as text_file:
#        text_file.write(headers)
#        text_file.write("\n")

# create_output_csv(10)

# data = {'CompanyName': '[LibertyX]',
#  'URL': '[https://libertyx.com/]',
#  'probability': '[0.999997]',
#  'Description': "[Developer of a financial software platform designed to help consumers buy and sell bitcoin in-person or via mobile application. The company's platform provides cashiers and virtual automatic teller machines to purchase bitcoin at their local stores' checkout, enabling users to easily buy bitcoin online or offline from any location.]",
#  'text_version_1': '[company_websites/website_text/10_16_20_31_14/LibertyX/LibertyX_20240713161926.txt]',
#  'text_version_1_similarity_score': '[0.43115934331983397]',
#  'text_version_2': '[company_websites/website_text/10_16_20_31_14/LibertyX/LibertyX_20240713170351.txt]',
#  'text_version_2_similarity_score': '[0.43124752933400595]',
#  'text_version_3': '[company_websites/website_text/10_16_20_31_14/LibertyX/LibertyX_20240809024937.txt]',
#  'text_version_3_similarity_score': '[0.43131241342232873]',
#  'text_version_4': '[company_websites/website_text/10_16_20_31_14/LibertyX/LibertyX_20240830003650.txt]',
#  'text_version_4_similarity_score': '[0.43489493467242235]',
#  'text_version_5': '[company_websites/website_text/10_16_20_31_14/LibertyX/LibertyX_20240901110535.txt]',
#  'text_version_5_similarity_score': '[0.4351317222277744]',
#  'text_version_6': '[company_websites/website_text/10_16_20_31_14/LibertyX/LibertyX_20240907004245.txt]',
#  'text_version_6_similarity_score': '[0.43468885033577287]',
#  'text_version_7': '[company_websites/website_text/10_16_20_31_14/LibertyX/LibertyX_20240908171106.txt]',
#  'text_version_7_similarity_score': '[0.4348393386114345]',
#  'text_version_8': '[company_websites/website_text/10_16_20_31_14/LibertyX/LibertyX_20240924041027.txt]',
#  'text_version_8_similarity_score': '[0.4347656017645243]',
#  'text_version_9': '[company_websites/website_text/10_16_20_31_14/LibertyX/LibertyX_20241006065055.txt]',
#  'text_version_9_similarity_score': '[0.4322250245930109]',
#  'text_version_10': '[company_websites/website_text/10_16_20_31_14/LibertyX/LibertyX_20241010004842.txt]',
#  'text_version_10_similarity_score': '[0.4324139742744152]'}
# data_with_outer_brackets = {key: [value] for key, value in data.items()}
# df = pd.DataFrame.from_dict(data_with_outer_brackets).set_index("CompanyName")
# print(df)
# df.to_csv(f"company_websites/historical_versions/test_async_historical_{date}.csv", mode='a', index = True, header=False)


# company_df = pd.DataFrame.from_dict({"CompanyID" : ['123']})
# headers = ["CompanyName", "URL", "probability", "Description"]
# for i in range(1, 11, 1):
#     headers.append(f"text_version_{i}")
#     headers.append(f"text_version_{i}_similarity_score")
# for header in headers:
#     if header not in list(company_df.columns.values):
#         company_df[header] = ""
# company_df = company_df.set_index("CompanyID")
# print(company_df)


# linked_pages = ["asdf"]
# if len(linked_pages) != 0:
#     print("here")
#     num_linked_pages = len(linked_pages)
#     character_limit = 100000
#     per_page_limit = character_limit // num_linked_pages

# from memory_profiler import profile
# from sklearn.feature_extraction.text import TfidfVectorizer

# @profile
# def main():
#     company_description = "Operator of a payment network intended to provide the network infrastructure, security, and communications to function with cutting-edge speed. The company's network offers a blockchain-based platform through its digital packages that cannot be destroyed, counterfeited, or hacked, enabling users to protect objects of value like currency, information, property, or important digital data."
#     with open("company_websites/website_text/10_16_20_31_14/LibertyX/LibertyX_20241010004842.txt", "r") as text:
#         text = text.read()
#         documents = [text, company_description]
#         tfidf = TfidfVectorizer().fit_transform(documents)
#         # no need to normalize, since Vectorizer will return normalized tf-idf
#         similarity_score = tfidf * tfidf.T


# main()

import pandas as pd

# print(list(pd.Series([1, 2, 3]).index))

from urllib.parse import urlparse

# "https://web.archive.org/web/20240826000455/https://www.youtube.com/pdaxph"
# insta = "https://web.archive.org/web/20240205055224/http://instagram.com/JourneyProtect"

# print(urlparse(insta).netloc)

# company_df = pd.read_csv(
#         "text_ready_name_url_10_07_02_49_38.csv"
#     )

# print(company_df["URL"].count())
from requests_html import AsyncHTMLSession
import asyncio
from bs4 import BeautifulSoup

async def test():
    session = AsyncHTMLSession(workers=10)
    r = await session.get(
                    f"https://web.archive.org/cdx/search/cdx",
                    params={
                        "url": "http://digibyte.co/",
                        "output": "json",
                        "fl": ["timestamp"],
                        "limit": -10,
                        "filter": ["statuscode:200", "!mimetype:application/octet-stream"],
                    },
                )
    print(r.json())
    text = await session.get("https://web.archive.org/web/20240616025227/http://digibyte.co/", timeout = 30)
    soup = BeautifulSoup(text.content, "html.parser")
    page_text = " ".join(soup.text.split())
    print(page_text)

asyncio.run(test())