# Website Version Scraper

This project has two main pieces of functionality.
    1. Find the URL of a company website based on its name
    2. Gather multiple versions of the company webpage from differing time periods


## Table of Contents

- [File Structure](#file_structure)

## File Structure

    .
    ├── data
    │   ├── input_data
    │       ├──
    │   ├── historical_versions
    │   ├── website_text
    │   ├── name_url
    ├── src                     
    │   ├── async_historical_information.py
    │   ├── name_to_url.py
    │   ├── llm_request.py
    │   └── ...                 
    └── README.md

### name_to_url.py

This file takes in a CSV containing company ids, company names, and a short description of the company. Then it matches the company names using a one or none pattern matching between the company name and the companies in a CSV file downloaded from the CrunchBase Open Data Map. (keeping the Open Data Map (ODM) in memory significantly reduced processing time by eliminating API calls in addition to API restrictions). 
The matching process is a combination of simple string matching as well as consulting an LLM to receieve a probability that the company name corresponds to the company name in the ODM based on comparing the descriptions between the two sources of data. If the probability is above a certain threshold, then the URL is accepted. 
This repeats for every company and exports the new CSV to the name_url directory. 

### async_historical_information.py

This file takes in the outputted CSV from name_to_url.py and saves the historical versions of the webpage. 
This program outputs n historical versions of the company webpages and related texts by first calling the WaybackMachine cdx server to get a list of the timestamps that the company website has been saved. Then for each of the timestamps the webpage text is scraped and saved in addition to a limited number of the webpage URLs that were included on the original homepage text. 
Since this program is highly parallelized to reduce the processing time, there are rate limiters to control the number of parallel requests to the WaybackMachine. 
The company webpage text and related webpages are saved to individual files in the website_text directory. Additionally, the pathnames for the files in the website_text directory are included in the output CSV in historical_versions which includes every field from the input CSV in addition to each of the file paths to the webpage text and the associated timestamp with the webpage text. 