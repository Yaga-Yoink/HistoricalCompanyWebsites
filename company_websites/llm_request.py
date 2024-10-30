import requests
import json
import pandas as pd
# from pandas_nql import PandasNQL
from dotenv import load_dotenv
import os
import json

# Send OpenAPI like post request to local llm server. Returns the text response from llm.
def send_request(role, message):
    url = "http://localhost:1234/v1/chat/completions"
    # print("here")
    # Your request payload
    payload = {
        "messages": [
            { "role": "system", "content": role },
            { "role": "user", "content": message }
        ],
        "temperature": 0.7,
        "max_tokens": -1,
        "stream": False
    }

    headers = {
        "Content-Type": "application/json"
    }

    # Send POST request
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    # print('llm_response', response.text)
    return response.json()["choices"][0]["message"]["content"]
# print(send_request("Give me the probability that the following text description is describing something related to finance, crypto, or venture capital: Bankaholic is a financial portal provides consumers with interest rates, credit card reviews, insurance quotes, and personal finance tips."))

