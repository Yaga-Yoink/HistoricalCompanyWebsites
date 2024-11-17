import requests
import json

# Send OpenAPI like post request to local llm server. Returns the text response from llm.
def send_request(role, message):
    url = "http://localhost:1234/v1/chat/completions"
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
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    return response.json()["choices"][0]["message"]["content"]
