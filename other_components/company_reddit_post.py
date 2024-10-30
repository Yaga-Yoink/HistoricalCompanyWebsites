import praw
from input import crypto_fintech_companies
from utils import iterate_func
import pandas as pd


# Return a dictionary of 'post_count' number of post titles to their urls with 'keyword' in the title
# Requires post_count > 0
def search_reddit_title(keyword, post_count):
    reddit = praw.Reddit("bot1")
    title_url_dict = {keyword: []}
    for submission in reddit.subreddit("all").search(
        query=keyword,
        sort="relevance",
        limit=post_count,
        params={"self": True, "title": keyword},
    ):
        title_url_dict[keyword].append(submission.title)
        # Use for {title : url}
        # title_url_dict[submission.title] = submission.url
    return title_url_dict


if __name__ == "__main__":
    result = iterate_func(search_reddit_title, crypto_fintech_companies, 20)
    # print(pd.DataFrame.from_dict(result))
    pd.DataFrame.from_dict(result).to_csv("data/company_reddit_post.csv")
