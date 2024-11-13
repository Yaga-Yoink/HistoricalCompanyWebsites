import datetime
import asyncio

# A class for throtting API calls by staying under a specified API rate limit.
class RateLimiter:
    def __init__(self, time_period, number_of_calls):
        self.time_period = time_period  # Time period in seconds
        self.number_of_calls = number_of_calls  # Max API calls allowed in time period
        self.api_call_count = 0
        self.start_time = datetime.datetime.now()
        self.lock = asyncio.Lock()  # For thread safety

    # 
    async def api_limit(self):
        async with self.lock:  # Ensure only one coroutine can modify shared variables at a time
            self.api_call_count += 1
            
            if self.api_call_count >= self.number_of_calls:
                end_time = datetime.datetime.now()

                # Calculate the time that has passed since the start
                time_elapsed = (end_time - self.start_time).seconds

                # Sleep if we haven't completed the time period
                if time_elapsed < self.time_period:
                    # print("Rate limit reached. Sleeping...")
                    await asyncio.sleep(self.time_period - time_elapsed)
                
                # Reset the start time and API call count
                self.start_time = datetime.datetime.now()
                self.api_call_count = 1