# '''
# @Name :Nithya
# @email : nss9899@nyu.edu
# '''

import collections
import datetime
import time
import csv
from math import sqrt
from polygon import RESTClient
from sqlalchemy import create_engine
from sqlalchemy import text

class PolygonApi:

    # Initializing class with api key and initializing sql local data base
    def __init__(self):
        self.currency_pairs = [["AUD", "USD"],
                               ["GBP", "EUR"],
                               ["USD", "CAD"],
                               ["USD", "JPY"],
                               ["USD", "MXN"],
                               ["EUR", "USD"],
                               ["USD", "CNY"],
                               ["USD", "CZK"],
                               ["USD", "PLN"],
                               ["USD", "INR"]
                               ]
        self.key = "beBybSi8daPgsTp5yx5cHtHpYcrjp5Jq"
        self.engine = create_engine("sqlite+pysqlite:///sqlite/final.db", echo=False, future=True)


    # Function modified from polygon  code to format the date string
    def ts_to_datetime(self, ts) -> str:
        return datetime.datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

    # Function which clears the raw data tables once we have aggregated the data in a 6 minute interval
    def reset_raw_data_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                conn.execute(text("DROP TABLE " + curr[0] + curr[1] + "_raw;"))
                conn.execute(text(
                    "CREATE TABLE " + curr[0] + curr[1] + "_raw(ticktime text, fxrate  numeric, inserttime text);"))

    # This creates a table for storing the raw, unaggregated price data for each currency pair in the SQLite database
    def initialize_raw_data_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                conn.execute(text(
                    "CREATE TABLE " + curr[0] + curr[1] + "_raw(ticktime text, fxrate  numeric, inserttime text);"))

    # This creates a table for storing the (6 min interval) aggregated price data for each currency pair in the SQLite database
    def initialize_aggregated_tables(self):
        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                conn.execute(text("CREATE TABLE " + curr[0] + curr[1] + "_agg(inserttime text, avgfxrate  numeric, stdfxrate numeric);"))
    
    # We will call this funtion every 6 minutes to find the upper and lower bounds usingolality and the mean value 
    def calculate_keltner(self, vol_value, avg_value):
        upper_value = []
        lower_value = []

        for i in range(100):
            upper_value.append(avg_value + (i + 1) * 0.025 * vol_value)
            lower_value.append(avg_value - (i + 1) * 0.025 * vol_value)
        
        return upper_value, lower_value


    def aggregate_raw_data_tables(self):
        
        low_bound = collections.defaultdict(list)
        upper_bound = collections.defaultdict(list)

        with self.engine.begin() as conn:
            for curr in self.currency_pairs:
                result = conn.execute(text("SELECT AVG(fxrate) as avg_price, MAX(fxrate) as max_price, MIN(fxrate) as min_price FROM " + curr[0] + curr[1] + "_raw;"))

                #getting avg, max, min for every curr in 6 minutes
                result_db = []
                for row in result:
                    result_db.append(row.avg_price)
                    result_db.append(row.min_price)
                    result_db.append(row.max_price)
                    result_db.append(row.max_price - row.min_price)

                volality_value = result_db[3]
                mean_value = result_db[0]

                #Getting upper and lower bounds every 6 minutes
                upper_bounds, lower_bounds = self.calculate_keltner(volality_value, mean_value)

                #to get data in the list
                key_value = curr[0] + curr[1]
                low_bound[key_value] = lower_bounds
                upper_bound[key_value] = upper_bounds

        return low_bound, upper_bound


    def findingFD(self, lower_bounds, upper_bounds):

        #connecting the engine
        with self.engine.begin() as conn:
            file = open('output.csv', 'w', newline='')
            header = ['Min', 'Max', 'Mean', "Vol", "FD",'key']
            action = csv.DictWriter(file, fieldnames=header)
            action.writerow({"Min": "Min" , "Max": "Max", "Mean": "Mean", "Vol": "Vol", "FD": "fd","key": "key"})

            for curr in self.currency_pairs:
                key = curr[0] + curr[1]
                result = conn.execute(text("SELECT fxrate from " + key + "_raw;"))
                result_stat = conn.execute(text("SELECT AVG(fxrate) as avg_price, MAX(fxrate) as max_price, MIN(fxrate) as min_price FROM " + key + "_raw;"))

                # for every bound, check how many data points will cross it
                for i in range(100):
                    count = 0
                    for row in result:
                        if upper_bounds[key][i] <= row.fxrate or lower_bounds[key][i] >= row.fxrate:
                            count += 1
                    for row in result_stat:
                        max_price = row.max_price
                        avg_price = row.avg_price
                        min_price = row.min_price
                        volatility = row.max_price - row.min_price
                        fd = count
                        if volatility != 0:
                            fd = count/volatility

                    # writing data into csv file
                    action.writerow({"Min": min_price, "Max": max_price, "Mean": avg_price, "Vol": volatility, "FD": fd , "key" : key })


    def collectData(self):
        file = open('output.csv', 'w', newline='')
        header = ['Min', 'Max', 'Mean', "Vol", "FD",'key']
        writer = csv.DictWriter(file, fieldnames=header)
        writer.writeheader()

        # Number of list iterations - each one should last about 1 second
        count_val = 0
        count = 0
        agg_count = 0
        temp = 0
        # Create the needed tables in the database
        self.initialize_raw_data_tables()
        self.initialize_aggregated_tables()
        # Open a RESTClient for making the api calls
        client = RESTClient(self.key)
        # Loop that runs until the total duration of the program hits 24 hours.
        prev_low, prev_up = [], []
        
        while count < 86400:  # 86400 seconds = 24 hours
            # Make a check to see if 6 minutes has been reached or not
            if agg_count == 10:

                # resetting the counter every 6mins
                agg_count = 0

                # getting previous bounds values
                lower_bounds, upper_bounds = self.aggregate_raw_data_tables()
                
                # when count_val is 0, values are not considered in the 1st iteration
                # when count_val is more than 1 , we calulate using previous data points

                if count_val == 0:
                    prev_low = lower_bounds
                    prev_up = upper_bounds
                    self.reset_raw_data_tables()
                else :
                    self.findingFD(prev_low, prev_up)
                    prev_low = lower_bounds
                    prev_up = upper_bounds
                    self.reset_raw_data_tables()

                count_val = count_val + 1

            # Only call the api every 1 second, so wait here for 0.75 seconds, because the code takes about .15 seconds to run
            time.sleep(0.75)
            count += 1
            agg_count += 1
            
            # Loop through each currency pair
            for currency in self.currency_pairs:
                # Set the input variables to the API
                from_ = currency[0]
                to = currency[1]
                # Call the API with the required parameters
                try:
                    resp = client.get_real_time_currency_conversion(from_, to, amount=100, precision=2)
                except:
                    continue
                # This gets the Last Trade object defined in the API Resource
                last_trade = resp.last
                # print(type(last_trade), last_trade.timestamp)
                # Format the timestamp from the result
                dt = self.ts_to_datetime(last_trade.timestamp)
                # Get the current time and format it
                insert_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # Calculate the price by taking the average of the bid and ask prices
                avg_price = (last_trade.bid + last_trade.ask) / 2
                # Write the data to the SQLite database, raw data tables
                with self.engine.begin() as conn:
                    conn.execute(text(
                        "INSERT INTO " + from_ + to + "_raw(ticktime, fxrate, inserttime) VALUES (:ticktime, :fxrate, :inserttime)"),
                                 [{"ticktime": dt, "fxrate": avg_price, "inserttime": insert_time}])
