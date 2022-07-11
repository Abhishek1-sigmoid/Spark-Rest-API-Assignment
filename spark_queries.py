from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('Stock Project').getOrCreate()
spark_df = spark.read.csv('stock_csv_dataset/*.csv', sep=',', header=True)
spark_df = spark_df.drop('_c0')
spark_df = spark_df.drop_duplicates()
spark_df = spark_df.dropna()
spark_df.createTempView('stock_table')

# query 1

def max_diff_stock_percent():
    try:
        query = """
            Select stocks.company, stocks.date, stocks.max_diff_stock_percent from (Select date,
            company,(high-open)/open*100 as max_diff_stock_percent, dense_rank() 
            OVER (partition by date order by ( high-open)/open desc ) as dense_rank FROM stock_table)stocks where
            stocks.dense_rank=1
        """
        data = spark.sql(query).collect()
        results = {}
        for row in data:
            results[row['date']] = {'company': row['company'], 'max_diff_stock_percent': row['max_diff_stock_percent']}
        return results
    except Exception as e:
        return {'Error': e}

# query 2

def most_traded_stock_per_day():
    try:
        query = """
            Select stock.company, stock.date, stock.volume from 
            (Select date, company, volume,dense_rank() over (partition by date order by volume desc) as 
            dense_rank from stock_table)stock
            where stock.dense_rank=1
        """
        data = spark.sql(query).collect()
        results = {}
        for row in data:
            results[row['date']] = {'company': row['company'], 'date': row['date'], 'volume': row['volume']}
        return results
    except Exception as e:
        return {'Error': e}

# query 3

def max_stock_gap():
    try:
        query = """
            Select stocks.company,abs(stocks.previous_close-stocks.open) as max_gap from 
            (Select company, open, date, close, lag(close,1,35.724998) over(partition by company order by date) as
            previous_close from stock_table asc)stocks order by max_gap desc limit 1
        """
        data = spark.sql(query).collect()
        results = {}
        for row in data:
            results['company'] = row['company']
            results['max_gap'] = row['max_gap']
        return results
    except Exception as e:
        return {'Error': e}

# query 4

def max_moved_stock():
    try:
        query = """
        with df1 as (select company, open from (select company, open, dense_rank() over (partition by company order by date) 
        as d_rank1 from stock_table)stock_table_1 where stock_table_1.d_rank1=1),df2 as (select company, close from 
        (select company, close, dense_rank() over (partition by company order by date desc) as d_rank2 from stock_table)stock_table_2 
        where stock_table_2.d_rank2 = 1) select df1.company, df1.open, df2.close, df1.open-df2.close as max_diff 
        from df1 inner join df2 where df1.company = df2.company
        order by max_diff DESC limit 1
        """
        data = spark.sql(query).collect()
        results = {}
        for row in data:
            results['company'] = row['company']
            results['open'] = row['open']
            results['close'] = row['close']
            results['max_diff'] = row['max_diff']
        return results
    except Exception as e:
        return {'Error': e}

# query 5

def standard_deviation():
    try:
        query = """
            select Company, stddev_samp(Volume) as Standard_Deviation from stock_table group by Company
        """
        data = spark.sql(query).collect()
        data = dict(data)
        results = []
        for key, val in data.items():
            results.append({'Company': key, 'Standard_Deviation': val})
        return results
    except Exception as e:
        return {'Error': e}

# query 6

def mean_median_stock_price():
    try:
        query = """
            Select company, avg(Close) as mean, percentile_approx(Close,0.5) as median from stock_table group by company
        """
        data = spark.sql(query).collect()
        results = []
        for row in data:
            results.append({'company': row['company'], 'mean': row['mean'], 'median': row['median']})
        return results
    except Exception as e:
        return {'Error': e}

# query 7

def average_volume():
    try:
        query = """
            select Company, AVG(Volume) as Average_Volume from stock_table group by Company order by Average_Volume desc
        """
        data = spark.sql(query).collect()
        data = dict(data)
        results = []
        for key, val in data.items():
            results.append({'Company': key, 'Average_Volume': val})
        return results
    except Exception as e:
        return {'Error': e}

# query 8

def max_average_volume():
    try:
        query = """
            select Company, AVG(Volume) as Average_Volume from stock_table group by Company order by Average_Volume desc limit 1
        """
        data = spark.sql(query).collect()
        data = dict(data)
        results = []
        for key, val in data.items():
            results.append({'company': key, 'max_average_volume': val})
        return results
    except Exception as e:
        return {'Error': e}

# query 9

def highest_lowest_stock_price():
    try:
        query = """
            select Company, MAX(high) as Highest_Price, MIN(low) as Lowest_Price from stock_table group by Company
        """
        data = spark.sql(query).collect()
        results = []
        for row in data:
            results.append({'company': row['Company'], 'highest_price': row['Highest_Price'], 'lowest_price': row['Lowest_Price']})
        return results
    except Exception as e:
        return {'Error': e}
