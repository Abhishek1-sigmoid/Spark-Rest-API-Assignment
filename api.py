from flask import Flask, jsonify
from spark_queries import *

app = Flask('Stock Project')

# query 1
@app.route('/api/max_diff_stock_percent')
def api_max_diff_stock_percent():
    data = max_diff_stock_percent()
    return jsonify(data)

# query 2
@app.route('/api/most_traded_stock_per_day')
def api_most_traded_stock_per_day():
    data = most_traded_stock_per_day()
    return jsonify(data)

# query 3
@app.route('/api/max_stock_gap')
def api_max_stock_gap():
    data = max_stock_gap()
    return jsonify(data)

# query 4
@app.route('/api/max_moved_stock')
def api_max_moved_stock():
    data = max_moved_stock()
    return jsonify(data)

# query 5
@app.route('/api/standard_deviation')
def api_standard_deviation():
    data = standard_deviation()
    return jsonify(data)

# query 6
@app.route('/api/mean_median_stock_price')
def api_mean_median_stock_price():
    data = mean_median_stock_price()
    return jsonify(data)

# query 7
@app.route('/api/average_volume')
def api_average_volume():
    data = average_volume()
    return jsonify(data)

# query 8
@app.route('/api/max_average_volume')
def api_max_average_volume():
    data = max_average_volume()
    return jsonify(data)

# query 9
@app.route('/api/highest_lowest_stock_price')
def api_highest_lowest_stock_price():
    data = highest_lowest_stock_price()
    return jsonify(data)


if __name__ == '__main__':
    app.run()
