import json
import requests
import pandas as pd

def change_time_format(time):
    return time[:10]


def get_historic_data(ticket_symbol):
    try:
        url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"
        querystring = {"ticker_symbol": ticket_symbol, "years": "1", "format": "json"}
        headers = {
            "X-RapidAPI-Key": "1891359635msh632422f9655aaa5p156337jsn80881f698847",
            "X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
        }
        response = requests.request("GET", url, headers=headers, params=querystring)
        response = json.loads(response.text)
        response = response['historical prices']
        df = pd.DataFrame(response)
        date = df['Date'].apply(change_time_format)
        df['Date'] = date
        df['Company'] = ticket_symbol
        file_name = 'stock_csv_dataset/' + str(ticket_symbol) + '.csv'
        df.to_csv(file_name)
    except Exception as e:
        print(e)
