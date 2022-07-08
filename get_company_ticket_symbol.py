import json
import requests


def get_company_ticket_symbol():
	try:
		url = "https://stock-market-data.p.rapidapi.com/market/index/s-and-p-six-hundred"
		headers = {
			"X-RapidAPI-Key": "1891359635msh632422f9655aaa5p156337jsn80881f698847",
			"X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
		}
		response = requests.request("GET", url, headers=headers)
		response = json.loads(response.text)
		ticket_symbol = response['stocks'][:100]
		return ticket_symbol
	except Exception as e:
		print(e)
