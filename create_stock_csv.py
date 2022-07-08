from get_company_ticket_symbol import get_company_ticket_symbol
from historic_data import get_historic_data


def create_stock_csv():
    try:
        ticket_symbols = get_company_ticket_symbol()
        for ticket in ticket_symbols:
            get_historic_data(ticket)
    except Exception as e:
        print(e)


create_stock_csv()
