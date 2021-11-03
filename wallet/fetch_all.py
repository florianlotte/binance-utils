from binance import Client

from config.base import DATABASE_URL, API_SECRET, API_KEY
from model.base import init_db

from fetch.binance_deposit import main as deposit_main
from fetch.binance_withdraw import main as withdraw_main
from fetch.binance_transfer import main as transfert_main
from fetch.binance_order import main as order_main
from fetch.binance_trade import main as trade_main
from fetch.binance_dust import main as dust_main


def start(client, session):
    deposit_main(client, session)
    withdraw_main(client, session)
    transfert_main(client, session)
    order_main(client, session)
    trade_main(client, session)
    dust_main(client, session)
    # convert is manual


def main(db_url):
    session = init_db(db_url)
    client = Client(API_KEY, API_SECRET)
    start(client, session)


if __name__ == "__main__":
    main(DATABASE_URL)
