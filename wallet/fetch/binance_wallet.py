from decimal import Decimal

from binance import Client
from loguru import logger

from config.base import API_KEY, API_SECRET, DATABASE_URL
from model.base import init_db, Wallet


def main(client, session):
    account = client.get_account()
    for b in account['balances']:
        logger.info(b)
        wallet = Wallet(
            asset=b['asset'],
            real_balance=Decimal(b['free'])+Decimal(b['locked'])
        )
        session.merge(wallet)
    session.commit()


if __name__ == "__main__":
    _session = init_db(DATABASE_URL)
    _client = Client(API_KEY, API_SECRET)
    main(_client, _session)
