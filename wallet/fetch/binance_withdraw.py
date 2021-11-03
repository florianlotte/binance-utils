import time
from datetime import datetime
from decimal import Decimal

from binance import Client
from loguru import logger

from config.base import DATABASE_URL, API_KEY, API_SECRET
from model.base import init_db, Withdraw


def get_withdraw_from_coin(w):
    return Withdraw(
        asset=w['coin'],
        id=w['id'],
        txid=w['txId'],
        amount=Decimal(w['amount']),
        transaction_fee=Decimal(w['transactionFee']),
        address=w['address'],
        time=datetime.strptime(w['applyTime'], '%Y-%m-%d %H:%M:%S'),
        network=w['network'],
        status=w['status']
    )


def get_withdraw_from_fiat(w):
    return None


def coin_withdraw(client, session):
    start_time = datetime(year=2020, month=1, day=1)
    logger.debug(f"start {int(start_time.timestamp())}")
    now = datetime.now()
    logger.debug(f"now {int(now.timestamp())}")
    step = 90 * 24 * 60 * 60
    for t in range(int(start_time.timestamp()), int(now.timestamp()), step):
        logger.debug(f"range from {t} to {t + step}")
        withdraws = client.get_withdraw_history(
            startTime=t * 1000,
            endTime=(t + step) * 1000
        )
        for w in withdraws:
            logger.debug(w)
            deposit = get_withdraw_from_coin(w)
            logger.info(deposit)
            session.merge(deposit)
        session.commit()
        time.sleep(0.5)


def fiat_withdraw(client, session):
    pass


def main(client, session):
    coin_withdraw(client, session)
    fiat_withdraw(client, session)


if __name__ == "__main__":
    _session = init_db(DATABASE_URL)
    _client = Client(API_KEY, API_SECRET)
    main(_client, _session)
