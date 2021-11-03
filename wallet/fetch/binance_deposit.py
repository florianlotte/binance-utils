import time
from datetime import datetime
from decimal import Decimal

from binance import Client
from loguru import logger

from config.base import DATABASE_URL, API_SECRET, API_KEY
from model.base import Deposit, init_db


def get_deposit_from_coin(d):
    return Deposit(
        id=d['txId'],
        time=datetime.utcfromtimestamp(d['insertTime'] / 1000),
        asset=d['coin'],
        amount=Decimal(d['amount']),
        network=d['network'],
        address=d['address']
    )


def get_deposit_from_fiat(d):
    return Deposit(
        id=d['orderNo'],
        time=datetime.utcfromtimestamp(d['createTime'] / 1000),
        asset=d['fiatCurrency'],
        amount=float(d['amount']),
        network='FIAT'
    )


def coin_deposit(client, session):
    start_time = datetime(year=2020, month=1, day=1)
    logger.debug(f"start {int(start_time.timestamp())}")
    now = datetime.now()
    logger.debug(f"now {int(now.timestamp())}")
    step = 90 * 24 * 60 * 60
    for t in range(int(start_time.timestamp()), int(now.timestamp()), step):
        logger.debug(f"range from {t} to {t + step}")
        deposits = client.get_deposit_history(
            startTime=t * 1000,
            endTime=(t + step) * 1000
        )
        for d in deposits:
            logger.debug(d)
            deposit = get_deposit_from_coin(d)
            logger.info(deposit)
            session.merge(deposit)
        session.commit()
        time.sleep(0.5)


def fiat_deposit(client, session):
    start_time = datetime(year=2020, month=1, day=1)
    logger.debug(f"start {int(start_time.timestamp())}")
    deposits = client.get_fiat_deposit_withdraw_history(
        transactionType=0,
        beginTime=int(start_time.timestamp()) * 1000
    )
    for d in deposits['data']:
        logger.debug(d)
        deposit = get_deposit_from_fiat(d)
        logger.info(deposit)
        session.merge(deposit)
    session.commit()


def main(client, session):
    coin_deposit(client, session)
    fiat_deposit(client, session)


if __name__ == "__main__":
    _session = init_db(DATABASE_URL)
    _client = Client(API_KEY, API_SECRET)
    main(_client, _session)
