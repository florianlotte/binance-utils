from datetime import datetime
from decimal import Decimal

from binance import Client
from loguru import logger

from config.base import DATABASE_URL, API_KEY, API_SECRET
from model.base import init_db, Dust


def get_dust(t):
    dt = t['userAssetDribbletDetails']
    assert len(dt) == 1
    return Dust(
        id=int(t['transId']),
        time=datetime.utcfromtimestamp(t['operateTime'] / 1000),
        base_asset=dt[0]['fromAsset'],
        amount=Decimal(dt[0]['amount']),
        quote_asset='BNB',
        quote_amount=Decimal(dt[0]['transferedAmount']),
        commission_asset='BNB',
        commission_amount=Decimal(dt[0]['serviceChargeAmount']),
    )


def dust(client, session):
    dusts = client.get_dust_log()
    logger.debug(dusts)
    if dusts['total'] > 0:
        for d in dusts['userAssetDribblets']:
            db_transfer = get_dust(d)
            logger.info(db_transfer)
            session.merge(db_transfer)
        session.commit()


def main(client, session):
    dust(client, session)


if __name__ == "__main__":
    _session = init_db(DATABASE_URL)
    _client = Client(API_KEY, API_SECRET)
    main(_client, _session)
