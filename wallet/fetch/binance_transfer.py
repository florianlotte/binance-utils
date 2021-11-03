import time
from datetime import datetime

from binance import Client
from loguru import logger

from config.base import DATABASE_URL, API_KEY, API_SECRET
from model.base import init_db, Transfer

transfer_types = [
    'MAIN_C2C',
    'MAIN_UMFUTURE',
    'MAIN_CMFUTURE',
    'MAIN_MARGIN',
    'MAIN_MINING',
    'C2C_MAIN',
    'C2C_UMFUTURE',
    'C2C_MINING',
    'C2C_MARGIN',
    'UMFUTURE_MAIN',
    'UMFUTURE_C2C',
    'UMFUTURE_MARGIN',
    'CMFUTURE_MAIN',
    'CMFUTURE_MARGIN',
    'MARGIN_MAIN',
    'MARGIN_UMFUTURE',
    'MARGIN_CMFUTURE',
    'MARGIN_MINING',
    'MARGIN_C2C',
    'MINING_MAIN',
    'MINING_UMFUTURE',
    'MINING_C2C',
    'MINING_MARGIN',
    'MAIN_PAY',
    'PAY_MAIN',
    # 'ISOLATEDMARGIN_MARGIN',
    # 'MARGIN_ISOLATEDMARGIN',
    # 'ISOLATEDMARGIN_ISOLATEDMARGIN'
]


def get_transfer(t):
    transfer_type = t['type'].split('_')
    assert len(transfer_type) == 2
    return Transfer(
        id=int(t['tranId']),
        time=datetime.utcfromtimestamp(t['timestamp'] / 1000),
        asset=t['asset'],
        amount=float(t['amount']),
        from_account=transfer_type[0],
        to_account=transfer_type[1]
    )


def tranfer(client, session):
    start_time = datetime(year=2020, month=1, day=1)
    logger.debug(f"start {int(start_time.timestamp())}")
    for tt in transfer_types:
        logger.info(f"Transfer type: {tt}")
        transfer = client.query_universal_transfer_history(
            type=tt
        )
        if transfer['total'] > 0:
            logger.debug(transfer)
            for t in transfer['rows']:
                db_transfer = get_transfer(t)
                logger.info(db_transfer)
                session.merge(db_transfer)
        session.commit()
        time.sleep(0.5)


def main(client, session):
    tranfer(client, session)


if __name__ == "__main__":
    _session = init_db(DATABASE_URL)
    _client = Client(API_KEY, API_SECRET)
    main(_client, _session)
