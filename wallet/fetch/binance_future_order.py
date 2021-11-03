import time
from decimal import Decimal

from binance import Client
from datetime import datetime
from loguru import logger

from config.base import DATABASE_URL, API_KEY, API_SECRET
from model.base import init_db, FutureOrder


def get_order(o):
    return FutureOrder(
        symbol=o['symbol'],
        id=o['orderId'],
        status=o['status'],
        time=datetime.utcfromtimestamp(o['time'] / 1000),
        update_time=datetime.utcfromtimestamp(o['updateTime'] / 1000),
        price=Decimal(o['price']),
        average_price=Decimal(o['avgPrice']),
        origin_quantity=Decimal(o['origQty']),
        executed_quantity=Decimal(o['executedQty']),
        side=o['side'],
        position_side=o['positionSide'],
        stop_price=o['stopPrice'],
        type=o['type']
    )


def main(client, session):
    logger.add("file_{time}.log", level="INFO")

    start_time = datetime(year=2021, month=3, day=1)
    logger.debug(f"start {int(start_time.timestamp())}")
    now = datetime.now()
    logger.debug(f"now {int(now.timestamp())}")
    step = 7 * 24 * 60 * 60
    for t in range(int(start_time.timestamp()), int(now.timestamp()), step):
        if (t + step) > int(now.timestamp()):
            end_time = int(now.timestamp())
        else:
            end_time = (t + step)
        orders = client.futures_get_all_orders(
            startTime=t * 1000,
            endTime=end_time * 1000,
            limit=1000
        )
        logger.info(f"{len(orders)} orders")
        for o in orders:
            if o['status'] == 'FILLED':
                logger.debug(o)
                order = get_order(o)
                logger.info(order)
                session.merge(order)
            else:
                logger.warning(f"Status is {o['status']}")
        session.commit()
        time.sleep(0.5)


if __name__ == "__main__":
    _session = init_db(DATABASE_URL)
    _client = Client(API_KEY, API_SECRET)
    main(_client, _session)
