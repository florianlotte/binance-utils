import time
from datetime import datetime
from decimal import Decimal

from binance import Client
from loguru import logger

from config.base import DATABASE_URL, API_KEY, API_SECRET
from model.base import init_db, Order


def get_order(o):
    return Order(
        id=o['orderId'],
        time=datetime.utcfromtimestamp(o['time'] / 1000),
        update_time=datetime.utcfromtimestamp(o['updateTime'] / 1000),
        symbol=o['symbol'],
        origin_quantity=Decimal(o['origQty']),
        execute_quantity=Decimal(o['executedQty']),
        cummulative_quote_quantity=Decimal(o['cummulativeQuoteQty']),
        side=o['side'],
        type=o['type'],
        status=o['status'],
        stop_price=o['stopPrice'],
        iceberg_quantity=o['icebergQty'],
        origin_quote_order_quantity=o['origQuoteOrderQty']
    )


def main(client, session):
    exchange_info = client.get_exchange_info()
    symbols = [a['symbol'] for a in exchange_info['symbols'] if a['status'] == 'TRADING']
    logger.info(f"Symbols {len(symbols)}")
    logger.debug(symbols)

    for s in symbols:
        logger.info(f"Symbole {s}")
        orders = client.get_all_orders(symbol=s)
        for o in orders:
            if o['status'] == 'FILLED':
                logger.debug(o)
                order = get_order(o)
                logger.info(order)
                session.merge(order)
        session.commit()
        time.sleep(0.5)


if __name__ == "__main__":
    _session = init_db(DATABASE_URL)
    _client = Client(API_KEY, API_SECRET)
    main(_client, _session)
