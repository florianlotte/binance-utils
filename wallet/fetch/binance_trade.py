import time
from datetime import datetime

from binance import Client
from loguru import logger

from config.base import DATABASE_URL, API_KEY, API_SECRET
from model.base import init_db, Trade


def get_trade(t):
    return Trade(
        symbol=t['symbol'],
        id=t['id'],
        order_id=t['orderId'],
        price=t['price'],
        quantity=t['qty'],
        quote_quantity=t['quoteQty'],
        commission=t['commission'],
        commission_asset=t['commissionAsset'],
        time=datetime.utcfromtimestamp(t['time'] / 1000),
        is_bayer=t['isBuyer'],
        is_maker=t['isMaker'],
        is_best_match=t['isBestMatch']
    )


def main(client, session):

    exchange_info = client.get_exchange_info()
    symbols = [a['symbol'] for a in exchange_info['symbols'] if a['status'] == 'TRADING']
    logger.info(f"> {len(symbols)} symbols")
    logger.debug(symbols)

    for s in symbols:
        logger.info(f"Symbole {s}")
        trades = client.get_my_trades(symbol=s)
        for t in trades:
            logger.debug(t)
            trade = get_trade(t)
            logger.info(trade)
            session.merge(trade)
        session.commit()
        time.sleep(0.5)


if __name__ == "__main__":
    _session = init_db(DATABASE_URL)
    _client = Client(API_KEY, API_SECRET)
    main(_client, _session)
