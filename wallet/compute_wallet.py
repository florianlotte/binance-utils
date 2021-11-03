from decimal import Decimal, ROUND_UP

from binance import Client
from loguru import logger
from sqlalchemy import asc

from config.base import DATABASE_URL, API_KEY, API_SECRET
from model.base import init_db, Deposit, Exchange, Order, Transfer, Dust, Withdraw, Convert, Wallet as DB_Wallet
from wallet.base import Wallet, Data


def get_move(data: Data, session):
    # Get all deposits
    logger.info('With DEPOSITS')
    deposits = session.query(Deposit).order_by(asc(Deposit.time)).all()
    for d in deposits:
        data.add(time=d.time, account='DEPOSIT', side='IN', qty=d.amount, asset=d.asset)

    # Get all withdraw
    logger.info('With WITHDRAWS')
    withdraws = session.query(Withdraw).order_by(asc(Withdraw.time)).all()
    for w in withdraws:
        data.add(time=w.time, account='WITHDRAW', side='OUT', qty=w.amount, asset=w.asset,
                 fee=w.transaction_fee, fee_asset=w.asset)

    # Get all transfers
    logger.info('With TANSFERS')
    transfers = session.query(Transfer).filter(Transfer.from_account == 'MAIN').order_by(asc(Transfer.time)).all()
    for t in transfers:
        data.add(time=t.time, account='TRANSFERT', side='OUT', qty=t.amount, asset=t.asset)
    transfers = session.query(Transfer).filter(Transfer.to_account == 'MAIN').order_by(asc(Transfer.time)).all()
    for t in transfers:
        data.add(time=t.time, account='TRANSFERT', side='IN', qty=t.amount, asset=t.asset)


def get_order(data, assets, session):
    # Compute all orders
    logger.info('With ORDERS')
    orders = session.query(Order).order_by(asc(Order.time)).all()
    for o in orders:
        for t in o.trades:
            data.add(time=t.time, account='TRADE', side=o.side, qty=t.quantity, asset=assets[t.symbol]['base'],
                     quote_qty=t.quote_quantity, quote_asset=assets[t.symbol]['quote'],
                     fee=t.commission, fee_asset=t.commission_asset)

    # Add convert
    logger.info('With CONVERT')
    converts = session.query(Convert).order_by(asc(Convert.time)).all()
    for c in converts:
        data.add(time=c.time, account='CONVERT', side='SELL', qty=c.from_amount, asset=c.from_asset,
                 quote_asset=c.to_asset, quote_qty=c.to_amount)

    # Add dust
    logger.info('With DUSTS')
    dusts = session.query(Dust).order_by(asc(Dust.time)).all()
    for d in dusts:
        data.add(time=d.time, account='DUST', side='SELL', qty=d.amount, asset=d.base_asset, quote_qty=d.quote_amount,
                 quote_asset=d.quote_asset, fee=d.commission_amount, fee_asset=d.commission_asset)


def build_local_wallet(client, session):
    data = Data()
    wallet = Wallet(quote_asset='USDT', client=client)

    # Get exchange info
    exchange_infos = session.query(Exchange).filter(Exchange.status == 'TRADING').all()
    assets = dict()
    for asset in exchange_infos:
        assets[asset.symbol] = {'base': asset.base_asset, 'quote': asset.quote_asset}

    get_move(data, session)
    get_order(data, assets, session)

    df = data.to_df()
    for i in range(0, len(df)):
        wallet.add_line(df.iloc[i])
    wallet.sanitize()

    return wallet


def build_real_wallet(client):
    account = client.get_account()
    real_wallet = {}
    for balance in account['balances']:
        asset_free = Decimal(balance['free'])
        asset_locked = Decimal(balance['locked'])
        if asset_locked > 0 or asset_free > 0:
            real_wallet[balance['asset']] = asset_free + asset_locked
    logger.info(real_wallet)
    return real_wallet


def compare_wallets(local_wallet, real_wallet):
    for real_asset in list(real_wallet):
        if real_asset not in local_wallet.balances:
            logger.warning(f"{real_asset} not in local wallet!")

    _local_wallet = {}
    for local_asset in list(local_wallet.balances):
        _local_wallet[local_asset] = local_wallet.balances[local_asset].last_qty
        if local_asset not in real_wallet:
            logger.warning(f"{local_asset} not in real wallet!")
        elif local_wallet.balances[local_asset].last_qty != real_wallet[local_asset]:
            logger.warning(
                f"{local_asset} is wrong. Real={real_wallet[local_asset]} / Local={local_wallet.balances[local_asset].last_qty} "
                f"=> {real_wallet[local_asset] - local_wallet.balances[local_asset].last_qty}")

    assert real_wallet == _local_wallet


def main():
    session = init_db(DATABASE_URL)
    client = Client(API_KEY, API_SECRET)
    real_wallet = build_real_wallet(client)
    local_wallet = build_local_wallet(client, session)
    compare_wallets(local_wallet, real_wallet)
    for b in local_wallet.balances:
        wallet = DB_Wallet(
            asset=b,
            local_balance=local_wallet.balances[b].last_qty,
            avg_price=local_wallet.balances[b].mean_price.quantize(Decimal('0.01'), rounding=ROUND_UP)
        )
        session.merge(wallet)
    session.commit()

    compare_wallets(local_wallet,real_wallet)


if __name__ == "__main__":
    main()
