from decimal import Decimal

from loguru import logger


class Balance:
    mean_price = Decimal(0)
    last_qty = Decimal(0)

    def __init__(self, base_asset, quote_asset):
        self.base_asset = base_asset
        self.quote_asset = quote_asset

    def add(self, side, qty, price=Decimal(0)):
        if side > 0 and price > 0:
            if self.mean_price == 0:
                self.mean_price = price
            else:
                self.mean_price = (self.mean_price * self.last_qty + price * qty) / (self.last_qty + qty)
        self.last_qty = self.last_qty + side * qty


class Wallet:
    balances = {}

    def __init__(self, quote_asset, client):
        self.quote_asset = quote_asset
        self.client = client

    def add_line(self, line):
        if line['side'] in ['IN', 'BUY']:
            logger.info(f"{line['account']} {line['quote_qty']} {line['quote_asset']}")
            converted_qty = self.convert_to_quote(time=line['time'], qty=line['quote_qty'], asset=line['quote_asset'])
            self.add_asset(asset=line['asset'], side=1, qty=line['qty'], price=(converted_qty / line['qty']))
            if line['quote_qty']:
                self.add_asset(asset=line['quote_asset'], side=-1, qty=line['quote_qty'])
            if line['fee_asset']:
                self.add_asset(asset=line['fee_asset'], side=-1, qty=line['fee'])
        if line['side'] in ['OUT', 'SELL']:
            self.add_asset(asset=line['asset'], side=-1, qty=line['qty'])
            if line['quote_qty']:
                self.add_asset(asset=line['quote_asset'], side=1, qty=line['quote_qty'])
            if line['fee_asset']:
                self.add_asset(asset=line['fee_asset'], side=-1, qty=line['fee'])

    def convert_to_quote(self, time, qty, asset):
        if qty == 0 or self.quote_asset == asset:
            return qty
        elif asset:
            quote_price = self.avg_1m_price(time=time, base_asset=asset, quote_asset=self.quote_asset)
            return qty * quote_price
        else:
            return Decimal(0)

    def add_asset(self, side, qty, asset, price=Decimal(0)):
        balance = self.get_balance(asset)
        balance.add(side=side, qty=qty, price=price)

    def get_balance(self, asset) -> Balance:
        if asset not in self.balances:
            self.balances[asset] = Balance(base_asset=asset, quote_asset=self.quote_asset)
        return self.balances[asset]

    def sanitize(self):
        for asset in list(self.balances):
            if self.balances[asset].last_qty == 0:
                self.balances.pop(asset)

    def avg_1m_price(self, time, base_asset, quote_asset):
        start_time = int(time.timestamp() * 1000) - 60
        candles = self.client.get_klines(symbol=base_asset + quote_asset, interval='1m', limit=1, startTime=start_time)
        return (Decimal(candles[0][1]) + Decimal(candles[0][4])) / 2


class Data:
    data = []

    def add(self, time, account, side, qty, asset, quote_qty=Decimal(0), quote_asset='', fee=Decimal(0), fee_asset=''):
        self.data.append([time, account, side, qty, asset, quote_qty, quote_asset, fee, fee_asset])

    def to_df(self):
        import pandas as pd
        df = pd.DataFrame(
            self.data,
            columns=['time', 'account', 'side', 'qty', 'asset', 'quote_qty', 'quote_asset', 'fee', 'fee_asset']
        )
        df.sort_values(by=['time'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df
