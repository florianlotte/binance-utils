from datetime import datetime

from sqlalchemy import Column, String, create_engine, DateTime, Float, Boolean, BigInteger, \
    ForeignKeyConstraint, Integer, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker, relationship

Base = declarative_base()


class Dust(Base):
    __tablename__ = 'dust'

    id = Column(BigInteger, primary_key=True, nullable=False)
    time = Column(DateTime, nullable=False)
    base_asset = Column(String(9), nullable=False)
    amount = Column(Numeric, nullable=False)
    quote_asset = Column(String(9), nullable=False)
    quote_amount = Column(Numeric, nullable=False)
    commission_asset = Column(String(9), nullable=False)
    commission_amount = Column(Numeric, nullable=False)

    def __repr__(self):
        return f"[{self.time}] convert {self.base_asset} dust to {self.quote_asset}"


class Convert(Base):
    __tablename__ = 'convert'

    time = Column(DateTime, primary_key=True, nullable=False)
    from_asset = Column(String(9), primary_key=True, nullable=False)
    to_asset = Column(String(9), primary_key=True, nullable=False)
    from_amount = Column(Numeric, nullable=False)
    to_amount = Column(Numeric, nullable=False)
    status = Column(String, nullable=False, default='SUCCESS')
    type = Column(String, nullable=False, default='MARKET')

    def __repr__(self):
        return f"[{self.time}] convert {self.from_amount}{self.from_asset} => {self.to_amount}{self.to_asset}"


class Transfer(Base):
    __tablename__ = 'transfer'

    id = Column(BigInteger, primary_key=True, nullable=False)
    time = Column(DateTime, nullable=False)
    asset = Column(String(9), nullable=False)
    amount = Column(Numeric, nullable=False)
    from_account = Column(String, nullable=False)
    to_account = Column(String, nullable=False)

    def __repr__(self):
        return f"[{self.time}][{self.from_account} -> {self.to_account}] {self.amount} {self.asset}"


class Deposit(Base):
    __tablename__ = 'deposit'

    asset = Column(String(9), primary_key=True, nullable=False)
    id = Column(String, primary_key=True, nullable=False)

    amount = Column(Numeric, nullable=False)
    network = Column(String(9), nullable=True)
    address = Column(String, nullable=True)
    time = Column(DateTime, nullable=False)

    def __repr__(self):
        return f"[{self.time}] {self.asset} {self.amount}"


class Withdraw(Base):
    __tablename__ = 'withdraw'

    asset = Column(String(9), primary_key=True, nullable=False)
    id = Column(String, primary_key=True, nullable=False)

    txid = Column(String, nullable=False)
    amount = Column(Numeric, nullable=False)
    transaction_fee = Column(Numeric, nullable=False)
    address = Column(String, nullable=False)
    time = Column(DateTime, nullable=False)
    network = Column(String(9), nullable=False)
    status = Column(Integer, nullable=False)

    def __repr__(self):
        return f"[{self.time}] {self.asset} {self.amount}"


class Order(Base):
    __tablename__ = 'order'

    symbol = Column(String(18), primary_key=True, nullable=False)
    id = Column(BigInteger, primary_key=True, nullable=False)

    time = Column(DateTime, nullable=False)
    update_time = Column(DateTime, nullable=False)
    origin_quantity = Column(Numeric, nullable=False)
    execute_quantity = Column(Numeric, nullable=False)
    cummulative_quote_quantity = Column(Numeric, nullable=False)
    side = Column(String, nullable=False)
    type = Column(String, nullable=False)
    status = Column(String, nullable=False)
    stop_price = Column(Numeric, nullable=False)
    iceberg_quantity = Column(Numeric, nullable=False)
    origin_quote_order_quantity = Column(Numeric, nullable=False)

    trades = relationship("Trade", back_populates="order")

    def __repr__(self):
        return f"[{self.time}] {self.side} {self.symbol} {self.origin_quantity} {self.cummulative_quote_quantity}"


class FutureOrder(Base):
    __tablename__ = 'future_order'

    symbol = Column(String(18), primary_key=True, nullable=False)
    id = Column(BigInteger, primary_key=True, nullable=False)

    time = Column(DateTime, nullable=False)
    update_time = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    price = Column(Numeric, nullable=False)
    average_price = Column(Numeric, nullable=False)
    origin_quantity = Column(Numeric, nullable=False)
    executed_quantity = Column(Numeric, nullable=False)
    side = Column(String, nullable=False)
    position_side = Column(String, nullable=False)
    type = Column(String, nullable=False)
    status = Column(String, nullable=False)
    stop_price = Column(Numeric, nullable=False)

    def __repr__(self):
        return f"[{self.time}][{self.id}] {self.symbol} {self.origin_quantity} {self.average_price}"



class Trade(Base):
    __tablename__ = 'trade'
    __table_args__ = (
        ForeignKeyConstraint(['symbol', 'order_id'], ['order.symbol', 'order.id']),
    )

    symbol = Column(String(18), primary_key=True, nullable=False)
    id = Column(BigInteger, primary_key=True, nullable=False)
    order_id = Column(BigInteger, nullable=False)
    price = Column(Numeric, nullable=False)
    quantity = Column(Numeric, nullable=False)
    quote_quantity = Column(Numeric, nullable=False)
    commission = Column(Numeric, nullable=False)
    commission_asset = Column(String(9), nullable=False)
    time = Column(DateTime, nullable=False)
    is_bayer = Column(Boolean, nullable=False)
    is_maker = Column(Boolean, nullable=False)
    is_best_match = Column(Boolean, nullable=False)

    order = relationship('Order', back_populates='trades')

    def __repr__(self):
        return f"[{self.time}] {self.symbol} {self.quantity} {self.price}"


class Exchange(Base):
    __tablename__ = 'exchange'

    symbol = Column(String(18), primary_key=True, nullable=False)
    status = Column(String, nullable=False)
    base_asset = Column(String(9), nullable=False)
    quote_asset = Column(String(9), nullable=False)
    is_spot_trading_allowed = Column(Boolean, nullable=False)
    is_margin_trading_allowed = Column(Boolean, nullable=False)

    def __repr__(self):
        return f"[{self.symbol}] {self.base_asset} {self.quote_asset} {self.status}"


class Wallet(Base):
    __tablename__ = 'wallet'

    asset = Column(String(9), primary_key=True, nullable=False)
    real_balance = Column(Numeric, nullable=False)
    local_balance = Column(Numeric, nullable=True)
    avg_price = Column(Numeric, nullable=True)

    def __repr__(self):
        return f"[{self.real_balance}] {self.local_balance} {self.avg_price}"


def init_db(uri):
    engine = create_engine(uri, convert_unicode=True)
    db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
    Base.query = db_session.query_property()
    Base.metadata.create_all(bind=engine)
    return db_session
