from sqlalchemy import Column, String, BigInteger, Numeric, ForeignKey, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Exchange(Base):
    __tablename__ = 'exchange'
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)

class Product(Base):
    __tablename__ = 'product'
    id = Column(String, primary_key=True)
    exchange_id = Column(String, ForeignKey('exchange.id'), nullable=False)
    base_asset = Column(String, nullable=False)
    quote_asset = Column(String, nullable=False)

class Strategy(Base):
    __tablename__ = 'strategy'
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    configuration_json = Column(Text, nullable=True)

class Order(Base):
    __tablename__ = 'order'
    id = Column(String, primary_key=True)
    exchange_order_id = Column(String, nullable=True)
    strategy_id = Column(String, ForeignKey('strategy.id'), nullable=False)
    product_id = Column(String, ForeignKey('product.id'), nullable=False)
    exchange_id = Column(String, ForeignKey('exchange.id'), nullable=False)
    type = Column(String, nullable=False)
    side = Column(String, nullable=False)
    price = Column(Numeric, nullable=True)
    trigger_price = Column(Numeric, nullable=True)
    quantity = Column(Numeric, nullable=False)
    status = Column(String, nullable=False)
    timestamp = Column(BigInteger, nullable=False)
    filled_quantity = Column(Numeric, nullable=True, default=0)
    filled_price = Column(Numeric, nullable=True)

class Trade(Base):
    __tablename__ = 'trade'
    id = Column(String, primary_key=True)
    order_id = Column(String, ForeignKey('order.id'), nullable=False)
    exchange_trade_id = Column(String, nullable=True)
    product_id = Column(String, ForeignKey('product.id'), nullable=False)
    side = Column(String, nullable=False)
    price = Column(Numeric, nullable=False)
    quantity = Column(Numeric, nullable=False)
    fee = Column(Numeric, nullable=True)
    fee_asset = Column(String, nullable=True)
    timestamp = Column(BigInteger, nullable=False)

class Position(Base):
    __tablename__ = 'position'
    strategy_id = Column(String, ForeignKey('strategy.id'), primary_key=True)
    product_id = Column(String, ForeignKey('product.id'), primary_key=True)
    side = Column(String, primary_key=True)
    quantity = Column(Numeric, nullable=False)
    entry_price = Column(Numeric, nullable=False)
    unrealized_pnl = Column(Numeric, nullable=False)
    last_update_timestamp = Column(BigInteger, nullable=False)
