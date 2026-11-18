from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Union

SCHEMA_VERSION = 2


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ts(dt: datetime) -> str:
    if dt.tzinfo is None:
        raise ValueError("timezone'suz datetime kullanma")
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def new_event_id() -> str:
    return str(uuid.uuid4())

class Currency(str, Enum):
    TRY = "TRY"
    USD = "USD"
    EUR = "EUR"


class EventType(str, Enum):
    PRODUCT_UPSERTED = "product_upserted"
    LISTING_PRICE_CHANGED = "listing_price_changed"
    ORDER_CREATED = "order_created"
    ORDER_STATUS_CHANGED = "order_status_changed"
    ORDER_LINE_RETURNED = "order_line_returned"
    PAGE_VIEW = "page_view"
    ADD_TO_CART = "add_to_cart"


class OrderStatus(str, Enum):
    CREATED = "created"
    PAID = "paid"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


ALLOWED_TRANSITIONS: dict[OrderStatus, set[OrderStatus]] = {
    OrderStatus.CREATED:   {OrderStatus.PAID, OrderStatus.CANCELLED},
    OrderStatus.PAID:      {OrderStatus.SHIPPED, OrderStatus.CANCELLED},
    OrderStatus.SHIPPED:   {OrderStatus.DELIVERED},
    OrderStatus.DELIVERED: set(),
    OrderStatus.CANCELLED: set(),
}

@dataclass(frozen=True)
class Money:
    amount: Decimal
    currency: Currency = Currency.TRY

    def __post_init__(self):
        q = Decimal(str(self.amount)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if q < 0:
            raise ValueError(f"negatif tutar: {q}")
        object.__setattr__(self, "amount", q)

@dataclass(frozen=True)
class ProductUpserted:
    product_id: str
    name: str
    brand: str
    category_path: tuple[str, ...]      # ("Elektronik", "Ses", "Kulaklık")
    is_active: bool = True

    def __post_init__(self):
        if not 1 <= len(self.category_path) <= 4:
            raise ValueError("kategori derinliği 1-4 olmalı")


@dataclass(frozen=True)
class ListingPriceChanged:
    listing_id: str                     # f"{product_id}:{seller_id}"
    product_id: str
    seller_id: str
    list_price: Money                   # etiket fiyatı
    sale_price: Money                   # satış fiyatı (indirimli olabilir)
    stock: int

    def __post_init__(self):
        if self.list_price.currency != self.sale_price.currency:
            raise ValueError("list/sale para birimi farklı")
        if self.sale_price.amount > self.list_price.amount:
            raise ValueError("sale_price > list_price")
        if self.stock < 0:
            raise ValueError("negatif stok")
        
@dataclass(frozen=True)
class OrderLine:
    line_no: int
    listing_id: str
    product_id: str
    seller_id: str
    quantity: int

    def __post_init__(self):
        if self.quantity <= 0:
            raise ValueError("quantity > 0 olmalı")


@dataclass(frozen=True)
class OrderCreated:
    order_id: str
    customer_id: str
    lines: tuple[OrderLine, ...]
    shipping_fee: Money
    coupon_code: str | None = None
    coupon_discount: Money | None = None   # sipariş seviyesinde, satırlara dağıtılacak

    def __post_init__(self):
        if not self.lines:
            raise ValueError("boş sipariş")
        if [l.line_no for l in self.lines] != list(range(1, len(self.lines) + 1)):
            raise ValueError("line_no 1..n sıralı olmalı")
        if (self.coupon_code is None) != (self.coupon_discount is None):
            raise ValueError("kupon kodu ve tutarı birlikte olmalı")


@dataclass(frozen=True)
class OrderStatusChanged:
    order_id: str
    from_status: OrderStatus
    to_status: OrderStatus
    reason: str | None = None

    def __post_init__(self):
        if self.to_status not in ALLOWED_TRANSITIONS[self.from_status]:
            raise ValueError(f"geçersiz geçiş: {self.from_status.value} -> {self.to_status.value}")


@dataclass(frozen=True)
class OrderLineReturned:
    order_id: str
    line_no: int
    quantity: int
    reason: str

@dataclass(frozen=True)
class DeviceContext:
    platform: str                        # "ios" | "android" | "web"
    app_version: str | None = None       # web'de null
    utm_source: str | None = None
    utm_campaign: str | None = None


@dataclass(frozen=True)
class ClickEvent:
    session_id: str
    customer_id: str | None              # giriş yapmamış ziyaretçi → None
    listing_id: str
    product_id: str
    context: DeviceContext

Payload = Union[ProductUpserted, ListingPriceChanged, OrderCreated,
                OrderStatusChanged, OrderLineReturned, ClickEvent]

# payload tipi -> (izin verilen event_type'lar, topic, partition key alanı)
ROUTING: dict[type, tuple[set[EventType], str, str]] = {
    ProductUpserted:     ({EventType.PRODUCT_UPSERTED},              "products", "product_id"),
    ListingPriceChanged: ({EventType.LISTING_PRICE_CHANGED},         "listings", "listing_id"),
    OrderCreated:        ({EventType.ORDER_CREATED},                 "orders",   "order_id"),
    OrderStatusChanged:  ({EventType.ORDER_STATUS_CHANGED},          "orders",   "order_id"),
    OrderLineReturned:   ({EventType.ORDER_LINE_RETURNED},           "orders",   "order_id"),
    ClickEvent:          ({EventType.PAGE_VIEW, EventType.ADD_TO_CART}, "events", "session_id"),
}


@dataclass(frozen=True)
class Envelope:
    event_type: EventType
    event_time: datetime          # olay ne zaman OLDU (iş zamanı)
    produced_at: datetime         # producer ne zaman GÖNDERDİ
    payload: Payload
    schema_version: int = SCHEMA_VERSION
    source: str = "ecom-simulator"
    event_id: str = field(default_factory=new_event_id)

    def __post_init__(self):
        allowed, _, _ = ROUTING[type(self.payload)]
        if self.event_type not in allowed:
            raise ValueError(f"{self.event_type.value} bu payload ile kullanılamaz")
        for name in ("event_time", "produced_at"):
            if getattr(self, name).tzinfo is None:
                raise ValueError(f"{name} timezone'suz")
        if self.event_time > self.produced_at:
            raise ValueError("olay, gönderilmeden önce olmalı")

    @property
    def topic(self) -> str:
        return ROUTING[type(self.payload)][1]

    @property
    def key(self) -> str:
        return getattr(self.payload, ROUTING[type(self.payload)][2])

    def to_dict(self) -> dict:
        d = _encode(self)
        return _downgrade_v1(d) if self.schema_version == 1 else d

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"))
    
def _encode(obj):
    if is_dataclass(obj):
        return {f.name: _encode(getattr(obj, f.name)) for f in fields(obj)}
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, Decimal):
        return str(obj)                  # "1299.90" — JSON'da float'a düşmesin
    if isinstance(obj, datetime):
        return ts(obj)
    if isinstance(obj, (list, tuple)):
        return [_encode(x) for x in obj]
    return obj


def _downgrade_v1(d: dict) -> dict:
    """v1 (legacy): ilan fiyatı düz float 'price', para birimi ve list_price yok."""
    if d["event_type"] == EventType.LISTING_PRICE_CHANGED.value:
        p = d["payload"]
        p["price"] = float(p.pop("sale_price")["amount"])
        p.pop("list_price")
    return d
