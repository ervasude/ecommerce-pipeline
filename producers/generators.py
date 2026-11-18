from __future__ import annotations

import heapq
import math
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal

from faker import Faker

from producers.catalog import Catalog
from producers.schemas import (
    ClickEvent, DeviceContext, Envelope, EventType, Money, OrderCreated,
    OrderLine, OrderLineReturned, OrderStatus, OrderStatusChanged,
)

# İstanbul saatine göre (0..23) sipariş yoğunluğu çarpanı
HOURLY_WEIGHT = [0.2, 0.1, 0.05, 0.05, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 0.9, 1.0,
                 1.1, 1.0, 0.9, 0.9, 1.0, 1.1, 1.3, 1.6, 1.9, 2.0, 1.6, 0.8]

# (kod, indirim, minimum sepet)
COUPONS = [("KASIM50", Decimal("50"), Decimal("300")),
           ("HOSGELDIN100", Decimal("100"), Decimal("750")),
           ("EFSANE250", Decimal("250"), Decimal("2000"))]

FREE_SHIPPING_THRESHOLD = Decimal("2000")
SHIPPING_FEE = Decimal("34.99")

# (utm_source, utm_campaign). None'lar organik trafik, bu yüzden daha sık seçiliyor
UTM = [(None, None), (None, None), ("google", "kasim-indirimleri"),
       ("instagram", "efsane-kasim"), ("email", "sepette-indirim")]

RETURN_REASONS = ["beden_uymadi", "hasarli", "vazgecti", "yanlis_urun"]

def _poisson(rng: random.Random, lam: float) -> int:
    """Belirli bir sürede ortalama 'lam' kez olan bir olayın kaç kez olduğu."""
    if lam <= 0:
        return 0
    limit, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


@dataclass(order=True)
class _Scheduled:
    due: datetime
    seq: int
    kind: str = field(compare=False)
    order_id: str = field(compare=False)
    data: dict = field(compare=False, default_factory=dict)


@dataclass
class _OrderState:
    order_id: str
    customer_id: str
    status: OrderStatus
    lines: list[OrderLine]

class Generator:
    def __init__(self, catalog: Catalog, n_customers: int = 2000, seed: int = 7,
                 orders_per_hour: float = 40, late_share: float = 0.05,
                 orphan_share: float = 0.002, browse_ratio: float = 10):
        self.cat = catalog
        self.rng = random.Random(seed)
        self.fake = Faker("tr_TR")
        self.fake.seed_instance(seed)
        self.customers = [f"C-{i:05d}" for i in range(1, n_customers + 1)]
        self.customer_w = [self.rng.paretovariate(1.5) for _ in self.customers]
        self.orders_per_hour = orders_per_hour
        self.late_share = late_share
        self.orphan_share = orphan_share
        self.orders: dict[str, _OrderState] = {}
        self._queue: list[_Scheduled] = []
        self._seq = 0
        self._next_order = 1
        self.browse_ratio = browse_ratio

    def _wrap(self, event_type: EventType, payload, event_time: datetime, now: datetime) -> Envelope:
        version = 1 if event_time < self.cat.v2_cutover else 2
        return Envelope(event_type, event_time, now, payload, schema_version=version)

    def _schedule(self, due: datetime, kind: str, order_id: str, **data) -> None:
        self._seq += 1
        heapq.heappush(self._queue, _Scheduled(due, self._seq, kind, order_id, data))

    def _pick_customer(self) -> str:
        return self.rng.choices(self.customers, weights=self.customer_w)[0]
    
    def tick(self, now: datetime, step: timedelta) -> list[Envelope]:
        msgs = self._flush_due(now)

        local = now - step / 2 + timedelta(hours=3)                       # İstanbul saati
        weekend = 1.3 if local.weekday() >= 5 else 1.0
        lam = self.orders_per_hour * HOURLY_WEIGHT[local.hour] * weekend * (step / timedelta(hours=1))

        for _ in range(_poisson(self.rng, lam)):
            msgs += self._new_order(now, step)
        for _ in range(_poisson(self.rng, lam * self.browse_ratio)):   # siparişsiz gezinme        
            msgs += self._browse_session(now - step * self.rng.random(), now, now)
        return msgs
    
    def _new_order(self, now: datetime, step: timedelta) -> list[Envelope]:
        # 1) Olay zamanı: normal / geç gelen / geçmişi olmayan
        r = self.rng.random()
        if r < self.orphan_share:
            event_time = now - timedelta(days=self.rng.uniform(35, 45))
        elif r < self.orphan_share + self.late_share:
            event_time = now - timedelta(hours=self.rng.uniform(1, 48))
        else:
            event_time = now - step * self.rng.random()

        # 2) Sepet
        n_lines = self.rng.choices([1, 2, 3, 4], weights=[55, 25, 13, 7])[0]
        picked, lines, gross = set(), [], Decimal("0")
        for _ in range(n_lines * 2):                            # stok yoksa yeniden dene
            if len(lines) == n_lines:
                break
            l = self.cat.sample_listing()
            if l is None or l.listing_id in picked:
                continue
            qty = self.rng.choices([1, 2, 3], weights=[80, 15, 5])[0]
            if not self.cat.reserve(l.listing_id, qty):
                continue
            picked.add(l.listing_id)
            lines.append(OrderLine(len(lines) + 1, l.listing_id, l.product_id, l.seller_id, qty))
            gross += l.sale_price * qty
        if not lines:
            return []

        # 3) Kupon ve kargo
        code = discount = None
        if self.rng.random() < 0.15:
            eligible = [c for c in COUPONS if gross >= c[2]]
            if eligible:
                code, amount, _ = self.rng.choice(eligible)
                discount = Money(amount)
        shipping = Money(0 if gross >= FREE_SHIPPING_THRESHOLD else SHIPPING_FEE)

        # 4) Mesajlar: önce gezinme ve sepet, sonra sipariş
        customer = self._pick_customer()
        oid = f"O-{self._next_order:07d}"
        self._next_order += 1
        self.orders[oid] = _OrderState(oid, customer, OrderStatus.CREATED, lines)

        session_start = event_time - timedelta(minutes=self.rng.uniform(3, 25))
        msgs = self._browse_session(session_start, event_time, now, customer=customer, buy=lines)
        msgs.append(self._wrap(EventType.ORDER_CREATED,
                               OrderCreated(oid, customer, tuple(lines), shipping, code, discount),
                               event_time, now))

        # 5) Yaşam döngüsünün ilk adımını planla
        if self.rng.random() < 0.92:
            self._schedule(event_time + timedelta(minutes=self.rng.uniform(1, 30)), "paid", oid)
        else:
            self._schedule(event_time + timedelta(hours=self.rng.uniform(0.5, 3)), "cancel", oid,
                           reason="payment_timeout")
        return msgs
    
    def _flush_due(self, now: datetime) -> list[Envelope]:
        msgs = []
        while self._queue and self._queue[0].due <= now:
            s = heapq.heappop(self._queue)
            o = self.orders[s.order_id]

            if s.kind == "return":
                msgs.append(self._wrap(
                    EventType.ORDER_LINE_RETURNED,
                    OrderLineReturned(o.order_id, s.data["line_no"], s.data["qty"], s.data["reason"]),
                    s.due, now))
                continue

            target = {"paid": OrderStatus.PAID, "shipped": OrderStatus.SHIPPED,
                      "delivered": OrderStatus.DELIVERED, "cancel": OrderStatus.CANCELLED}[s.kind]
            msgs.append(self._wrap(
                EventType.ORDER_STATUS_CHANGED,
                OrderStatusChanged(o.order_id, o.status, target, s.data.get("reason")),
                s.due, now))
            o.status = target
            self._after_transition(o, s.due)
        return msgs

    def _after_transition(self, o: _OrderState, at: datetime) -> None:
        if o.status == OrderStatus.PAID:
            if self.rng.random() < 0.04:
                self._schedule(at + timedelta(hours=self.rng.uniform(1, 12)), "cancel",
                               o.order_id, reason="customer_request")
            else:
                self._schedule(at + timedelta(hours=self.rng.uniform(4, 48)), "shipped", o.order_id)
        elif o.status == OrderStatus.SHIPPED:
            self._schedule(at + timedelta(days=self.rng.uniform(1, 4)), "delivered", o.order_id)
        elif o.status == OrderStatus.DELIVERED:
            for line in o.lines:
                if self.rng.random() < 0.08:
                    self._schedule(at + timedelta(days=self.rng.uniform(1, 14)), "return", o.order_id,
                                   line_no=line.line_no,
                                   qty=self.rng.randint(1, line.quantity),
                                   reason=self.rng.choice(RETURN_REASONS))
        elif o.status == OrderStatus.CANCELLED:
            for line in o.lines:
                self.cat.release(line.listing_id, line.quantity)

    def _browse_session(self, start: datetime, until: datetime, now: datetime,
                        customer: str | None = None, buy: list[OrderLine] | None = None) -> list[Envelope]:
        if customer is None and self.rng.random() > 0.4:      # %40 anonim ziyaretçi
            customer = self._pick_customer()

        sid = "SES-" + uuid.UUID(int=self.rng.getrandbits(128)).hex[:16]
        platform = self.rng.choices(["ios", "android", "web"], weights=[35, 45, 20])[0]
        app_version = None if platform == "web" else self.fake.numerify("8.#.#")
        ctx = DeviceContext(platform, app_version, *self.rng.choice(UTM))

        msgs: list[Envelope] = []
        t = start

        def emit(event_type: EventType, listing_id: str, product_id: str) -> None:
            nonlocal t
            msgs.append(self._wrap(event_type, ClickEvent(sid, customer, listing_id, product_id, ctx),
                                   min(t, until), now))
            t += timedelta(seconds=self.rng.uniform(5, 120))

        for _ in range(self.rng.randint(1, 8)):               # göz gezdirme
            l = self.cat.sample_listing()
            if l is None:
                continue
            emit(EventType.PAGE_VIEW, l.listing_id, l.product_id)
            if self.rng.random() < 0.05:                      # ← 0.1'den 0.05'e düştü
                emit(EventType.ADD_TO_CART, l.listing_id, l.product_id)

        for line in buy or []:                                # satın alınacak ürünler
            emit(EventType.PAGE_VIEW, line.listing_id, line.product_id)
            emit(EventType.ADD_TO_CART, line.listing_id, line.product_id)
        return msgs