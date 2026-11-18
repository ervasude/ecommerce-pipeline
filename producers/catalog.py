from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from faker import Faker

from producers.schemas import (
    Envelope, EventType, ListingPriceChanged, Money, ProductUpserted,
)

CATEGORY_TREE: dict[tuple[str, ...], dict] = {
    ("Elektronik", "Ses", "Kulaklık"):           {"names": ["Kablosuz Kulaklık", "Kulak İçi Kulaklık", "Oyuncu Kulaklığı"], "price": (400, 6000)},
    ("Elektronik", "Ses", "Hoparlör"):           {"names": ["Bluetooth Hoparlör", "Soundbar"], "price": (600, 15000)},
    ("Elektronik", "Telefon", "Aksesuar"):       {"names": ["Hızlı Şarj Adaptörü", "Telefon Kılıfı", "Powerbank"], "price": (100, 2000)},
    ("Ev & Yaşam", "Mutfak", "Küçük Ev Aletleri"): {"names": ["Airfryer", "Kahve Makinesi", "Blender"], "price": (1000, 12000)},
    ("Ev & Yaşam", "Mutfak", "Pişirme"):         {"names": ["Granit Tava", "Düdüklü Tencere"], "price": (300, 4000)},
    ("Moda", "Kadın", "Ayakkabı"):               {"names": ["Spor Ayakkabı", "Topuklu Ayakkabı", "Bot"], "price": (500, 5000)},
    ("Moda", "Erkek", "Giyim"):                  {"names": ["Basic Tişört", "Kapüşonlu Sweatshirt", "Mont"], "price": (150, 6000)},
    ("Kozmetik", "Cilt Bakımı"):                 {"names": ["Nemlendirici Krem", "Güneş Kremi", "Serum"], "price": (150, 2500)},
}


def _psych(x: float) -> Decimal:
    """Psikolojik fiyat: 847.3 -> 847.99, 1243 -> 1299.90"""
    if x >= 1000:
        return Decimal(int(x) // 100 * 100 + 99) + Decimal("0.90")
    return Decimal(int(x)) + Decimal("0.99")

@dataclass
class ProductState:
    product_id: str
    name: str
    brand: str
    category_path: tuple[str, ...]
    popularity: float
    is_active: bool = True


@dataclass
class ListingState:
    listing_id: str
    product_id: str
    seller_id: str
    list_price: Decimal
    sale_price: Decimal
    stock: int
    on_campaign: bool = False

class Catalog:
    def __init__(self, v2_cutover: datetime, seed: int = 42):
        self.rng = random.Random(seed)
        self.fake = Faker("tr_TR")
        self.fake.seed_instance(seed)
        self.v2_cutover = v2_cutover
        self.products: dict[str, ProductState] = {}
        self.listings: dict[str, ListingState] = {}
        self.seller_ids: list[str] = []
        self._next_product = 1

    def _wrap(self, event_type: EventType, payload, now: datetime) -> Envelope:
        version = 1 if now < self.v2_cutover else 2
        return Envelope(event_type, now, now, payload, schema_version=version)

    def _product_msg(self, p: ProductState, now: datetime) -> Envelope:
        return self._wrap(
            EventType.PRODUCT_UPSERTED,
            ProductUpserted(p.product_id, p.name, p.brand, p.category_path, p.is_active),
            now)

    def _listing_msg(self, l: ListingState, now: datetime) -> Envelope:
        return self._wrap(
            EventType.LISTING_PRICE_CHANGED,
            ListingPriceChanged(l.listing_id, l.product_id, l.seller_id,
                                Money(l.list_price), Money(l.sale_price), l.stock),
            now)
    
    def bootstrap(self, now: datetime, n_products: int = 200, n_sellers: int = 40) -> list[Envelope]:
        self.seller_ids = [f"S-{i:03d}" for i in range(1, n_sellers + 1)]
        msgs: list[Envelope] = []
        for _ in range(n_products):
            msgs.extend(self._create_product(now))
        return msgs

    def _create_product(self, now: datetime) -> list[Envelope]:
        path = self.rng.choice(list(CATEGORY_TREE))
        spec = CATEGORY_TREE[path]
        pid = f"P-{self._next_product:05d}"
        self._next_product += 1
        brand = self.fake.last_name()
        p = ProductState(pid, f"{brand} {self.rng.choice(spec['names'])}", brand, path,
                         popularity=self.rng.paretovariate(1.2))
        self.products[pid] = p

        msgs = [self._product_msg(p, now)]
        base = self.rng.uniform(*spec["price"])
        for seller in self.rng.sample(self.seller_ids, k=self.rng.randint(1, 4)):
            msgs.append(self._create_listing(p, seller, base, now))
        return msgs

    def _create_listing(self, p: ProductState, seller_id: str, base: float, now: datetime) -> Envelope:
        price = _psych(base * self.rng.uniform(0.9, 1.1))
        l = ListingState(f"{p.product_id}:{seller_id}", p.product_id, seller_id,
                         list_price=price, sale_price=price, stock=self.rng.randint(5, 300))
        self.listings[l.listing_id] = l
        return self._listing_msg(l, now)

    def change_prices(self, now: datetime, share: float = 0.05) -> list[Envelope]:
        active = [l for l in self.listings.values() if self.products[l.product_id].is_active]
        if not active:
            return []
        msgs = []
        for l in self.rng.sample(active, k=max(1, int(len(active) * share))):
            r = self.rng.random()
            if r < 0.15 and not l.on_campaign:                 # kampanya başlar
                l.sale_price = min(_psych(float(l.list_price) * self.rng.uniform(0.6, 0.9)), l.list_price)
                l.on_campaign = True
            elif r < 0.30 and l.on_campaign:                   # kampanya biter
                l.sale_price = l.list_price
                l.on_campaign = False
            elif r < 0.40:                                     # etiket fiyatına zam
                l.list_price = _psych(float(l.list_price) * self.rng.uniform(1.03, 1.20))
                if not l.on_campaign:
                    l.sale_price = l.list_price
            else:                                              # küçük dalgalanma
                l.sale_price = min(_psych(float(l.sale_price) * self.rng.uniform(0.95, 1.05)), l.list_price)
            msgs.append(self._listing_msg(l, now))
        return msgs
    
    def recategorize(self, now: datetime, k: int = 1) -> list[Envelope]:
        msgs = []
        for p in self.rng.sample(list(self.products.values()), k=min(k, len(self.products))):
            siblings = [c for c in CATEGORY_TREE if c[0] == p.category_path[0] and c != p.category_path]
            if not siblings:
                continue
            p.category_path = self.rng.choice(siblings)
            msgs.append(self._product_msg(p, now))
        return msgs

    def onboard(self, now: datetime) -> list[Envelope]:
        if self.rng.random() < 0.5:                            # yeni ürün lansmanı
            return self._create_product(now)
        p = self.rng.choice([p for p in self.products.values() if p.is_active])
        free = [s for s in self.seller_ids if f"{p.product_id}:{s}" not in self.listings]
        if not free:
            return []
        base = float(next(l.list_price for l in self.listings.values() if l.product_id == p.product_id))
        return [self._create_listing(p, self.rng.choice(free), base, now)]   # yeni satıcı

    def deactivate(self, now: datetime) -> list[Envelope]:
        active = [p for p in self.products.values() if p.is_active]
        if len(active) < 10:
            return []
        p = self.rng.choice(active)
        p.is_active = False
        return [self._product_msg(p, now)]

    def reserve(self, listing_id: str, qty: int) -> bool:
        l = self.listings[listing_id]
        if l.stock < qty:
            return False
        l.stock -= qty
        return True
    
    def release(self, listing_id: str, qty: int) -> None:
        self.listings[listing_id].stock += qty

    def restock(self, now: datetime, threshold: int = 5) -> list[Envelope]:
        msgs = []
        for l in self.listings.values():
            if l.stock < threshold and self.products[l.product_id].is_active:
                l.stock += self.rng.randint(50, 300)
                msgs.append(self._listing_msg(l, now))
        return msgs

    def sample_listing(self) -> ListingState | None:
        cands = [l for l in self.listings.values()
                 if l.stock > 0 and self.products[l.product_id].is_active]
        if not cands:
            return None
        weights = [self.products[l.product_id].popularity for l in cands]
        return self.rng.choices(cands, weights=weights, k=1)[0]
    
