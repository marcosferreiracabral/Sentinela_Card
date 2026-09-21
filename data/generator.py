"""Synthetic transaction dataset generator with realistic baseline and fraud injection patterns."""

import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from app.features.geographic import CITY_COORDS, haversine_km
from app.schemas.transaction import TRANSACTION_SCHEMA

SEED: int = 42

MCCS: list[dict[str, Any]] = [
    {"code": "5411", "name": "Supermercado", "mean": 120.0, "sigma": 0.7, "min": 15, "max": 500, "w": 22},
    {"code": "5812", "name": "Restaurante", "mean": 95.0, "sigma": 0.6, "min": 25, "max": 320, "w": 12},
    {"code": "5814", "name": "Fast Food", "mean": 38.0, "sigma": 0.5, "min": 8, "max": 90, "w": 14},
    {"code": "5311", "name": "Loja de Departamento", "mean": 260.0, "sigma": 0.6, "min": 40, "max": 1200, "w": 6},
    {"code": "5732", "name": "Eletrônicos", "mean": 900.0, "sigma": 0.8, "min": 100, "max": 5000, "w": 3},
    {"code": "5661", "name": "Calçados", "mean": 180.0, "sigma": 0.5, "min": 60, "max": 700, "w": 3},
    {"code": "5912", "name": "Farmácia", "mean": 60.0, "sigma": 0.6, "min": 10, "max": 300, "w": 8},
    {"code": "5541", "name": "Posto de Gasolina", "mean": 160.0, "sigma": 0.4, "min": 50, "max": 400, "w": 8},
    {"code": "4121", "name": "Taxi", "mean": 30.0, "sigma": 0.5, "min": 10, "max": 90, "w": 4},
    {"code": "4111", "name": "Transporte", "mean": 45.0, "sigma": 0.5, "min": 10, "max": 160, "w": 3},
    {"code": "4722", "name": "Agência de Viagens", "mean": 1400.0, "sigma": 0.7, "min": 400, "max": 5000, "w": 1},
    {"code": "5944", "name": "Joalheria", "mean": 2500.0, "sigma": 0.8, "min": 600, "max": 12000, "w": 1},
    {"code": "7995", "name": "Apostas", "mean": 120.0, "sigma": 1.0, "min": 20, "max": 3000, "w": 1},
    {"code": "6051", "name": "Transferência", "mean": 600.0, "sigma": 0.9, "min": 50, "max": 8000, "w": 1},
    {"code": "5816", "name": "Bens Digitais", "mean": 80.0, "sigma": 0.7, "min": 5, "max": 400, "w": 3},
    {"code": "5999", "name": "Varejo Geral", "mean": 90.0, "sigma": 0.7, "min": 10, "max": 600, "w": 4},
    {"code": "4814", "name": "Telecom", "mean": 80.0, "sigma": 0.4, "min": 20, "max": 200, "w": 3},
    {"code": "7832", "name": "Cinema", "mean": 35.0, "sigma": 0.4, "min": 10, "max": 80, "w": 2},
    {"code": "7997", "name": "Clubes", "mean": 60.0, "sigma": 0.5, "min": 20, "max": 250, "w": 1},
    {"code": "5200", "name": "Casa e Construção", "mean": 380.0, "sigma": 0.7, "min": 60, "max": 1800, "w": 2},
    {"code": "4215", "name": "Transportadora", "mean": 80.0, "sigma": 0.6, "min": 15, "max": 300, "w": 2},
    {"code": "5813", "name": "Bar", "mean": 55.0, "sigma": 0.6, "min": 15, "max": 200, "w": 5},
]

POPULAR_CITIES: list[str] = [
    "São Paulo",
    "Rio de Janeiro",
    "Belo Horizonte",
    "Salvador",
    "Recife",
    "Curitiba",
    "Porto Alegre",
    "Brasília",
]
ALL_CITIES: list[str] = list(CITY_COORDS.keys())

MERCHANT_NAMES_BY_MCC: dict[str, list[str]] = {
    "5411": ["Supermercado Bom Preço", "Mercado São José", "Hortifruti Central"],
    "5812": ["Restaurante Sabor Mineiro", "Trattoria Nonna", "Churrascaria Fogo Alto"],
    "5814": ["Padaria Central", "Burger do Bairro", "Pizzaria Rápida"],
    "5311": ["Loja de Departamento Megastore", "Magazine Bom Comprar", "Casas do Nordeste"],
    "5732": ["Eletrônicos X", "Tech World", "Casa de Som e Imagem"],
    "5661": ["Calçados Pé Leve", "Sapataria Central"],
    "5912": ["Farmácia Saúde+", "Drogaria Popular"],
    "5541": ["Posto Ipiranga", "Posto Shell Boa Vista"],
    "4121": ["Taxi Copa", "Companhia de Táxi 99"],
    "4111": ["Metrô e Ônibus", "Terminal Rodoviário"],
    "4722": ["Agência Turismo Brasil", "CVC Viagens"],
    "5944": ["Joia Ouro", "Joalheria Brilhante"],
    "7995": ["Bet Esportiva BR", "Lotérica da Praça"],
    "6051": ["Câmbio e Transferências", "MoneyGo"],
    "5816": ["Loja de Games", "App Store BR"],
    "5999": ["Kiosque de Recarga", "Armarinho Central", "Presentes e Cia"],
    "4814": ["Operadora Vivo", "Operadora Claro"],
    "7832": ["Cinema Center", "Cine Arte"],
    "7997": ["Academia Corpo Livre", "Clube Social"],
    "5200": ["Casa e Construção", "Material de Construção BR"],
    "4215": ["Transportadora Expressa", "Correios BR"],
    "5813": ["Bar do Zé", "Pub Cerveja Gourmet"],
}

BIN_POOL: list[str] = [
    "453201",
    "402400",
    "455187",
    "525400",
    "501123",
    "546122",
    "402422",
    "555444",
    "453900",
    "402425",
    "544555",
    "523555",
]

HOUR_WEIGHTS: list[float] = [
    1.0,
    0.5,
    0.4,
    0.3,
    0.3,
    0.4,
    0.8,
    1.8,
    3.5,
    2.8,
    2.0,
    2.5,
    3.2,
    2.8,
    2.0,
    1.8,
    2.0,
    3.0,
    4.5,
    5.0,
    3.5,
    2.2,
    1.4,
    0.8,
]


class Merchant:
    """Merchant entity representation."""

    __slots__ = ("merchant_id", "name", "code", "city", "terminal_id")

    def __init__(self, merchant_id: str, name: str, code: str, city: str, terminal_id: str) -> None:
        """Initializes merchant instance."""
        self.merchant_id = merchant_id
        self.name = name
        self.code = code
        self.city = city
        self.terminal_id = terminal_id


class CustomerProfile:
    """Customer profile representation."""

    __slots__ = ("customer_id", "home_city", "card_id", "devices", "merchants", "n_regular_tx")

    def __init__(self, customer_id: str, home_city: str, card_id: str, devices: list[str]) -> None:
        """Initializes customer profile instance."""
        self.customer_id = customer_id
        self.home_city = home_city
        self.card_id = card_id
        self.devices = devices
        self.merchants: list[Merchant] = []
        self.n_regular_tx = 0


def _rng_amount(mcc: dict[str, Any], rng: random.Random) -> float:
    """Generates log-normal transaction amount bounded by MCC limits.

    Args:
        mcc: MCC parameter dictionary.
        rng: Random generator instance.

    Returns:
        Sampled monetary amount.
    """
    value = math.exp(rng.gauss(math.log(mcc["mean"]), mcc["sigma"]))
    return round(max(float(mcc["min"]), min(float(mcc["max"]), value)), 2)


def _hour_weighted(rng: random.Random) -> int:
    """Samples hour of day according to diurnal transaction weights.

    Args:
        rng: Random generator instance.

    Returns:
        Sampled hour in [0, 23].
    """
    return rng.choices(range(24), weights=HOUR_WEIGHTS)[0]


def _card_id(rng: random.Random) -> str:
    """Generates random card identifier from BIN pool.

    Args:
        rng: Random generator instance.

    Returns:
        Card identifier string.
    """
    return f"{rng.choice(BIN_POOL)}{rng.randint(1000000000, 9999999999)}"


def _device(rng: random.Random) -> str:
    """Generates random device identifier.

    Args:
        rng: Random generator instance.

    Returns:
        Device identifier string.
    """
    return f"DVR-{rng.randint(100000, 999999)}"


def build_world(rng: random.Random, merchants_per_city: int = 8) -> dict[str, Merchant]:
    """Builds synthetic merchant universe across all configured cities.

    Args:
        rng: Random generator instance.
        merchants_per_city: Number of merchants to generate per city.

    Returns:
        Dictionary mapping merchant IDs to Merchant instances.
    """
    merchants: dict[str, Merchant] = {}
    idx = 0
    for city in ALL_CITIES:
        chosen = rng.choices(MCCS, weights=[float(m["w"]) for m in MCCS], k=merchants_per_city)
        for pos, mcc in enumerate(chosen):
            code_str = str(mcc["code"])
            names = MERCHANT_NAMES_BY_MCC[code_str]
            name = names[pos % len(names)]
            m = Merchant(f"M{idx:05d}", name, code_str, city, f"T{idx:07d}")
            merchants[m.merchant_id] = m
            idx += 1
    return merchants


def _pick_merchant(rng: random.Random, merchants: dict[str, Merchant], city: str, code: str | None) -> Merchant:
    """Selects merchant matching city and optional MCC filter.

    Args:
        rng: Random generator instance.
        merchants: Merchant universe dictionary.
        city: Target city name.
        code: Optional MCC code filter.

    Returns:
        Selected Merchant instance.
    """
    matches = [m for m in merchants.values() if m.city == city and (code is None or m.code == code)]
    if not matches:
        matches = [m for m in merchants.values() if m.city == city] or list(merchants.values())
    return rng.choice(matches)


def _make_tx(
    rng: random.Random,
    cust: CustomerProfile,
    ts: datetime,
    merchant: Merchant,
    amount: float | None = None,
    force_declined: bool = False,
) -> dict[str, Any]:
    """Constructs single synthetic transaction dictionary.

    Args:
        rng: Random generator instance.
        cust: Customer profile.
        ts: Transaction timestamp.
        merchant: Merchant entity.
        amount: Optional explicit amount; sampled if None.
        force_declined: Forces authorization decline flag.

    Returns:
        Transaction dictionary matching canonical schema.
    """
    mcc = next(m for m in MCCS if m["code"] == merchant.code)
    if amount is None:
        amount = _rng_amount(mcc, rng)
    online = merchant.code in ("5816", "6051", "7995", "4722", "4814") and rng.random() < 0.5
    if online:
        entry, channel, device = "ecommerce", "online", (rng.choice(cust.devices) if cust.devices else None)
    elif merchant.code == "4121":
        entry, channel, device = "contactless", "pos", None
    else:
        roll = rng.random()
        if roll < 0.82:
            entry, channel = "chip", "pos"
        elif roll < 0.93:
            entry, channel = "contactless", "pos"
        else:
            entry, channel = "magnetic", "pos"
        device = (rng.choice(cust.devices) if cust.devices else None) if rng.random() < 0.6 else None
    declined = force_declined or (rng.random() < 0.006 and merchant.code not in ("7995", "6051", "5816"))
    return {
        "transaction_id": f"tx_{rng.randint(10**11, 10**12):012d}",
        "customer_id": cust.customer_id,
        "card_id": cust.card_id,
        "timestamp": ts,
        "amount": round(amount, 2),
        "currency": "BRL",
        "merchant_id": merchant.merchant_id,
        "merchant_category": merchant.code,
        "merchant_city": merchant.city,
        "merchant_country": "BR",
        "terminal_id": merchant.terminal_id,
        "entry_mode": entry,
        "device_id": device,
        "channel": channel,
        "auth_result": "declined" if declined else "approved",
    }


def build_customers(rng: random.Random, n_customers: int) -> list[CustomerProfile]:
    """Generates customer profiles with assigned home cities and registered devices.

    Args:
        rng: Random generator instance.
        n_customers: Total number of customer profiles to create.

    Returns:
        List of generated CustomerProfile instances.
    """
    return [
        CustomerProfile(
            f"C{c:06d}",
            rng.choices(POPULAR_CITIES, weights=[5, 4, 3, 2, 2, 2, 2, 3], k=1)[0],
            _card_id(rng),
            [_device(rng) for _ in range(rng.randint(1, 3))],
        )
        for c in range(n_customers)
    ]


def attach_merchants(customers: list[CustomerProfile], merchants: dict[str, Merchant], rng: random.Random) -> None:
    """Attaches preferred local merchants to each customer profile.

    Args:
        customers: List of customer profiles.
        merchants: Merchant universe dictionary.
        rng: Random generator instance.
    """
    for cust in customers:
        home = [m for m in merchants.values() if m.city == cust.home_city]
        pool = home if len(home) >= 3 else list(merchants.values())
        cust.merchants = rng.sample(pool, min(8, len(pool)))


def generate_normal_transactions(
    rng: random.Random,
    customers: list[CustomerProfile],
    merchants: dict[str, Merchant],
    now: datetime,
) -> list[dict[str, Any]]:
    """Generates baseline regular transactions with daily routines and plausible travel.

    Args:
        rng: Random generator instance.
        customers: List of customer profiles.
        merchants: Merchant universe dictionary.
        now: Reference timestamp for dataset cutoff.

    Returns:
        List of regular transaction dictionaries sorted by timestamp.
    """
    start = now - timedelta(days=45)
    txs: list[dict[str, Any]] = []
    for cust in customers:
        t = start + timedelta(hours=rng.uniform(0, 24))
        while t < now and cust.n_regular_tx < 55:
            for _ in range(rng.randint(1, 5)):
                t += timedelta(minutes=rng.uniform(25, 400))
                if t >= now:
                    break
                merchant = cust.merchants[rng.randrange(len(cust.merchants))]
                txs.append(_make_tx(rng, cust, t, merchant))
                cust.n_regular_tx += 1
            if rng.random() < 0.08:
                dest = rng.choice([c for c in POPULAR_CITIES if c != cust.home_city])
                dist_km = haversine_km(*CITY_COORDS[cust.home_city], *CITY_COORDS[dest])
                travel_h = (dist_km / 480.0) + rng.uniform(1.0, 4.0)
                t += timedelta(hours=travel_h)
                if t >= now:
                    break
                for _ in range(rng.randint(2, 4)):
                    if t >= now:
                        break
                    merchant = _pick_merchant(rng, merchants, dest, None)
                    txs.append(_make_tx(rng, cust, t, merchant))
                    cust.n_regular_tx += 1
                    t += timedelta(hours=rng.uniform(1, 8))
                t += timedelta(hours=travel_h)
            else:
                t += timedelta(hours=rng.uniform(5, 22))
    txs.sort(key=lambda r: (r["timestamp"], r["transaction_id"]))
    return txs


def generate_fraud_sequences(
    rng: random.Random,
    customers: list[CustomerProfile],
    merchants: dict[str, Merchant],
    now: datetime,
    n_cloning: int,
    n_travel: int,
    n_bin: int,
    n_dormant: int,
    n_new_device: int,
    n_unusual: int,
) -> tuple[list[dict[str, Any]], list[tuple[str, str]]]:
    """Generates synthetic fraud attack sequences and matching ground-truth labels.

    Args:
        rng: Random generator instance.
        customers: List of customer profiles.
        merchants: Merchant universe dictionary.
        now: Reference cutoff timestamp.
        n_cloning: Number of card cloning attack sequences.
        n_travel: Number of impossible travel sequences.
        n_bin: Number of BIN attack sequences.
        n_dormant: Number of dormant card wake-up sequences.
        n_new_device: Number of high amount new device sequences.
        n_unusual: Number of unusual hour/place sequences.

    Returns:
        Tuple containing list of fraud transaction records and list of (transaction_id, fraud_type) labels.
    """
    txs: list[dict[str, Any]] = []
    labels: list[tuple[str, str]] = []
    eligible = [c for c in customers if c.n_regular_tx >= 10]

    for cust in rng.sample(eligible, min(n_cloning, len(eligible))):
        ts = now - timedelta(hours=rng.uniform(1, 48))
        ts = ts.replace(hour=14, minute=rng.randint(0, 59))
        test_merchant = _pick_merchant(rng, merchants, cust.home_city, "5999")
        test_tx = _make_tx(rng, cust, ts, test_merchant, amount=round(rng.uniform(1.5, 8.0), 2), force_declined=False)
        test_tx["entry_mode"] = "magnetic"
        test_tx["device_id"] = None
        txs.append(test_tx)
        labels.append((test_tx["transaction_id"], "card_cloning"))
        big_ts = ts + timedelta(minutes=rng.uniform(2, 9))
        big_merchant = _pick_merchant(rng, merchants, cust.home_city, rng.choice(["5732", "5944", "5311"]))
        big_tx = _make_tx(rng, cust, big_ts, big_merchant, amount=round(test_tx["amount"] * rng.uniform(8, 30), 2))
        big_tx["entry_mode"] = "magnetic"
        big_tx["device_id"] = None
        txs.append(big_tx)
        labels.append((big_tx["transaction_id"], "card_cloning"))

    for cust in rng.sample(eligible, min(n_travel, len(eligible))):
        ts = now - timedelta(hours=rng.uniform(1, 48))
        home_tx = _make_tx(rng, cust, ts, _pick_merchant(rng, merchants, cust.home_city, "5541"))
        txs.append(home_tx)
        far = "Manaus" if cust.home_city != "Manaus" else "Porto Alegre"
        gap_min = rng.uniform(5, 28)
        far_merchant = _pick_merchant(rng, merchants, far, "5944")
        far_tx = _make_tx(rng, cust, ts + timedelta(minutes=gap_min), far_merchant, amount=rng.uniform(1500, 12000))
        far_tx["entry_mode"] = "chip"
        txs.append(far_tx)
        labels.append((far_tx["transaction_id"], "impossible_travel"))

    for _ in range(n_bin):
        bin_id = rng.choice(BIN_POOL)
        start = now - timedelta(hours=rng.uniform(1, 30))
        for i in range(rng.randint(4, 6)):
            ts = start + timedelta(minutes=i * rng.uniform(0.5, 2.2))
            fake_card = f"{bin_id}{rng.randint(1000000000, 9999999999)}"
            merchant = _pick_merchant(rng, merchants, rng.choice(POPULAR_CITIES), "5999")
            tx = _make_tx(
                rng,
                CustomerProfile(f"UNK{rng.randint(1, 9999)}", rng.choice(POPULAR_CITIES), fake_card, []),
                ts,
                merchant,
                amount=round(rng.uniform(3, 50), 2),
            )
            tx["transaction_id"] = f"tx_bin_{rng.randint(10**11, 10**12):012d}"
            tx["entry_mode"] = "ecommerce"
            tx["channel"] = "online"
            tx["device_id"] = None
            tx["auth_result"] = "declined"
            txs.append(tx)
            labels.append((tx["transaction_id"], "bin_attack"))

    for cust in rng.sample([c for c in eligible if c.n_regular_tx > 5], min(n_dormant, len(eligible))):
        old_ts = now - timedelta(days=rng.uniform(200, 320))
        txs.append(_make_tx(rng, cust, old_ts, _pick_merchant(rng, merchants, cust.home_city, "5411")))
        wake_ts = now - timedelta(hours=rng.uniform(2, 30))
        wake_tx = _make_tx(
            rng,
            cust,
            wake_ts,
            _pick_merchant(rng, merchants, cust.home_city, "5944"),
            amount=rng.uniform(1500, 9000),
        )
        wake_tx["entry_mode"] = "chip"
        txs.append(wake_tx)
        labels.append((wake_tx["transaction_id"], "dormant_card_wake"))

    for cust in rng.sample(list(eligible), min(n_new_device, len(eligible))):
        ts = now - timedelta(hours=rng.uniform(1, 48))
        tx = _make_tx(
            rng,
            cust,
            ts,
            _pick_merchant(rng, merchants, cust.home_city, "5732"),
            amount=rng.uniform(1500, 6000),
        )
        tx["device_id"] = f"DVR-NEW-{rng.randint(1, 50000)}"
        tx["entry_mode"] = "chip"
        txs.append(tx)
        labels.append((tx["transaction_id"], "new_device_high_amount"))

    for cust in rng.sample(list(eligible), min(n_unusual, len(eligible))):
        ts = now - timedelta(hours=rng.uniform(1, 48))
        ts = ts.replace(hour=rng.randint(2, 5))
        new_city = rng.choice([c for c in ALL_CITIES if c != cust.home_city])
        tx = _make_tx(rng, cust, ts, _pick_merchant(rng, merchants, new_city, "5813"), amount=rng.uniform(80, 400))
        txs.append(tx)
        labels.append((tx["transaction_id"], "unusual_hour_and_place"))

    return txs, labels


def generate(
    spark: SparkSession | None = None,
    n_transactions: int = 100000,
    n_cloning: int = 500,
    n_travel: int = 200,
    n_bin: int = 100,
    n_dormant: int = 60,
    n_new_device: int = 80,
    n_unusual: int = 70,
    seed: int = SEED,
    output_dir: str = "data",
    write: bool = True,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Generates synthetic transactions and metadata artifacts.

    Args:
        spark: Optional active SparkSession instance.
        n_transactions: Target total transaction count.
        n_cloning: Number of card cloning attack samples.
        n_travel: Number of impossible travel samples.
        n_bin: Number of BIN attack sequences.
        n_dormant: Number of dormant card wake samples.
        n_new_device: Number of new device attack samples.
        n_unusual: Number of unusual hour/place samples.
        seed: Random generator seed.
        output_dir: Output base directory path.
        write: Whether to persist generated artifacts to parquet and json files.

    Returns:
        Tuple of (list of all generated records, summary metrics dictionary).
    """
    rng = random.Random(seed)
    merchants = build_world(rng)
    now = datetime.now(timezone.utc).replace(microsecond=0)

    n_customers = max(60, min(3000, n_transactions // 40))
    customers = build_customers(rng, n_customers)
    attach_merchants(customers, merchants, rng)

    normal = generate_normal_transactions(rng, customers, merchants, now)
    fraud, labels = generate_fraud_sequences(
        rng,
        customers,
        merchants,
        now,
        n_cloning=n_cloning,
        n_travel=n_travel,
        n_bin=n_bin,
        n_dormant=n_dormant,
        n_new_device=n_new_device,
        n_unusual=n_unusual,
    )

    if n_transactions and n_transactions > 0:
        if len(fraud) < n_transactions:
            normal = normal[: (n_transactions - len(fraud))]
            all_rows = normal + fraud
        else:
            all_rows = (normal + fraud)[:n_transactions]
    else:
        all_rows = normal + fraud

    all_rows.sort(key=lambda r: (r["timestamp"], r["transaction_id"]))
    all_rows = [dict(r, amount=round(float(r["amount"]), 2)) for r in all_rows]

    out_dir = Path(output_dir)
    meta_dir = out_dir / "metadata"
    meta_dir.mkdir(parents=True, exist_ok=True)

    if write:
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        pdf = pd.DataFrame(all_rows)
        ts_s = pd.to_datetime(pdf["timestamp"])
        if hasattr(ts_s.dt, "tz") and ts_s.dt.tz is not None:
            ts_s = ts_s.dt.tz_convert("UTC").dt.tz_localize(None)
        pdf["timestamp"] = ts_s.astype("datetime64[us]")
        table = pa.Table.from_pandas(pdf)
        pq.write_table(table, str(out_dir / "seed_transactions.parquet"))

        label_map: dict[str, str] = {}
        for tx_id, ftype in labels:
            label_map[tx_id] = ftype
        label_rows = [
            {
                "transaction_id": str(r["transaction_id"]),
                "is_fraud": str(r["transaction_id"]) in label_map,
                "fraud_type": label_map.get(str(r["transaction_id"]), "none"),
            }
            for r in all_rows
        ]
        label_pdf = pd.DataFrame(label_rows)
        label_table = pa.Table.from_pandas(label_pdf)
        pq.write_table(label_table, str(meta_dir / "fraud_labels.parquet"))

        name_map = {mid: m.name for mid, m in merchants.items()}
        (meta_dir / "merchants.json").write_text(json.dumps(name_map, ensure_ascii=False), encoding="utf-8")

    summary = {
        "normal": len(normal),
        "fraud": len(fraud),
        "customers": n_customers,
        "seed": seed,
        "written": len(all_rows),
    }
    return all_rows, summary
