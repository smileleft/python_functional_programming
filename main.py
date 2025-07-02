import asyncio
import random
import string
from datetime import datetime, timedelta
from typing import List, Dict
from functools import partial
from copy import deepcopy
from toolz import pipe, keyfilter, valfilter, groupby


# ----------------------------
# Mock Data generate
# ----------------------------

def random_string(length=8):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def mock_payment_data(shop_id: str, count: int) -> List[Dict]:
    now = datetime.now()
    return [
        {
            "pay_id": f"ORDER_{i}",
            "card_number": f"****-****-****-{random.randint(1000,9999)}",
            "created_at": (now - timedelta(seconds=random.randint(0, 300))).isoformat(),
            "total_price": random.randint(1000, 50000),
            "shop_id": shop_id
        }
        for i in range(count)
    ]

def mock_order_data(count: int) -> List[Dict]:
    now = datetime.now()
    return [
        {
            "order_id": f"ORDER_{i}",
            "order_item": random_string(5),
            "customer_id": random_string(6),
            "card_number": f"****-****-****-{random.randint(1000,9999)}",
            "created_at": (now - timedelta(seconds=random.randint(0, 600))).isoformat(),
            "is_completed": False
        }
        for i in range(count)
    ]


def is_uncompleted(order: Dict) -> bool:
    """Order completion filter"""
    return not order["is_completed"]

def create_payment_matcher(pay_ids: set):
    """payment matching function"""
    return lambda order: order["order_id"] in pay_ids

def mark_completed(order: Dict) -> Dict:
    """Update order status as completed"""
    return {**order, "is_completed": True}

def sync_orders_advanced_fp(orders: List[Dict], payments: List[Dict], shop_filter=None) -> List[Dict]:
    # filtering and generate set of pay_id
    pay_ids = pipe(
        payments,
        cfilter(lambda p: p["shop_id"] == shop_id),
        cmap(lambda p: p["pay_id"]),
        set
    )

    needs_update = compose(
        create_payment_matcher(pay_ids),
        lambda o: o if is_uncompleted(o) else None
    )

    def process_order(order):
        if order and needs_update(order):
            return mark_completed(order)
        return order

    updated_orders = list(map(process_order, orders))

    return updated_orders    



async def periodic_advanced_sync():
    orders = mock_order_data(30)
    shop_id = "SHOP_001"

    print(f"[Initial State] count of order: {sum(1 for o in orders if o['is_completed'])}")

    while True:
        start_time = datetime.now()
        print("\n[Requesting payment information...]")

        payments = mock_payment_data(shop_id, random.randint(10, 20))

        orders = sync_orders_advanced(orders, payments, shop_filter=shop_id)

        completed_count = sum(1 for o in orders if o['is_completed'])
        pending_count = len(orders) - completed_count

        # groupby
        grouped = groupby(lambda o: o['is_completed'], orders)

        print(f"[{datetime.now().isoformat()}] status summary:")
        print(f" completed  {len(grouped.get(True, []))} / not completed {len(grouped.get(False, []))}")

        elapsed = (datetime.now() - start_time).total_seconds()
        wait_time = max(0, 5 - elapsed)
        await asyncio.sleep(wait_time)


if __name__ == "__main__":
    asyncio.run(periodic_advanced_sync())
