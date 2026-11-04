"""
FlashDrop - Concurrency and Functionality Test Suite
===================================================
Demonstrates and validates:
1. Concurrency Safety: Multiple concurrent threads compete for the last item;
   only ONE succeeds (202), all others fail with 409, and inventory never goes negative.
2. Idempotency Handling:
   - Same key + same payload -> returns existing order without duplicate deduction.
   - Same key + different payload -> returns 409 Conflict.
3. Background Worker:
   - Order transitions from PENDING -> PROCESSING -> CONFIRMED.
4. Input Validation & Error Responses:
   - 422 for invalid inputs, 404 for unknown products/orders.
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi.testclient import TestClient

# Import the FastAPI app and shared state from main
from main import app, inventory, orders, idempotency_store, lock


def run_all_tests():
    with TestClient(app) as client:
        # ---------------------------------------------------------------------
        # TEST 1: Concurrency Protection (Competing for the Last Item)
        # ---------------------------------------------------------------------
        print("\n" + "=" * 70)
        print("TEST 1: Concurrency Protection (Competing for the Last Item)")
        print("=" * 70)

        # Reset inventory to exactly 1 item for testing the race condition
        with lock:
            inventory["iphone_16"] = 1
            orders.clear()
            idempotency_store.clear()

        print(f"Initial available stock for 'iphone_16': {inventory['iphone_16']}")
        num_competing_threads = 10
        print(f"Launching {num_competing_threads} simultaneous requests competing for 1 item...")

        def place_order(user_idx: int):
            payload = {
                "userId": f"user_{user_idx}",
                "productId": "iphone_16",
                "quantity": 1,
                "idempotencyKey": f"idemp_key_{user_idx}",
            }
            response = client.post("/orders", json=payload)
            return user_idx, response.status_code, response.json()

        results = []
        with ThreadPoolExecutor(max_workers=num_competing_threads) as executor:
            futures = [executor.submit(place_order, i) for i in range(1, num_competing_threads + 1)]
            for future in as_completed(futures):
                results.append(future.result())

        success_count = sum(1 for _, status, _ in results if status == 202)
        conflict_count = sum(1 for _, status, _ in results if status == 409)

        print("\nResults:")
        for user_idx, status_code, body in sorted(results, key=lambda x: x[0]):
            print(f"  User {user_idx:02d} -> HTTP {status_code} | Body: {body}")

        print(f"\nSummary:")
        print(f"  Total requests:     {num_competing_threads}")
        print(f"  Successful (202):   {success_count}")
        print(f"  Rejected (409):     {conflict_count}")
        print(f"  Final stock level:  {inventory['iphone_16']}")

        assert success_count == 1, f"Expected exactly 1 order to succeed, got {success_count}!"
        assert conflict_count == num_competing_threads - 1, f"Expected {num_competing_threads - 1} 409s, got {conflict_count}!"
        assert inventory["iphone_16"] == 0, f"Expected final inventory 0, got {inventory['iphone_16']}!"

        print(">>> SUCCESS: Concurrency lock successfully prevented overselling! No negative stock.")

        # ---------------------------------------------------------------------
        # TEST 2: Idempotency Protection
        # ---------------------------------------------------------------------
        print("\n" + "=" * 70)
        print("TEST 2: Idempotency Protection")
        print("=" * 70)

        with lock:
            inventory["iphone_16"] = 10
            orders.clear()
            idempotency_store.clear()

        initial_stock = inventory["iphone_16"]
        payload = {
            "userId": "buyer_bob",
            "productId": "iphone_16",
            "quantity": 2,
            "idempotencyKey": "bob_order_uuid_1001",
        }

        # 1. First attempt: should succeed
        resp1 = client.post("/orders", json=payload)
        assert resp1.status_code == 202, f"Expected 202, got {resp1.status_code}"
        order_id_1 = resp1.json()["orderId"]
        print(f"1. First request -> HTTP {resp1.status_code}, Order ID: {order_id_1}")
        assert inventory["iphone_16"] == initial_stock - 2

        # 2. Duplicate retry with same key and identical payload
        resp2 = client.post("/orders", json=payload)
        assert resp2.status_code == 202, f"Expected 202, got {resp2.status_code}"
        order_id_2 = resp2.json()["orderId"]
        print(f"2. Duplicate request with SAME payload -> HTTP {resp2.status_code}, Order ID: {order_id_2}")
        assert order_id_1 == order_id_2, "Order IDs must match for idempotent retry!"
        assert inventory["iphone_16"] == initial_stock - 2, "Stock must NOT be deducted again!"

        # 3. Duplicate request with same key but DIFFERENT payload (quantity changed to 3)
        conflicting_payload = dict(payload, quantity=3)
        resp3 = client.post("/orders", json=conflicting_payload)
        print(f"3. Duplicate request with DIFFERENT payload -> HTTP {resp3.status_code} ({resp3.json()})")
        assert resp3.status_code == 409, f"Expected 409 Conflict, got {resp3.status_code}"

        print(">>> SUCCESS: Idempotency handling verified! No duplicate deductions or payload mismatches.")

        # ---------------------------------------------------------------------
        # TEST 3: Asynchronous Worker Order Lifecycle
        # ---------------------------------------------------------------------
        print("\n" + "=" * 70)
        print("TEST 3: Asynchronous Worker Order Lifecycle")
        print("=" * 70)

        with lock:
            inventory["iphone_16"] = 5

        payload_async = {
            "userId": "buyer_carol",
            "productId": "iphone_16",
            "quantity": 1,
            "idempotencyKey": "carol_order_async",
        }

        resp_async = client.post("/orders", json=payload_async)
        assert resp_async.status_code == 202
        async_order_id = resp_async.json()["orderId"]
        initial_status = resp_async.json()["status"]
        print(f"Order created: {async_order_id}, Initial Status: {initial_status}")
        assert initial_status == "PENDING"

        print("Polling order status as background worker processes the queue...")
        max_wait = 10.0
        start_time = time.time()
        final_status = initial_status
        while time.time() - start_time < max_wait:
            status_resp = client.get(f"/orders/{async_order_id}")
            assert status_resp.status_code == 200
            current_status = status_resp.json()["status"]
            if current_status != final_status:
                print(f"  Order {async_order_id} status transitioned to: {current_status}")
                final_status = current_status
            if final_status == "CONFIRMED":
                break
            time.sleep(0.5)

        print(f"Order {async_order_id} reached final status: {final_status}")
        assert final_status == "CONFIRMED", f"Expected CONFIRMED, got {final_status}"
        print(">>> SUCCESS: Background worker correctly transitioned order to CONFIRMED.")

        # ---------------------------------------------------------------------
        # TEST 4: Error Handling & HTTP Status Codes
        # ---------------------------------------------------------------------
        print("\n" + "=" * 70)
        print("TEST 4: Error Handling & HTTP Status Codes")
        print("=" * 70)

        # Invalid input: negative quantity -> 422
        bad_qty_payload = {
            "userId": "user_1",
            "productId": "iphone_16",
            "quantity": -2,
            "idempotencyKey": "bad_qty_key",
        }
        resp = client.post("/orders", json=bad_qty_payload)
        print(f"Negative quantity -> HTTP {resp.status_code}")
        assert resp.status_code == 422

        # Nonexistent product -> 404
        bad_product_payload = {
            "userId": "user_1",
            "productId": "playstation_6",
            "quantity": 1,
            "idempotencyKey": "bad_prod_key",
        }
        resp = client.post("/orders", json=bad_product_payload)
        print(f"Unknown product -> HTTP {resp.status_code} ({resp.json()})")
        assert resp.status_code == 404

        # Nonexistent order ID -> 404
        resp = client.get("/orders/nonexistent_order_id_12345")
        print(f"Unknown order ID -> HTTP {resp.status_code} ({resp.json()})")
        assert resp.status_code == 404

        print(">>> SUCCESS: All error status codes (404, 409, 422) verified.")


if __name__ == "__main__":
    try:
        run_all_tests()
        print("\n" + "*" * 70)
        print("ALL TESTS PASSED SUCCESSFULLY!")
        print("*" * 70)
    except AssertionError as err:
        print(f"\nTEST FAILED: {err}")
        sys.exit(1)
