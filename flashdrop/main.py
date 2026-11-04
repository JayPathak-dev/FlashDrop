"""
FlashDrop - High-Concurrency Order Processing Backend
=====================================================
A simple, robust, and beginner-friendly FastAPI backend designed to handle
flash sales where multiple concurrent users compete for limited product inventory.

Key Concepts Demonstrated:
- Concurrency Protection: Thread-safe check-and-deduct using `threading.Lock`.
- Idempotency: Duplicate prevention using in-memory `(userId, idempotencyKey)` tracking.
- Asynchronous Processing: Decoupling order acceptance from downstream fulfillment
  using `queue.Queue` and a background `threading.Thread` worker.
- Clean Lifespan Management: Graceful background worker startup and shutdown.
"""

import logging
import queue
import threading
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

# -----------------------------------------------------------------------------
# 1. Logging Configuration
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
)
logger = logging.getLogger("flashdrop")

# -----------------------------------------------------------------------------
# 2. In-Memory Data Stores & Synchronization Primitives
# -----------------------------------------------------------------------------
# Initial product inventory: {product_id: available_stock}
inventory: Dict[str, int] = {
    "iphone_16": 100
}

# In-memory orders dictionary: {order_id: order_dict}
orders: Dict[str, Dict[str, Any]] = {}

# In-memory idempotency store: {(user_id, idempotency_key): record_dict}
idempotency_store: Dict[tuple, Dict[str, Any]] = {}

# In-memory queue to simulate asynchronous order processing
order_queue: queue.Queue = queue.Queue()

# Shared threading lock to protect all shared state across threads
lock: threading.Lock = threading.Lock()

# Event flag to signal clean shutdown to the background worker thread
stop_event: threading.Event = threading.Event()


# -----------------------------------------------------------------------------
# 3. Pydantic Models for Request Validation & Responses
# -----------------------------------------------------------------------------
class OrderCreateRequest(BaseModel):
    """
    Validates the incoming order creation request body.
    - userId: non-empty string identifying the customer.
    - productId: non-empty string identifying the product.
    - quantity: positive integer (> 0).
    - idempotencyKey: client-provided unique string to prevent duplicate orders.
    """
    userId: str = Field(..., min_length=1, description="Unique user identifier", examples=["user_123"])
    productId: str = Field(..., min_length=1, description="Product identifier", examples=["iphone_16"])
    quantity: int = Field(..., gt=0, description="Quantity to purchase (must be > 0)", examples=[1])
    idempotencyKey: str = Field(..., min_length=1, description="Unique idempotency key", examples=["request_abc123"])


class OrderResponse(BaseModel):
    """
    Standard order response schema returned to clients.
    """
    orderId: str = Field(..., description="Unique order identifier")
    status: str = Field(..., description="Current status of the order (PENDING, PROCESSING, CONFIRMED)")


# -----------------------------------------------------------------------------
# 4. Asynchronous Background Worker
# -----------------------------------------------------------------------------
def background_worker() -> None:
    """
    Background worker loop running on a dedicated thread.
    Pulls order IDs from the queue and simulates order fulfillment:
      1. Receives order ID.
      2. Updates status to 'PROCESSING'.
      3. Simulates fulfillment work (time.sleep(2)).
      4. Updates status to 'CONFIRMED'.
    """
    logger.info("Background worker started and listening to queue.")
    while not stop_event.is_set():
        try:
            # Poll with a short timeout (0.2s) so that shutdown signals (stop_event)
            # are detected promptly without leaving lingering sentinels in the queue.
            order_id = order_queue.get(timeout=0.2)
        except queue.Empty:
            continue

        try:
            logger.info(f"Worker picked up order {order_id}.")

            # Step 1: Update status to PROCESSING under lock
            with lock:
                if order_id in orders:
                    orders[order_id]["status"] = "PROCESSING"
                    logger.info(f"Order {order_id} status updated to PROCESSING.")

            # Step 2: Simulate processing latency (e.g. payment gateway, warehouse notification)
            time.sleep(2)

            # Step 3: Update status to CONFIRMED under lock
            with lock:
                if order_id in orders:
                    orders[order_id]["status"] = "CONFIRMED"
                    logger.info(f"Order {order_id} status updated to CONFIRMED.")

        except Exception as exc:
            logger.error(f"Unexpected error while processing order {order_id}: {exc}")
        finally:
            order_queue.task_done()

    logger.info("Background worker stopped.")


# -----------------------------------------------------------------------------
# 5. FastAPI Application Lifespan (Startup & Shutdown)
# -----------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the lifecycle of the FastAPI application:
    - Startup: starts the background worker thread.
    - Shutdown: signals the worker to exit cleanly and waits for it to finish.
    """
    # Startup: Start worker thread
    stop_event.clear()
    worker_thread = threading.Thread(
        target=background_worker,
        name="OrderProcessingWorker",
        daemon=True,
    )
    worker_thread.start()
    logger.info("Application startup completed: worker thread active.")

    yield  # Application runs and handles HTTP requests

    # Shutdown: Signal worker thread to terminate
    logger.info("Application shutdown initiated: terminating worker thread...")
    stop_event.set()
    worker_thread.join(timeout=3.0)
    logger.info("Application shutdown completed: worker thread terminated.")


# -----------------------------------------------------------------------------
# 6. FastAPI Application Definition
# -----------------------------------------------------------------------------
app = FastAPI(
    title="FlashDrop Order Processing API",
    description="High-concurrency flash sale backend demonstrating thread safety, idempotency, and async queues.",
    version="1.0.0",
    lifespan=lifespan,
)


# -----------------------------------------------------------------------------
# 7. API Endpoints
# -----------------------------------------------------------------------------
@app.post(
    "/orders",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=OrderResponse,
    summary="Create a new order",
    description=(
        "Handles flash-sale order placement with idempotency checks, atomic inventory deduction, "
        "and asynchronous background processing."
    ),
)
def create_order(request: OrderCreateRequest):
    """
    Atomic Order Creation Workflow:
    1. Check idempotency:
       - If (userId, idempotencyKey) already exists with matching payload, return existing order (202).
       - If exists with different payload, return 409 Conflict.
    2. Check product exists (return 404 if not found).
    3. Check available inventory (return 409 if insufficient).
    4. Deduct inventory atomically under lock.
    5. Generate unique order ID and record order with 'PENDING' status.
    6. Record idempotency entry.
    7. Enqueue order ID for background processing.
    8. Return order ID and 'PENDING' status immediately.
    """
    composite_key = (request.userId, request.idempotencyKey)

    # Protect all checks, deductions, and dictionary mutations inside a single lock
    with lock:
        # Step 1: Idempotency check
        if composite_key in idempotency_store:
            record = idempotency_store[composite_key]
            # Verify if request parameters match the original request
            if (
                record["productId"] == request.productId
                and record["quantity"] == request.quantity
            ):
                existing_order_id = record["orderId"]
                existing_order = orders.get(existing_order_id, {})
                current_status = existing_order.get("status", "PENDING")
                logger.info(
                    f"Idempotent retry detected for user={request.userId}, "
                    f"key={request.idempotencyKey}. Returning existing order {existing_order_id}."
                )
                return OrderResponse(orderId=existing_order_id, status=current_status)
            else:
                logger.warning(
                    f"Idempotency conflict for user={request.userId}, "
                    f"key={request.idempotencyKey}: payload mismatch."
                )
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Idempotency key has already been used with different request parameters.",
                )

        # Step 2: Validate that the requested product exists
        if request.productId not in inventory:
            logger.warning(f"Order rejected: Product '{request.productId}' not found.")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product '{request.productId}' not found.",
            )

        # Step 3: Check whether sufficient stock is available
        available_stock = inventory[request.productId]
        if available_stock < request.quantity:
            logger.warning(
                f"Order rejected: Insufficient stock for '{request.productId}'. "
                f"Requested: {request.quantity}, Available: {available_stock}."
            )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Insufficient inventory for product '{request.productId}'.",
            )

        # Step 4: Deduct stock safely (cannot become negative due to lock + check above)
        inventory[request.productId] -= request.quantity
        remaining_stock = inventory[request.productId]
        logger.info(
            f"Inventory deducted: {request.quantity} unit(s) of '{request.productId}'. "
            f"Remaining stock: {remaining_stock}."
        )

        # Step 5: Generate a unique order ID and store order with PENDING status
        order_id = f"ord_{uuid.uuid4().hex[:12]}"
        orders[order_id] = {
            "orderId": order_id,
            "userId": request.userId,
            "productId": request.productId,
            "quantity": request.quantity,
            "status": "PENDING",
        }

        # Step 6: Store idempotency record
        idempotency_store[composite_key] = {
            "orderId": order_id,
            "productId": request.productId,
            "quantity": request.quantity,
        }

        # Step 7: Push order ID to the background queue for asynchronous fulfillment
        order_queue.put(order_id)
        logger.info(f"Order {order_id} enqueued for background processing.")

    # Step 8: Return immediate response to the client
    return OrderResponse(orderId=order_id, status="PENDING")


@app.get(
    "/orders/{order_id}",
    response_model=OrderResponse,
    summary="Check order status",
    description="Retrieves the current status of an order by its unique order ID.",
)
def get_order_status(order_id: str):
    """
    Retrieves the status of an existing order.
    Returns:
    - 200 OK with OrderResponse if found.
    - 404 Not Found if order does not exist.
    """
    with lock:
        order = orders.get(order_id)
        if not order:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order '{order_id}' not found.",
            )
        return OrderResponse(orderId=order["orderId"], status=order["status"])


@app.get(
    "/inventory",
    summary="Get current inventory snapshot",
    description="Utility endpoint to inspect current stock levels for testing and debugging.",
)
def get_inventory():
    """
    Returns a snapshot of available inventory for all products.
    """
    with lock:
        return {"inventory": inventory.copy()}
