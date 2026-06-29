"""Shared constants for task queue names.

Centralised here so every module (api.py, worker.py, workflows.py, tests)
imports from one place.  A rename is a one-line change that propagates
everywhere; an independent redefinition in each file creates the risk of
a silent divergence that surfaces as a "no worker registered" stall.
"""

# Main task queue: workflow execution + all general activities.
OMS_QUEUE = "OMS_QUEUE"

# Dedicated queue for validate_order_api only, capped at 150 RPS.
# Do NOT add other activities here — it throttles them to 150 RPS as well.
COMMERCE_QUEUE = "COMMERCE_QUEUE"

# Temporal Search Attribute name for filtering open workflows by business status.
# Must be registered in the namespace before use — see temporal-init in docker-compose.yml.
ORDER_STATUS_ATTR = "OrderStatus"
