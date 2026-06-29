"""Bootstraps two Temporal workers on a single client.

  OMS_QUEUE       — handles workflow tasks and all general activities (no rate cap)
  COMMERCE_QUEUE  — dedicated queue for Commerce API validation, capped at 150 RPS

Splitting queues isolates the Commerce API rate limit from other high-throughput
activities (PIM enrichment, Kafka publish, dashboard writes).

"""

import asyncio
import logging
import os

from temporalio.client import Client
from temporalio.common import VersioningBehavior, WorkerDeploymentVersion
from temporalio.worker import Worker, WorkerDeploymentConfig

from oms.activities import OmsActivities
from oms.constants import COMMERCE_QUEUE, OMS_QUEUE
from oms.workflows import OrderProcessingWorkflow


def _deployment_config(deployment_name: str) -> WorkerDeploymentConfig | None:
    """Return a WorkerDeploymentConfig if BUILD_ID is set, otherwise None.

    Workers started without a deployment config are unversioned and will race
    with any other unversioned worker on the same task queue during rolling
    deploys.  If old and new code differ in their Command sequence for an open
    execution, a NonDeterminismException occurs.

    ENV behaviour:
      - dev / test / local (default): logs a WARNING and returns None so the
        worker starts without versioning.  Acceptable for local development
        where there is no rolling deploy.
      - any other value (staging, production, …): raises RuntimeError at
        startup so a misconfigured deploy fails fast rather than silently
        racing against other workers.
    """
    build_id = os.environ.get("BUILD_ID")
    if not build_id:
        env = os.environ.get("ENV", "dev").lower()
        if env not in ("dev", "test", "local"):
            raise RuntimeError(
                f"BUILD_ID is required in {env!r} environment but was not set. "
                "Unversioned workers race with each other during rolling deploys and "
                "can cause NonDeterminismException on open workflow executions. "
                "Set BUILD_ID to an immutable identifier for this build "
                "(e.g. a Git SHA or CI job number)."
            )
        logging.warning(
            "BUILD_ID not set — starting UNVERSIONED worker for deployment %r (ENV=%r). "
            "This is only safe in local development. "
            "Always set BUILD_ID in staging and production environments.",
            deployment_name,
            env,
        )
        return None
    return WorkerDeploymentConfig(
        version=WorkerDeploymentVersion(
            deployment_name=deployment_name,
            build_id=build_id,
        ),
        use_worker_versioning=True,
        
        default_versioning_behavior=VersioningBehavior.PINNED,
    )


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    target = os.environ.get("TEMPORAL_HOST", "127.0.0.1:7233")
    client = await Client.connect(target)

    activities = OmsActivities()

    all_activity_methods = [
        activities.assess_order_risk,
        activities.validate_payment_rrn,
        activities.enrich_with_pim,
        activities.update_customer_dashboard,
        activities.publish_to_fulfillment_kafka,
    ]

    # OMS_QUEUE: workflow execution + general activities
    # Concurrency defaults are tuned down from SDK defaults (100 activities)
    # to protect downstream connection pools (PIM, Kafka, dashboard DB).
    # Adjust based on observed p99 activity latency and pool capacity.
    oms_worker = Worker(
        client,
        task_queue=OMS_QUEUE,
        workflows=[OrderProcessingWorkflow],
        activities=all_activity_methods,
        max_concurrent_activities=50,
        max_concurrent_workflow_tasks=200,
        deployment_config=_deployment_config("oms"),
    )

    # COMMERCE_QUEUE: validate_order_api only, hard-capped at 150 RPS.
    #
    # Two separate rate-limit parameters work together:
    #   max_task_queue_activities_per_second=150.0  — server-side cap enforced by
    #     Temporal on the queue itself, across ALL worker replicas. This is the
    #     true global guard: 2 replicas → still 150 RPS total, not 300.
    #   max_activities_per_second=150.0  — per-worker local guard kept as
    #     defence-in-depth in case the server-side cap is ever misconfigured.
    #   max_concurrent_activities=15  — at p99 ~100 ms per call, 15 slots yields
    #     ~150 completions/s in steady state. Raise only if p99 exceeds 200 ms.
    commerce_worker = Worker(
        client,
        task_queue=COMMERCE_QUEUE,
        activities=[activities.validate_order_api],
        max_task_queue_activities_per_second=150.0,
        max_activities_per_second=150.0,
        max_concurrent_activities=15,
        deployment_config=_deployment_config("oms-commerce"),
    )

    print("OMS Worker: polling OMS_QUEUE + COMMERCE_QUEUE (150 RPS cap)...")
    await asyncio.gather(oms_worker.run(), commerce_worker.run())


if __name__ == "__main__":
    asyncio.run(main())
