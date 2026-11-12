import os
import sys
import json
import pymongo
import requests
import beelib
from datetime import datetime, timedelta

# Runs in-cluster on a daily CronJob (kubeconfigs/gather-healthcheck-job.yaml). The
# real gather CronJobs (datadis-gather-starter/consumer, kubeconfigs/services.yaml) only
# fire every ~2 days and the consumer takes ~2h to finish, so LOOKBACK_HOURS is set wide
# enough to always cover the most recently completed cycle even when this check runs the
# day after a "quiet" (no-run-scheduled) day. Detection of a fully-failed cycle can lag
# by up to a day - that's an acceptable trade-off for a lightweight freshness check.
LOOKBACK_HOURS = float(os.getenv("LOOKBACK_HOURS", "50"))
HEALTHY_THRESHOLD_PCT = float(os.getenv("HEALTHY_THRESHOLD_PCT", "0.975"))
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
MAX_STALE_ATTEMPTS = 6

config = beelib.beeconfig.read_config("config.json")


def notify_discord(message, csv_content=None, csv_filename="failed_cups.csv"):
    if not DISCORD_WEBHOOK_URL:
        print("DISCORD_WEBHOOK_URL not set, skipping Discord notification", file=sys.stderr)
        return
    if csv_content:
        resp = requests.post(
            DISCORD_WEBHOOK_URL,
            data={"payload_json": json.dumps({"content": message})},
            files={"file": (csv_filename, csv_content, "text/csv")},
            timeout=10,
        )
    else:
        resp = requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=10)
    resp.raise_for_status()


def find_stale_cups(collection, cutoff):
    """CUPS with no new data saved in the lookback window (missing or old last_data_saved_at)."""
    query = {"last_data_saved_at": {"$not": {"$gte": cutoff}}}
    return {doc["_id"] for doc in collection.find(query, {"_id": 1})}


def _has_exhausted_block(doc, field):
    return any(
        block.get("retries", 1) <= 0 and block.get("values", 0) < block.get("total", 0)
        for block in (doc.get(field) or {}).values()
    )


def find_exhausted_retry_cups(collection):
    """CUPS with a data_1h and/or data_15m block that will never be retried again (retries
    hit 0 while still incomplete) - a distinct failure from staleness, since a device's
    current period can keep updating fine while an older block is permanently stuck.
    Returns (hourly_only, quarterly_only, both) - a device without any blocks for one of
    the two frequencies simply can't appear as exhausted on that frequency, so it naturally
    falls into hourly_only/quarterly_only rather than both."""
    hourly = set()
    quarterly = set()
    for doc in collection.find({}, {"data_1h": 1, "data_15m": 1}):
        if _has_exhausted_block(doc, "data_1h"):
            hourly.add(doc["_id"])
        if _has_exhausted_block(doc, "data_15m"):
            quarterly.add(doc["_id"])
    both = hourly & quarterly
    return hourly - both, quarterly - both, both


def count_plateaued_blocks(collection):
    """Raw count of individual data_1h/data_15m blocks whose `values` has stopped improving across
    MAX_STALE_ATTEMPTS consecutive attempts while still incomplete - a block that is permanently
    stuck (e.g. a one-off outage gap) rather than fully exhausted on `retries` (see
    find_exhausted_retry_cups). Counts blocks directly rather than grouping by CUPS, since a single
    CUPS can have several plateaued blocks across its date ranges and the block count is what
    reflects the actual scale of the problem."""
    hourly_blocks = 0
    quarterly_blocks = 0
    for doc in collection.find({}, {"data_1h": 1, "data_15m": 1}):
        hourly_blocks += sum(
            1 for block in (doc.get("data_1h") or {}).values()
            if block.get("stale_attempts", 0) >= MAX_STALE_ATTEMPTS and block.get("values", 0) < block.get("total", 0)
        )
        quarterly_blocks += sum(
            1 for block in (doc.get("data_15m") or {}).values()
            if block.get("stale_attempts", 0) >= MAX_STALE_ATTEMPTS and block.get("values", 0) < block.get("total", 0)
        )
    return hourly_blocks, quarterly_blocks


def build_stale_cups_csv(stale_cups):
    if not stale_cups:
        return None
    lines = ["cups"]
    lines.extend(sorted(stale_cups))
    return "\n".join(lines)


def check_gather_health():
    mongo = pymongo.MongoClient(
        f"mongodb://{config['mongo']['user']}:{config['mongo']['password']}@"
        f"{config['mongo']['host']}:{config['mongo']['port']}/{config['mongo']['database']}?authSource=admin"
    )
    db = mongo[config['mongo']['database']]
    collection = db[config['mongo']['collection']]

    cutoff = datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)
    total = collection.count_documents({})

    hourly_only, quarterly_only, both = find_exhausted_retry_cups(collection)
    plateaued_hourly_blocks, plateaued_quarterly_blocks = count_plateaued_blocks(collection)

    # last_data_saved_at only gets set when a chunk actually returned new values (DatadisGatherer.py's
    # download_device()) - unlike a plain "was this device touched" timestamp, this can't be faked by
    # a run that executed but saved nothing (e.g. Datadis's 24h per-query dedup returning empty chunks).
    # CUPS exhausted on BOTH frequencies can structurally never get new data again, so they're excluded
    # from the freshness population entirely rather than counted as "no new data".
    stale_cups = find_stale_cups(collection, cutoff) - both
    population = total - len(both)
    fresh = population - len(stale_cups)
    pct_fresh = (fresh / population) if population else 0.0

    print(f"Total tracked CUPS: {total}")
    print(f"New data saved in the last {LOOKBACK_HOURS}h: {fresh}/{population} ({pct_fresh:.1%}) "
          f"[excludes {len(both)} CUPS exhausted on both frequencies]")
    print(f"No new data: {len(stale_cups)}")
    print(f"Hourly-only retries exhausted: {len(hourly_only)}")
    print(f"Quarterly-only retries exhausted: {len(quarterly_only)}")
    print(f"Both (hourly & quarterly) retries exhausted: {len(both)}")
    print(f"Hourly plateaued blocks: {plateaued_hourly_blocks}")
    print(f"Quarterly plateaued blocks: {plateaued_quarterly_blocks}")

    ok = pct_fresh >= HEALTHY_THRESHOLD_PCT
    icon = "✅" if ok else "\U0001F6A8"
    status = "healthy" if ok else "FAILED"
    message = (
        f"{icon} **Datadis gather healthcheck: {status}**\n"
        f"New data saved in the last {LOOKBACK_HOURS:.0f}h: {fresh}/{population} ({pct_fresh:.1%}, "
        f"threshold {HEALTHY_THRESHOLD_PCT:.0%})\n"
        f"No new data: {len(stale_cups)}\n"
        f"Hourly-only retries exhausted: {len(hourly_only)}\n"
        f"Quarterly-only retries exhausted: {len(quarterly_only)}\n"
        f"Both (hourly & quarterly) retries exhausted: {len(both)}\n"
        f"Hourly plateaued blocks: {plateaued_hourly_blocks}\n"
        f"Quarterly plateaued blocks: {plateaued_quarterly_blocks}"
    )
    csv_content = build_stale_cups_csv(stale_cups)
    notify_discord(message, csv_content=csv_content)
    return ok


if __name__ == "__main__":
    # Always exit 0: the healthy/unhealthy state is already surfaced via the Discord message
    # (icon + "healthy"/"FAILED" text), so there's nothing a k8s-level Job "Failed" status adds
    # beyond noise - it doesn't represent a crash, just a reported metric that's below threshold.
    check_gather_health()