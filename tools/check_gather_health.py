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
HEALTHY_THRESHOLD_PCT = float(os.getenv("HEALTHY_THRESHOLD_PCT", "0.95"))
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

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
        f"Both (hourly & quarterly) retries exhausted: {len(both)}"
    )
    csv_content = build_stale_cups_csv(stale_cups)
    notify_discord(message, csv_content=csv_content)
    return ok


if __name__ == "__main__":
    ok = check_gather_health()
    sys.exit(0 if ok else 1)