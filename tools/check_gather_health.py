import os
import sys
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


def notify_discord(message):
    if not DISCORD_WEBHOOK_URL:
        print("DISCORD_WEBHOOK_URL not set, skipping Discord notification", file=sys.stderr)
        return
    resp = requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=10)
    resp.raise_for_status()


def check_gather_health():
    mongo = pymongo.MongoClient(
        f"mongodb://{config['mongo']['user']}:{config['mongo']['password']}@"
        f"{config['mongo']['host']}:{config['mongo']['port']}/{config['mongo']['database']}?authSource=admin"
    )
    db = mongo[config['mongo']['database']]
    collection = db[config['mongo']['collection']]

    cutoff = datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)
    total = collection.count_documents({})
    # last_data_saved_at only gets set when a chunk actually returned new values (DatadisGatherer.py's
    # download_device()) - unlike a plain "was this device touched" timestamp, this can't be faked by
    # a run that executed but saved nothing (e.g. Datadis's 24h per-query dedup returning empty chunks).
    fresh = collection.count_documents({"last_data_saved_at": {"$gte": cutoff}})
    stale = total - fresh

    pct_fresh = (fresh / total) if total else 0.0

    print(f"Total tracked CUPS: {total}")
    print(f"New data saved in the last {LOOKBACK_HOURS}h: {fresh} ({pct_fresh:.1%})")
    print(f"No new data: {stale}")

    ok = pct_fresh >= HEALTHY_THRESHOLD_PCT
    icon = "✅" if ok else "\U0001F6A8"
    status = "healthy" if ok else "FAILED"
    notify_discord(
        f"{icon} **Datadis gather healthcheck: {status}**\n"
        f"New data saved in the last {LOOKBACK_HOURS:.0f}h: {fresh}/{total} ({pct_fresh:.1%}, "
        f"threshold {HEALTHY_THRESHOLD_PCT:.0%})\n"
        f"No new data: {stale}"
    )
    return ok


if __name__ == "__main__":
    ok = check_gather_health()
    sys.exit(0 if ok else 1)