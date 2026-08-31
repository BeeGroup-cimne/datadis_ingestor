import pymongo
import beelib
from pprint import pprint

# 1. Load your credentials (replace these with your actual config file loading if needed)
config = beelib.beeconfig.read_config("config.json")
mongo = config['mongo']
mongo['host'] = "127.0.0.1"


def check_cups_state(cups_id):
    # 2. Establish the connection pool
    mongo = pymongo.MongoClient(
        f"mongodb://{config['mongo']['user']}:{config['mongo']['password']}@"
        f"{config['mongo']['host']}:{config['mongo']['port']}/{config['mongo']['database']}?authSource=admin"
    )

    # Target the verified database and collection
    db = mongo[config['mongo']['database']]
    collection = db[config['mongo']['collection']]

    # 3. Fetch the document
    device = collection.find_one({"_id": cups_id})

    # 4. Analyze and print the tracking state
    if not device:
        print(f"❌ CUPS {cups_id} does not exist in the database.")
        return

    print(f"✅ Found device: {cups_id}")

    # Check if contract data is present (indicates the Kafka consumer has touched it)
    if 'startDate' in device or 'marketer' in device:
        print("⚠️ Contract data is present in this document.")

    print("\n" + "=" * 40)
    print(" TRACKING STATE ANALYSIS ")
    print("=" * 40)

    # Loop through the known tracking collections
    tracking_keys = ["data_1h", "data_15m", "max_power"]
    found_tracking = False

    for tk in tracking_keys:
        if tk in device:
            found_tracking = True
            print(f"\n--- {tk.upper()} ---")
            blocks = device[tk]

            # Print each date block cleanly
            for date_range, status in blocks.items():
                v = status.get('values', 0)
                t = status.get('total', 0)
                r = status.get('retries', 0)

                # Highlight incomplete chunks
                if v == 0:
                    status_marker = "🔴 EMPTY"
                elif v < t:
                    status_marker = "🟡 PARTIAL"
                else:
                    status_marker = "🟢 COMPLETE"

                print(f"{status_marker} | {date_range} | Values: {v}/{t} | Retries left: {r}")
        else:
            print(f"\n❌ {tk.upper()} is completely MISSING from this document.")

    if not found_tracking:
        print(
            "\n🚨 CONCLUSION: All tracking state is missing. If this CUPS was downloaded previously, the state has been overwritten.")
    else:
        print("\n✅ CONCLUSION: Tracking state exists. The script should not redownload COMPLETE blocks.")


if __name__ == "__main__":
    # Ensure you have an active port-forward to MongoDB before running this:
    # kubectl port-forward pod/mongodb-0 27017:27017 -n sime-prod-databases

    target_cups = "ES0031405170512001NE0F"
    check_cups_state(target_cups)
