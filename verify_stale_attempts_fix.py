from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import DatadisGatherer as dg


def make_sequenced_query(sequence):
    it = iter(sequence)

    def _query(endpoint, **kwargs):
        result = next(it)
        if isinstance(result, Exception):
            raise result
        return result

    return _query


def make_consumption_rows(n, start=datetime(2024, 1, 1)):
    return [{"datetime": pd.Timestamp(start + timedelta(hours=i)), "cups": "ES1", "value": 1.0}
            for i in range(n)]


def make_status(total, values, retries, stale_attempts=None):
    status = {
        "date_ini_block": datetime.today() - relativedelta(days=10),
        "date_end_block": datetime.today(),
        "values": values,
        "total": total,
        "retries": retries,
    }
    if stale_attempts is not None:
        status["stale_attempts"] = stale_attempts
    return status


def make_device(cups, data_1h_status):
    return {"_id": cups, "data_1h": {"block": data_1h_status}, "data_15m": {}, "max_power": {}}


def make_supply(cups):
    return {"cups": cups, "nif": "nif", "distributorCode": "2", "pointType": 5, "authorized_nif": "nif"}


def case_improving_block_never_excluded():
    status = make_status(total=100, values=0, retries=6, stale_attempts=0)
    device = make_device("CUPS_IMPROVE", status)
    datadis_devices = MagicMock()
    supply = make_supply("CUPS_IMPROVE")
    seq = [make_consumption_rows(3), make_consumption_rows(6), make_consumption_rows(9)]
    with patch.object(dg.datadis.Datadis, "datadis_query", side_effect=make_sequenced_query(seq)), \
         patch.object(dg, "save_datadis_data", return_value=None):
        for _ in range(3):
            dg.download_device(supply, device, datadis_devices, [], {}, {}, {})
            assert status["stale_attempts"] == 0, f"expected 0 while improving, got {status['stale_attempts']}"


def case_plateaued_block_gets_excluded_after_threshold():
    status = make_status(total=100, values=5, retries=6, stale_attempts=0)
    device = make_device("CUPS_PLATEAU", status)
    datadis_devices = MagicMock()
    supply = make_supply("CUPS_PLATEAU")
    seq = [make_consumption_rows(5) for _ in range(dg.MAX_STALE_ATTEMPTS)]
    with patch.object(dg.datadis.Datadis, "datadis_query", side_effect=make_sequenced_query(seq)) as mocked_query, \
         patch.object(dg, "save_datadis_data", return_value=None):
        for _ in range(dg.MAX_STALE_ATTEMPTS):
            dg.download_device(supply, device, datadis_devices, [], {}, {}, {})
        assert status["stale_attempts"] == dg.MAX_STALE_ATTEMPTS, \
            f"expected counter at threshold {dg.MAX_STALE_ATTEMPTS}, got {status['stale_attempts']}"
        assert mocked_query.call_count == dg.MAX_STALE_ATTEMPTS, \
            f"expected {dg.MAX_STALE_ATTEMPTS} calls so far, got {mocked_query.call_count}"

        # Block is now plateaued: a further run must not attempt it again.
        dg.download_device(supply, device, datadis_devices, [], {}, {}, {})
        assert mocked_query.call_count == dg.MAX_STALE_ATTEMPTS, \
            f"expected no new calls once plateaued, got {mocked_query.call_count}"


def case_empty_to_partial_first_attempt_is_not_punished():
    status = make_status(total=100, values=0, retries=6, stale_attempts=0)
    device = make_device("CUPS_EMPTY_THEN_PARTIAL", status)
    datadis_devices = MagicMock()
    supply = make_supply("CUPS_EMPTY_THEN_PARTIAL")
    seq = [[], make_consumption_rows(7)]
    with patch.object(dg.datadis.Datadis, "datadis_query", side_effect=make_sequenced_query(seq)), \
         patch.object(dg, "save_datadis_data", return_value=None):
        dg.download_device(supply, device, datadis_devices, [], {}, {}, {})
        assert status["retries"] == 5, f"expected retries decremented to 5 on empty result, got {status['retries']}"
        assert status["stale_attempts"] == 0, f"expected stale_attempts untouched by empty result, got {status['stale_attempts']}"

        dg.download_device(supply, device, datadis_devices, [], {}, {}, {})
        assert status["retries"] == 5, f"expected retries unchanged on partial success, got {status['retries']}"
        assert status["stale_attempts"] == 0, \
            f"expected first real progress off an empty baseline not to be punished, got {status['stale_attempts']}"


def case_legacy_block_without_field_gets_sane_default():
    status = make_status(total=100, values=5, retries=6)  # no stale_attempts key at all
    assert "stale_attempts" not in status
    device = make_device("CUPS_LEGACY", status)
    datadis_devices = MagicMock()
    supply = make_supply("CUPS_LEGACY")
    seq = [make_consumption_rows(5)]  # same count as current values: no improvement
    with patch.object(dg.datadis.Datadis, "datadis_query", side_effect=make_sequenced_query(seq)) as mocked_query, \
         patch.object(dg, "save_datadis_data", return_value=None):
        dg.download_device(supply, device, datadis_devices, [], {}, {}, {})
    assert mocked_query.call_count == 1, "expected legacy block (missing field) to still be attempted"
    assert status.get("stale_attempts") == 1, f"expected field to materialize at 1, got {status.get('stale_attempts')}"


if __name__ == "__main__":
    case_improving_block_never_excluded()
    case_plateaued_block_gets_excluded_after_threshold()
    case_empty_to_partial_first_attempt_is_not_punished()
    case_legacy_block_without_field_gets_sane_default()
    print("All verification cases passed.")
