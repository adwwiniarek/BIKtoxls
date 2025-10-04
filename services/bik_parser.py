from __future__ import annotations
from typing import List, Dict, Any

# Placeholder parsera – w docelowej wersji wypełniasz danymi z BIK pdt/xls
def parse_payload_to_debts(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    debts = payload.get("debts", [])
    # tu można dodać walidację / normalizację
    return debts
