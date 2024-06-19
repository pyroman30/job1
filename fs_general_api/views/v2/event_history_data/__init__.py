from typing import Iterable, Optional


def data_point(name: str, value: str, items: Optional[Iterable] = None) -> dict:
    if not items:
        items = []

    return {"header": name, "value": value, "items": items}
