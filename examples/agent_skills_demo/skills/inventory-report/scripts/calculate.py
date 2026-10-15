"""Optional application-run calculator: python calculate.py < inventory.json."""

import json
import sys


def calculate(inventory):
    result = []
    for item in inventory["items"]:
        row = {"sku": item["sku"], "stock": item["stock"]}
        sales, lead = item.get("daily_sales"), item.get("replenishment_days")
        row["missing"] = [key for key in ("daily_sales", "replenishment_days") if item.get(key) is None]
        if not row["missing"]:
            row["sellable_days"] = item["stock"] / sales if sales > 0 else None
            row["minimum_replenishment"] = max(0, sales * lead - item["stock"])
        result.append(row)
    return result


if __name__ == "__main__":
    print(json.dumps(calculate(json.load(sys.stdin)), ensure_ascii=False))
