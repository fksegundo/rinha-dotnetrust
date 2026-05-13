#!/usr/bin/env python3
import argparse
import copy
import json
from collections import OrderedDict
from pathlib import Path


def reorder_request(request, variant):
    transaction = request["transaction"]
    customer = request["customer"]
    merchant = request["merchant"]
    terminal = request["terminal"]

    if variant % 2 == 0:
        reordered = OrderedDict()
        reordered["terminal"] = OrderedDict([
            ("km_from_home", terminal["km_from_home"]),
            ("card_present", terminal["card_present"]),
            ("is_online", terminal["is_online"]),
        ])
        reordered["merchant"] = OrderedDict([
            ("avg_amount", merchant["avg_amount"]),
            ("mcc", merchant["mcc"]),
            ("id", merchant["id"]),
        ])
        reordered["customer"] = OrderedDict([
            ("known_merchants", list(reversed(customer["known_merchants"]))),
            ("tx_count_24h", customer["tx_count_24h"]),
            ("avg_amount", customer["avg_amount"]),
        ])
        reordered["transaction"] = OrderedDict([
            ("requested_at", transaction["requested_at"]),
            ("installments", transaction["installments"]),
            ("amount", transaction["amount"]),
        ])
        reordered["id"] = request["id"]
        reordered["last_transaction"] = request["last_transaction"]
        return reordered

    reordered = OrderedDict()
    reordered["id"] = request["id"]
    reordered["last_transaction"] = request["last_transaction"]
    reordered["transaction"] = OrderedDict([
        ("installments", transaction["installments"]),
        ("amount", transaction["amount"]),
        ("requested_at", transaction["requested_at"]),
    ])
    reordered["customer"] = OrderedDict([
        ("avg_amount", customer["avg_amount"]),
        ("known_merchants", rotate(customer["known_merchants"], variant)),
        ("tx_count_24h", customer["tx_count_24h"]),
    ])
    reordered["terminal"] = OrderedDict([
        ("is_online", terminal["is_online"]),
        ("km_from_home", terminal["km_from_home"]),
        ("card_present", terminal["card_present"]),
    ])
    reordered["merchant"] = OrderedDict([
        ("mcc", merchant["mcc"]),
        ("avg_amount", merchant["avg_amount"]),
        ("id", merchant["id"]),
    ])
    return reordered


def rotate(values, amount):
    if not values:
        return values
    shift = amount % len(values)
    return values[shift:] + values[:shift]


def mutate_entry(entry, copy_index, index, mode):
    result = copy.deepcopy(entry)
    request = result["request"]
    request["id"] = f"{request['id']}-x{copy_index}-{index}"

    if mode == "clone":
        return result

    if mode in {"neutral", "reorder"}:
        result["request"] = reorder_request(request, copy_index + index)
        return result

    raise ValueError(f"unknown mode: {mode}")


def scale_stats(stats, factor):
    total = stats["total"] * factor
    fraud_count = stats["fraud_count"] * factor
    legit_count = stats["legit_count"] * factor
    edge_case_count = stats["edge_case_count"] * factor
    return {
        "total": total,
        "fraud_count": fraud_count,
        "legit_count": legit_count,
        "fraud_rate": round(fraud_count / total, 4) if total else 0,
        "legit_rate": round(legit_count / total, 4) if total else 0,
        "edge_case_count": edge_case_count,
        "edge_case_rate": round(edge_case_count / total, 4) if total else 0,
    }


def main():
    parser = argparse.ArgumentParser(description="Generate larger Rinha k6 datasets from the official test-data.json.")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--factor", type=int, default=2, help="Total output multiplier. 2 doubles the official dataset.")
    parser.add_argument(
        "--mode",
        choices=["clone", "neutral", "reorder"],
        default="neutral",
        help="clone only changes ids; neutral/reorder also reserializes equivalent JSON with changed object field order.",
    )
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    if args.factor < 1:
        raise SystemExit("--factor must be >= 1")

    with args.input.open("r", encoding="utf-8") as source:
        data = json.load(source)

    original_entries = data["entries"]
    output_entries = []

    for copy_index in range(args.factor):
        for index, entry in enumerate(original_entries):
            if copy_index == 0:
                output_entries.append(copy.deepcopy(entry))
            else:
                output_entries.append(mutate_entry(entry, copy_index, index, args.mode))

    output = {
        "references_checksum_sha256": data.get("references_checksum_sha256"),
        "stats": scale_stats(data["stats"], args.factor),
        "generator": {
            "source_total": len(original_entries),
            "factor": args.factor,
            "mode": args.mode,
        },
        "entries": output_entries,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as target:
        if args.pretty:
            json.dump(output, target, ensure_ascii=False, indent=2, separators=(",", ": "))
        else:
            json.dump(output, target, ensure_ascii=False, separators=(",", ":"))

    print(
        f"generated {len(output_entries)} entries "
        f"({output['stats']['fraud_count']} fraud, {output['stats']['legit_count']} legit) "
        f"at {args.output}"
    )


if __name__ == "__main__":
    main()
