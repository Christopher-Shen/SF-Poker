import argparse
import gspread  # type: ignore
import json
import pandas as pd  # type: ignore

from copy import deepcopy
from heapq import heapify, heappush, heappop
from oauth2client.service_account import ServiceAccountCredentials  # type: ignore
from typing import List, Tuple, Dict, Any, Union, Set


def compute_transactions(ledger):
    assert round(sum(ledger.values()), 2) == 0
    neg = []
    pos = []
    for name, value in ledger.items():
        if value < 0:
            heappush(neg, (value, value, name))
        else:
            heappush(pos, (-value, value, name))
    transactions = []
    while neg and pos:
        _, debt, debtee = heappop(pos)
        __, payment, debtor = heappop(neg)
        unaccounted = round(debt + payment, 2)
        if unaccounted > 0:
            heappush(pos, (-unaccounted, unaccounted, debtee))
        elif unaccounted < 0:
            heappush(neg, (unaccounted, unaccounted, debtor))
        amount = min(debt, -payment)
        transactions.append((debtee, debtor, amount))
    assert len(neg) == 0
    assert len(pos) == 0
    transactions = sorted(transactions)

    return transactions


def get_spreadsheet_data() -> Tuple[Dict[str, float], Dict[str, str]]:
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "SF_Poker_Secrets.json", scope
    )
    client = gspread.authorize(creds)

    google_sheet_client = client.open("SF Poker")

    results_worksheet = google_sheet_client.worksheet("result").get_all_records()

    cleaned_results_worksheet = [
        row for row in results_worksheet if row.get("venmo_handle").startswith("@")
    ]

    list_of_results_lists = google_sheet_client.worksheet("result").get_all_values()

    game_ids_to_settle: List[str] = [
        list_of_results_lists[0][i + 2]
        for i, is_done in enumerate(list_of_results_lists[4][2:])
        if not is_done
    ]

    length_ids = len(game_ids_to_settle)
    game_ids_to_settle_set = set(game_ids_to_settle)

    assert len(game_ids_to_settle_set) == length_ids

    all_results = {}
    venmo_info: Dict[str, str] = {}

    for row in cleaned_results_worksheet:
        player_results = {}
        for gid in game_ids_to_settle:
            if gid in row:
                player_pnl = row[gid]
                if player_pnl:
                    player_results[gid] = player_pnl
        player_name = row["name"]
        player_venmo = row["venmo_handle"]

        assert player_name not in player_results
        if player_results:
            all_results[player_name] = player_results
            venmo_info[player_name] = player_venmo

    summed_results: Dict[str, float] = {
        name: round(sum(all_results[name].values()), 2)
        for (name, results_dict) in all_results.items()
    }

    return summed_results, venmo_info


def settle_proxies(proxies, data) -> None:
    proxy_names = proxies.split(",")

    assert data[proxy_names[0]]
    assert data[proxy_names[1]]

    proxied_data = deepcopy(data)
    proxied_data[proxy_names[1]] = data[proxy_names[1]] + data[proxy_names[0]]
    del proxied_data[proxy_names[0]]

    return proxied_data


def print_ledger(data, venmo_info, transactions) -> str:
    out_string = "BILLS\n=============\n"

    for name in data:
        if data[name] < 0:
            out_string += f"{name}: -${-data[name]}\n"
        else:
            out_string += f"{name}: ${data[name]}\n"

    out_string += "\nTransactions To Settle\n======================\n"
    for debtee, debtor, amount in transactions:
        out_string += f"{debtee} ({venmo_info.get(debtee)}) requests ${amount} from {debtor} ({(venmo_info.get(debtor))})\n"

    return out_string


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxies", dest="proxies", type=str)
    args = parser.parse_args()

    data, venmo_info = get_spreadsheet_data()
    original_data = deepcopy(data)

    if args.proxies:
        data = settle_proxies(args.proxies, data)

    transactions = compute_transactions(data)

    out_string = print_ledger(original_data, venmo_info, transactions)
    print(out_string)


if __name__ == "__main__":
    main()
