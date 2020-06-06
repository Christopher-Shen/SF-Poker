import argparse
import gspread  # type: ignore
import json
import pandas as pd  # type: ignore

from copy import deepcopy
from heapq import heapify, heappush, heappop
from oauth2client.service_account import ServiceAccountCredentials  # type: ignore
from typing import List, Tuple, Dict, Any, Union, Set


def compute_transactions(ledger):
    assert round(sum(ledger.values()), 2) == 0, f"{round(sum(ledger.values()), 2)}"
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


def get_spreadsheet_data() -> Tuple[Dict[str, float], Dict[str, str], Set[str]]:
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

    all_results: Dict[str, Dict[str, float]] = {}
    venmo_info: Dict[str, str] = {}

    for row in cleaned_results_worksheet:
        player_results: Dict[str, float] = {}
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

    return summed_results, venmo_info, game_ids_to_settle_set


def settle_proxies(
    proxies, data
) -> Tuple[Dict[str, float], List[Tuple[str, str, float]]]:
    proxied_data = deepcopy(data)
    proxy_transactions: List[Tuple[str, str, float]] = []

    proxy_groups: List[str] = proxies.split(";")
    for proxy_pair in proxy_groups:
        proxy_names = proxy_pair.split(",")
        getting_proxied = proxy_names[0]
        proxy = proxy_names[1]

        assert data[getting_proxied]
        assert data[proxy]

        proxy_amount = data[getting_proxied]

        proxied_data[proxy] = data[proxy] + proxy_amount
        del proxied_data[getting_proxied]
        proxy_transactions.append((proxy, getting_proxied, -proxy_amount))

    return proxied_data, proxy_transactions


def print_ledger(
    data, venmo_info, transactions, game_ids_settled, proxy_transactions=None
) -> str:
    out_string = "Bills\n=============\n"

    for name in data:
        if data[name] < 0:
            out_string += f"{name}: -${-data[name]}\n"
        else:
            out_string += f"{name}: ${data[name]}\n"

    out_string += "\nTransactions To Settle\n======================\n"
    for debtee, debtor, amount in transactions:
        out_string += f"{debtee} ({venmo_info.get(debtee)}) requests ${amount} from {debtor} ({(venmo_info.get(debtor))})\n"

    if proxy_transactions:
        out_string += "\nProxies\n================\n"
        for proxy, getting_proxied, amount in proxy_transactions:
            if amount < 0:
                out_string += f"{getting_proxied} ({venmo_info.get(getting_proxied)}) requests ${-amount} from {proxy} ({(venmo_info.get(proxy))})\n"
            else:
                out_string += f"{proxy} ({venmo_info.get(proxy)}) requests ${amount} from {getting_proxied} ({(venmo_info.get(getting_proxied))})\n"

    out_string += f"\nGames Settled\n=================\n{', '.join(game_ids_settled)}"

    return out_string


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxies", dest="proxies", type=str)
    args = parser.parse_args()

    data, venmo_info, game_ids_settled = get_spreadsheet_data()
    original_data = deepcopy(data)

    proxy_transactions = []
    if args.proxies:
        data, proxy_transactions = settle_proxies(args.proxies, data)

    transactions = compute_transactions(data)

    out_string = print_ledger(
        original_data, venmo_info, transactions, game_ids_settled, proxy_transactions
    )
    print(out_string)


if __name__ == "__main__":
    main()
