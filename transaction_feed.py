import requests
import json
import os
import base64
import binascii
import datetime
import psycopg2

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
XRPL_RPC_URL = "https://s1.ripple.com:51234/"

# Your token (IOU) details
CURRENCY_CODE = "PFT"    # e.g. "USD", "FOO", etc.
ISSUER_ADDRESS = "rnQUEEg8yyjrwk9FhyXpKavHyCRJM9BDMW"  # Replace with the actual issuer

# Which account to check transactions for:
ACCOUNT_TO_CHECK = ISSUER_ADDRESS

# Database connection details (update for your Neon setup):

DB_CONN_STRING = "{{DB_CONNECT}}"

# ------------------------------------------------------------------------------
# Helpers to fetch and filter transactions
# ------------------------------------------------------------------------------
def fetch_account_transactions(account, ledger_index_min, limit=1000, marker=None):
    """
    Fetch transactions for a given account using the 'account_tx' method
    from the XRPL JSON RPC. Returns (transactions, marker).
    """
    request_body = {
        "method": "account_tx",
        "params": [
            {
                "account": account,
                "ledger_index_min": ledger_index_min,
                "ledger_index_max": -1,  # no upper limit
                "limit": limit
            }
        ]
    }
    if marker:
        request_body["params"][0]["marker"] = marker

    response = requests.post(XRPL_RPC_URL, json=request_body, timeout=20)
    response_json = response.json()

    if "result" not in response_json:
        raise Exception(f"Unexpected response: {response_json}")

    result = response_json["result"]
    txs = result.get("transactions", [])
    next_marker = result.get("marker", None)
    return txs, next_marker

def is_token_payment(tx, currency_code, issuer):
    """
    Check if a Payment transaction involves the specified token (IOU).
    """
    if tx.get("TransactionType") != "Payment":
        return False
    amount = tx.get("Amount")
    # For an IOU Payment, 'Amount' is a dictionary with 'currency', 'issuer', 'value'.
    if isinstance(amount, dict):
        if (amount.get("issuer") == issuer and
            amount.get("currency") == currency_code):
            return True
    return False

# ------------------------------------------------------------------------------
# Memo decoding
# ------------------------------------------------------------------------------
def decode_hex_or_base64(encoded_str):
    """
    Attempts to decode a string from hex or base64.
    Returns decoded ASCII (if possible) or the raw string if decoding fails.
    """
    if not encoded_str:
        return ""

    # Try hex decode first
    try:
        decoded_bytes = bytes.fromhex(encoded_str)
        return decoded_bytes.decode('utf-8', errors='replace')
    except ValueError:
        pass

    # If that fails, try base64 decode
    try:
        decoded_bytes = base64.b64decode(encoded_str)
        return decoded_bytes.decode('utf-8', errors='replace')
    except (binascii.Error, ValueError):
        # If all decoding fails, return the original encoded string
        return encoded_str

def extract_memos(transaction):
    """
    Extract and decode all memos from a transaction, concatenating into a single string.
    """
    memos = transaction.get("Memos", [])
    if not memos:
        return ""

    decoded_memos = []
    for memo_entry in memos:
        memo = memo_entry.get("Memo", {})
        memo_data   = decode_hex_or_base64(memo.get("MemoData", ""))   # main content
        decoded_memos.append(memo_data)

    # Join multiple memos with a separator
    return "\n".join(decoded_memos)

# ------------------------------------------------------------------------------
# Date/Time handling
# ------------------------------------------------------------------------------
def ripple_to_unix_time(ripple_time):
    """
    Convert Ripple epoch time (seconds since 1/1/2000) to Unix epoch time (seconds since 1/1/1970).
    Ripple epoch starts at 2000-01-01 00:00:00 UTC,
    which is 946684800 seconds after Unix epoch start (1970-01-01).
    """
    return ripple_time + 946684800

def get_ripple_datetime(ripple_time):
    """
    Returns a Python datetime (UTC) from the ripple ledger close time.
    """
    unix_ts = ripple_to_unix_time(ripple_time)
    return datetime.datetime.utcfromtimestamp(unix_ts)

# ------------------------------------------------------------------------------
# Database utility
# ------------------------------------------------------------------------------
def get_last_stored_ledger_index(conn):
    """
    Return the maximum ledger_index currently in pft_transactions.
    If table is empty (None result), return -1 to indicate "start from the beginning."
    """
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(ledger_index) FROM pft_transactions;")
        result = cur.fetchone()
        if result and result[0] is not None:
            return result[0]
        return -1

def insert_transaction(
    conn, ledger_index, tx_hash, from_address, to_address, memo,
    amount_int, tx_datetime
):
    """
    Insert into pft_transactions using an ON CONFLICT DO NOTHING approach
    to avoid duplicates by transaction_hash.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pft_transactions (
                ledger_index,
                transaction_hash,
                from_address,
                to_address,
                memo,
                amount,
                transaction_timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_hash) DO NOTHING;
            """,
            (
                ledger_index,
                tx_hash,
                from_address,
                to_address,
                memo,
                amount_int,
                tx_datetime,
            )
        )
    # Not committing yet – let's commit once per batch for efficiency
    # or at the end of the script. For safer immediate writes, uncomment the next line:
    # conn.commit()

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
def main():
    # Connect to Neon
    conn = psycopg2.connect(DB_CONN_STRING)
    conn.autocommit = False  # or True if you want immediate commits

    # 1) Determine where to resume
    last_ledger_index = get_last_stored_ledger_index(conn)
    ledger_index_min = last_ledger_index + 1
    if last_ledger_index == -1:
        print("Table is empty. Fetching all ledgers (this might be large).")
    else:
        print(f"Resuming from ledger_index > {last_ledger_index}")

    # 2) Fetch & filter transactions
    print(f"Fetching transactions for account: {ACCOUNT_TO_CHECK}")
    print(f"Filtering for Payment transactions of token '{CURRENCY_CODE}' from issuer {ISSUER_ADDRESS}\n")

    marker = None
    highest_ledger_this_run = ledger_index_min

    while True:
        transactions_batch, marker = fetch_account_transactions(
            account=ACCOUNT_TO_CHECK,
            ledger_index_min=ledger_index_min,
            marker=marker,
        )

        for entry in transactions_batch:
            tx = entry.get("tx", {})
            meta = entry.get("meta", {})
            ledger_index_tx = tx.get("ledger_index", 0)

            # Track highest ledger index we see, so we can save it later
            if ledger_index_tx > highest_ledger_this_run:
                highest_ledger_this_run = ledger_index_tx

            # Check if it’s a Payment transaction in our IOU
            if is_token_payment(tx, CURRENCY_CODE, ISSUER_ADDRESS):
                from_addr = tx.get("Account", "")
                to_addr = tx.get("Destination", "")
                tx_hash = tx.get("hash", "")  # unique transaction hash

                amount = tx.get("Amount", {})
                if isinstance(amount, dict):
                    # This is an IOU. 'value' is a decimal string. Example: "123.456"
                    value_str = amount.get("value", "0")
                else:
                    # If it were XRP in drops, but that shouldn’t happen for your IOU
                    value_str = amount

                # Convert value to an integer (BIGINT).
                # If your token can have decimals, you may need to scale or switch to numeric.
                try:
                    amount_int = int(float(value_str))
                except ValueError:
                    amount_int = 0

                ripple_time = tx.get("date")
                if ripple_time is not None:
                    tx_datetime = get_ripple_datetime(ripple_time)
                else:
                    tx_datetime = None  # fallback if somehow missing

                memo_str = extract_memos(tx)

                # Insert into DB with ON CONFLICT DO NOTHING
                insert_transaction(
                    conn=conn,
                    ledger_index=ledger_index_tx,
                    tx_hash=tx_hash,
                    from_address=from_addr,
                    to_address=to_addr,
                    memo=memo_str,
                    amount_int=amount_int,
                    tx_datetime=tx_datetime,
                )

        # Commit after each batch (optional approach)
        conn.commit()

        # Pagination check
        if not marker:
            break

    # 3) Done. We don’t necessarily need to write the last ledger anywhere,
    # because the next run will figure it out from the DB.
    conn.close()

    print("Done.")
    print(f"Last processed ledger index this run: {highest_ledger_this_run}")

if __name__ == "__main__":
    main()
