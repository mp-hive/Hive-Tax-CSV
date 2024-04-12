import csv
from beem import Hive
from beem.account import Account
from beem.amount import Amount
from datetime import datetime

# Configuration
account_name = "your-hive-account-name"  # Replace with your Hive account name
start_date = datetime.strptime("2023-01-01", "%Y-%m-%d")
end_date = datetime.strptime("2023-12-31", "%Y-%m-%d")
csv_file_path = "hive_transactions.csv"

# Initialize Hive instance with the specified node
nodes = ["https://api.deathwing.me"]
hive = Hive(node=nodes)

# Fetch dynamic global properties for VESTS to HP conversion
dyn_props = hive.get_dynamic_global_properties()
total_vesting_fund_hive = Amount(dyn_props["total_vesting_fund_hive"]).amount
total_vesting_shares = Amount(dyn_props["total_vesting_shares"]).amount
conversion_rate = total_vesting_fund_hive / total_vesting_shares

# Initialize Account with the specified Hive instance
account = Account(account_name, blockchain_instance=hive)

# Helper function to parse and correct amounts, converting VESTS to HP if necessary
def parse_amount(amount_str, is_vests=False):
    amount = Amount(amount_str)
    if is_vests:
        return float(amount.amount) * conversion_rate
    return amount.amount

# Open CSV file
with open(csv_file_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["Time", "Type", "In", "In-Currency", "Out", "Out-Currency", "Fee", "Fee-Currency", "Market", "Note"])

    # Fetch account history for specified operations
    for operation in account.history(only_ops=["claim_reward_balance", "interest", "fill_convert_request", "fill_order"], start=start_date, stop=end_date):
        timestamp = datetime.strptime(operation['timestamp'], "%Y-%m-%dT%H:%M:%S")
        
        if operation['type'] == 'claim_reward_balance':
            hive_amount = parse_amount(operation.get('reward_hive', '0 HIVE'))
            hbd_amount = parse_amount(operation.get('reward_hbd', '0 HBD'))
            vests_amount = parse_amount(operation.get('reward_vests', '0 VESTS'), is_vests=True)
            # Handling for reward balances (HIVE, HBD, HP)
            if hive_amount > 0:
                writer.writerow([timestamp.strftime("%Y-%m-%d %H:%M:%S"), "Income", f"{hive_amount}", "HIVE", "", "", "", "", "", ""])
            if hbd_amount > 0:
                writer.writerow([timestamp.strftime("%Y-%m-%d %H:%M:%S"), "Income", f"{hbd_amount}", "HBD", "", "", "", "", "", ""])
            if vests_amount > 0:
                writer.writerow([timestamp.strftime("%Y-%m-%d %H:%M:%S"), "Income", f"{vests_amount:.3f}", "HP", "", "", "", "", "", ""])

        elif operation['type'] == 'interest':
            interest_amount = parse_amount(operation.get('interest', '0 HBD'))
            writer.writerow([timestamp.strftime("%Y-%m-%d %H:%M:%S"), "Interest", f"{interest_amount}", "HBD", "", "", "", "", "", ""])

        elif operation['type'] == 'fill_convert_request':
            amount_in = Amount(operation['amount_in'])
            amount_out = Amount(operation['amount_out'])
            writer.writerow([timestamp.strftime("%Y-%m-%d %H:%M:%S"), "Transaction", f"{amount_in.amount}", amount_in.symbol, f"{amount_out.amount}", amount_out.symbol, "", "", "Hive Internal Market", "Conversion"])

        elif operation['type'] == 'fill_order':
            current_pays = Amount(operation['current_pays'])
            open_pays = Amount(operation['open_pays'])
            writer.writerow([timestamp.strftime("%Y-%m-%d %H:%M:%S"), "Trade", f"{current_pays.amount}", current_pays.symbol, f"{open_pays.amount}", open_pays.symbol, "", "", "Hive Internal Market", "Trade"])

print("CSV file created.")
