### Setup

```
git clone https://github.com/mp-hive/Hive-Tax-CSV.git
cd Hive-Tax-CSV
npm install
```

---

### How to use
#### Fetch Hive Layer-1 transactions
Edit `hive_tax_exporter.js`. Near the top of the file, insert your account name and the tax year you wish to fetch transactions from:

```
const HIVE_ACCOUNT = 'your-hive-account';
const YEAR = '2024';
```

Then run hive_tax_exporter.js. It will scan the blockchain and produce a csv file containing all relevant taxable transactions

```
node hive_tax_exporter.js
```

---

#### Fetch Hive-Engine Layer-2 transactions
Edit `he_tax_exporter.js`. Near the top of the file, insert your account name and the tax year you wish to fetch transactions from:

```
const HIVE_ACCOUNT = 'your-hive-account';
const YEAR = '2024';
const TOKEN_SYMBOL = 'LEO';
```

Then run hive_tax_exporter.js. It will scan the blockchain and produce two csv's:
- one containing all your trades for the selected token
- one containing all your reward and transfer-transactions for the selected token symbol

```
node he_tax_exporter.js
```

---

#### Alternate Option: Fetch HE raw transactions
Edit `he_fetch_raw_transactions.js`. Near the top of the file, insert your account name and the tax year you wish to fetch transactions from:

```
const HIVE_ACCOUNT = 'your-hive-account';
const YEAR = '2024';
const TOKEN_SYMBOL = 'LEO';
```
The script will produce two files, one with a standard csv containing all tx data, + one file with the raw json output.

```
node he_fetch_raw_transactions.js
```