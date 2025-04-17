const fs = require('fs');
const axios = require('axios');

// Configuration
const HIVE_ACCOUNT = 'your-hive-account';
const YEAR = '2024';
const TOKEN_SYMBOL = 'LEO';

// Calculate start and end timestamps for the year
const startDate = new Date(`${YEAR}-01-01T00:00:00Z`);
const endDate = new Date(`${YEAR}-12-31T23:59:59Z`);
const startTimestamp = Math.floor(startDate.getTime() / 1000);
const endTimestamp = Math.floor(endDate.getTime() / 1000);

// CSV filenames
const tradesFilename = `${HIVE_ACCOUNT}_he-trades_${TOKEN_SYMBOL}_${YEAR}.csv`;
const rewardsFilename = `${HIVE_ACCOUNT}_he-rewards_${TOKEN_SYMBOL}_${YEAR}.csv`;

// Format date for CSV
function formatDate(timestamp) {
  const date = new Date(timestamp * 1000);
  return date.toISOString().replace('T', ' ').substring(0, 19);
}

// Fetch all transactions with pagination
async function getAllTransactions() {
  try {
    console.log(`Fetching all ${TOKEN_SYMBOL} transactions for ${HIVE_ACCOUNT}...`);
    
    const historyAPI = 'https://accounts.hive-engine.com/accountHistory';
    
    // Headers from the network request
    const headers = {
      'accept': 'application/json, text/plain, */*',
      'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,no;q=0.7',
      'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
      'sec-ch-ua-mobile': '?0',
      'sec-ch-ua-platform': '"Linux"',
      'sec-fetch-dest': 'empty',
      'sec-fetch-mode': 'cors',
      'sec-fetch-site': 'cross-site',
      'sec-gpc': '1',
      'Referer': 'https://he.dtools.dev/'
    };
    
    // Use the exact page size the HE Explorer uses
    const pageSize = 50;
    
    // Create an array to hold all transactions
    const allTransactions = [];
    let page = 1;
    let hasMoreData = true;
    
    // Implement pagination using the same approach as HE Explorer
    while (hasMoreData) {
      const offset = (page - 1) * pageSize;
      console.log(`Fetching page ${page} with offset: ${offset}, limit: ${pageSize}`);
      
      try {
        const response = await axios.get(historyAPI, {
          params: {
            account: HIVE_ACCOUNT,
            symbol: TOKEN_SYMBOL,
            limit: pageSize,
            offset: offset
          },
          headers: headers
        });
        
        const transactions = response.data;
        console.log(`Received ${transactions.length} transactions in page ${page}`);
        
        // Add this batch to our results
        allTransactions.push(...transactions);
        
        // If we received fewer results than requested, we've reached the end
        if (transactions.length < pageSize) {
          hasMoreData = false;
          console.log("Reached end of data");
        } else {
          // Otherwise, increase the page and continue
          page++;
        }
        
        // Add a small delay to avoid overwhelming the API
        await new Promise(resolve => setTimeout(resolve, 300));
      } catch (error) {
        console.error(`Error fetching page ${page}:`, error.message);
        if (error.response) {
          console.error('API response:', error.response.status, error.response.statusText);
        }
        
        // If we encounter too many errors, stop
        if (page > 1000) {  // arbitrary large number
          hasMoreData = false;
          console.error("Too many errors or pages, stopping pagination");
        } else {
          page++; // Try next page on error
          await new Promise(resolve => setTimeout(resolve, 1000));
        }
      }
    }
    
    console.log(`Total transactions found across all pages: ${allTransactions.length}`);
    
    // Filter for the year we're interested in
    const filteredTransactions = allTransactions.filter(tx => 
      tx.timestamp >= startTimestamp && tx.timestamp <= endTimestamp
    );
    
    console.log(`Transactions for ${YEAR}: ${filteredTransactions.length}`);
    
    return filteredTransactions;
  } catch (error) {
    console.error('Error in getAllTransactions:', error.message);
    return [];
  }
}

// Process transactions to the required format for both files
function processTransactions(transactions) {
  const trades = [];
  const rewards = [];

  transactions.forEach(tx => {
    const timestamp = formatDate(tx.timestamp);
    const transactionId = tx.transactionId || '';
    const symbol = tx.symbol || '';
    const quantity = tx.quantity || '';
    const from = tx.from || '';
    const to = tx.to || '';
    
    // Replace SWAP.HIVE with HIVE
    const normalizeSymbol = (sym) => sym === 'SWAP.HIVE' ? 'HIVE' : sym;
    
    // Process based on operation type
    switch(tx.operation) {
      case 'market_buy':
        // For market_buy: Tokens received (in), HIVE spent (out)
        trades.push({
          timestamp,
          type: 'Handel',
          inAmount: tx.quantityTokens || '',
          inSymbol: normalizeSymbol(symbol),
          outAmount: tx.quantityHive || '',
          outSymbol: 'HIVE',
          fee: '',
          feeSymbol: '',
          market: 'Hive-Engine',
          note: transactionId
        });
        break;
        
      case 'market_sell':
        // For market_sell: HIVE received (in), Tokens spent (out)
        trades.push({
          timestamp,
          type: 'Handel',
          inAmount: tx.quantityHive || '',
          inSymbol: 'HIVE',
          outAmount: tx.quantityTokens || '',
          outSymbol: normalizeSymbol(symbol),
          fee: '',
          feeSymbol: '',
          market: 'Hive-Engine',
          note: transactionId
        });
        break;
        
      case 'tokens_transfer':
        if (to === HIVE_ACCOUNT) {
          // Incoming transfer - add to rewards as income
          rewards.push({
            timestamp,
            type: 'Inntekt',
            inAmount: quantity,
            inSymbol: normalizeSymbol(symbol),
            outAmount: '',
            outSymbol: '',
            fee: '',
            feeSymbol: '',
            market: 'Hive-Engine',
            note: `${from} tokens_transfer`
          });
        } else if (from === HIVE_ACCOUNT) {
          // Outgoing transfer - add to rewards as outgoing transfer
          rewards.push({
            timestamp,
            type: 'Overføring-ut',
            inAmount: '',
            inSymbol: '',
            outAmount: quantity,
            outSymbol: normalizeSymbol(symbol),
            fee: '',
            feeSymbol: '',
            market: 'Hive-Engine',
            note: `transfer to ${to}`
          });
        }
        break;
        
      case 'tokens_issue':
        // Add to rewards if we're the recipient
        if (to === HIVE_ACCOUNT) {
          rewards.push({
            timestamp,
            type: 'Inntekt',
            inAmount: quantity,
            inSymbol: normalizeSymbol(symbol),
            outAmount: '',
            outSymbol: '',
            fee: '',
            feeSymbol: '',
            market: 'Hive-Engine',
            note: `${from} tokens_issue`
          });
        }
        break;
        
      case 'tokens_stake':
        // Add to rewards only if from ≠ to and we're the recipient
        if (from !== to && to === HIVE_ACCOUNT) {
          rewards.push({
            timestamp,
            type: 'Inntekt',
            inAmount: quantity,
            inSymbol: normalizeSymbol(symbol),
            outAmount: '',
            outSymbol: '',
            fee: '',
            feeSymbol: '',
            market: 'Hive-Engine',
            note: `${from} tokens_stake`
          });
        }
        break;
    }
  });

  return { trades, rewards };
}

// Write transactions to CSV files
function writeCSVFiles(transactions) {
  const { trades, rewards } = processTransactions(transactions);
  
  // CSV header for both files
  const csvHeader = 'Tidspunkt,Type,Inn,Inn-Valuta,Ut,Ut-Valuta,Gebyr,Gebyr-Valuta,Marked,Notat\n';
  
  // Write trades file
  let tradesContent = csvHeader;
  trades.forEach(trade => {
    tradesContent += `${trade.timestamp},${trade.type},${trade.inAmount},${trade.inSymbol},${trade.outAmount},${trade.outSymbol},${trade.fee},${trade.feeSymbol},${trade.market},${trade.note}\n`;
  });
  
  fs.writeFileSync(tradesFilename, tradesContent);
  console.log(`Trades CSV file written to ${tradesFilename} with ${trades.length} rows`);
  
  // Write rewards file
  let rewardsContent = csvHeader;
  rewards.forEach(reward => {
    rewardsContent += `${reward.timestamp},${reward.type},${reward.inAmount},${reward.inSymbol},${reward.outAmount},${reward.outSymbol},${reward.fee},${reward.feeSymbol},${reward.market},${reward.note}\n`;
  });
  
  fs.writeFileSync(rewardsFilename, rewardsContent);
  console.log(`Rewards CSV file written to ${rewardsFilename} with ${rewards.length} rows`);
  
  // Also save the raw data as JSON for inspection
  fs.writeFileSync(`${HIVE_ACCOUNT}_${TOKEN_SYMBOL}_${YEAR}_raw.json`, JSON.stringify(transactions, null, 2));
  console.log(`Raw data written to ${HIVE_ACCOUNT}_${TOKEN_SYMBOL}_${YEAR}_raw.json`);
}

// Main function
async function main() {
  try {
    console.log(`Looking for ${TOKEN_SYMBOL} transactions for ${HIVE_ACCOUNT} in ${YEAR}`);
    
    // Get all transactions
    const transactions = await getAllTransactions();
    
    // Write to CSV files
    if (transactions.length > 0) {
      writeCSVFiles(transactions);
    } else {
      console.log(`No ${TOKEN_SYMBOL} transactions found for ${HIVE_ACCOUNT} in ${YEAR}`);
    }
  } catch (error) {
    console.error('An error occurred:', error);
  }
}

// Run
main();