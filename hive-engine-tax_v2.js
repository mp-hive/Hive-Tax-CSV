const fs = require('fs');
const axios = require('axios');

// Configuration
const HIVE_ACCOUNT = 'mightpossibly';
const YEAR = '2024';
const TOKEN_SYMBOL = 'LEO';

// Calculate start and end timestamps for the year
const startDate = new Date(`${YEAR}-01-01T00:00:00Z`);
const endDate = new Date(`${YEAR}-12-31T23:59:59Z`);
const startTimestamp = Math.floor(startDate.getTime() / 1000);
const endTimestamp = Math.floor(endDate.getTime() / 1000);

// CSV filename
const csvFilename = `${HIVE_ACCOUNT}_${TOKEN_SYMBOL}_${YEAR}.csv`;

// Format date for CSV
function formatDate(timestamp) {
  const date = new Date(timestamp * 1000);
  return date.toISOString().replace('T', ' ').substring(0, 19);
}

// Fetch all transactions with pagination, mimicking the HE Explorer behavior
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

// Process transactions to add calculated values for market operations
function processTransactions(transactions) {
  return transactions.map(tx => {
    // Make a copy of the transaction to avoid modifying the original
    const processedTx = { ...tx };
    
    // Add calculated values for market operations
    if (tx.operation === 'market_buy') {
      // For market_buy operations:
      // - inAmount: quantityTokens (tokens received)
      // - outAmount: quantityHive (SWAP.HIVE spent)
      processedTx.inAmount = tx.quantityTokens || '';
      processedTx.inSymbol = tx.symbol || '';
      processedTx.outAmount = tx.quantityHive || '';
      processedTx.outSymbol = 'SWAP.HIVE';
    } 
    else if (tx.operation === 'market_sell') {
      // For market_sell operations:
      // - inAmount: quantityHive (SWAP.HIVE received)
      // - outAmount: quantityTokens (tokens spent)
      processedTx.inAmount = tx.quantityHive || '';
      processedTx.inSymbol = 'SWAP.HIVE';
      processedTx.outAmount = tx.quantityTokens || '';
      processedTx.outSymbol = tx.symbol || '';
    }
    else if (tx.operation === 'market_cancel') {
      // For market_cancel, add the returned quantity
      processedTx.inAmount = tx.quantityReturned || '';
      processedTx.inSymbol = tx.orderType === 'buy' ? 'SWAP.HIVE' : tx.symbol;
      processedTx.outAmount = '';
      processedTx.outSymbol = '';
    }
    else if (tx.operation === 'tokens_transfer') {
      // For token transfers
      if (tx.to === HIVE_ACCOUNT) {
        // Received tokens
        processedTx.inAmount = tx.quantity || '';
        processedTx.inSymbol = tx.symbol || '';
        processedTx.outAmount = '';
        processedTx.outSymbol = '';
      } else {
        // Sent tokens
        processedTx.inAmount = '';
        processedTx.inSymbol = '';
        processedTx.outAmount = tx.quantity || '';
        processedTx.outSymbol = tx.symbol || '';
      }
    }
    else if (tx.operation === 'tokens_stake') {
      // For staking tokens (no actual in/out, just a state change)
      processedTx.inAmount = '';
      processedTx.inSymbol = '';
      processedTx.outAmount = tx.quantity || '';
      processedTx.outSymbol = tx.symbol || '';
    }
    else if (tx.operation === 'tokens_unstake') {
      // For unstaking tokens
      processedTx.inAmount = tx.quantity || '';
      processedTx.inSymbol = tx.symbol || '';
      processedTx.outAmount = '';
      processedTx.outSymbol = '';
    }
    else {
      // Default case for other operations
      processedTx.inAmount = '';
      processedTx.inSymbol = '';
      processedTx.outAmount = '';
      processedTx.outSymbol = '';
    }
    
    return processedTx;
  });
}

// Write to CSV file with expanded market details
function writeCSV(data) {
  // Process the transactions to add calculated values
  const processedData = processTransactions(data);
  
  // Create CSV header with new fields
  let csvContent = 'date,txid,operation,symbol,quantity,account,from,to,inAmount,inSymbol,outAmount,outSymbol\n';
  
  processedData.forEach(tx => {
    const date = formatDate(tx.timestamp);
    const transactionId = tx.transactionId || '';
    const operation = tx.operation || '';
    const symbol = tx.symbol || '';
    const quantity = tx.quantity || '';
    const from = tx.from || '';
    const to = tx.to || '';
    const inAmount = tx.inAmount || '';
    const inSymbol = tx.inSymbol || '';
    const outAmount = tx.outAmount || '';
    const outSymbol = tx.outSymbol || '';
    
    csvContent += `${date},${transactionId},${operation},${symbol},${quantity},${HIVE_ACCOUNT},${from},${to},${inAmount},${inSymbol},${outAmount},${outSymbol}\n`;
  });
  
  fs.writeFileSync(csvFilename, csvContent);
  console.log(`CSV file written to ${csvFilename} with ${data.length} rows`);
  
  // Also save the raw data as JSON for inspection
  fs.writeFileSync(`${HIVE_ACCOUNT}_${TOKEN_SYMBOL}_${YEAR}_raw.json`, JSON.stringify(data, null, 2));
  console.log(`Raw data written to ${HIVE_ACCOUNT}_${TOKEN_SYMBOL}_${YEAR}_raw.json`);
}

// Main function
async function main() {
  try {
    console.log(`Looking for ${TOKEN_SYMBOL} transactions for ${HIVE_ACCOUNT} in ${YEAR}`);
    
    // Get all transactions
    const transactions = await getAllTransactions();
    
    // Write to CSV
    if (transactions.length > 0) {
      writeCSV(transactions);
    } else {
      console.log(`No ${TOKEN_SYMBOL} transactions found for ${HIVE_ACCOUNT} in ${YEAR}`);
    }
  } catch (error) {
    console.error('An error occurred:', error);
  }
}

// Run
main();