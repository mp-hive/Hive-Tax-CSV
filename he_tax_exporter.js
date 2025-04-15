// he_tax_exporter.js
const axios = require('axios');
const fs = require('fs');

// Configuration - edit these values as needed
const HIVE_ACCOUNT = 'mightpossibly';
const YEAR = '2024';
const TOKEN_SYMBOL = 'LEO';

// Rate limiting configuration
const DELAY_BETWEEN_REQUESTS = 500; // 500ms delay between API calls
const MAX_RETRIES = 3; // Maximum number of retry attempts
const BATCH_SIZE = 10; // Number of transactions to process at once

// Minimum transaction value (to filter out dust transactions)
const MIN_TRANSACTION_VALUE = 0.001;

/**
 * Creates a delay using a promise
 * @param {number} ms - Milliseconds to delay
 * @returns {Promise} Promise that resolves after the delay
 */
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Converts a date to a timestamp (start of day)
 * @param {string} dateStr - Date string in YYYY-MM-DD format
 * @returns {number} Unix timestamp
 */
function getDateTimestamp(dateStr) {
  const date = new Date(dateStr);
  date.setUTCHours(0, 0, 0, 0);
  return Math.floor(date.getTime() / 1000);
}

/**
 * Formats a timestamp to a date-time string
 * @param {number} timestamp - Unix timestamp
 * @returns {string} Formatted date-time string (YYYY-MM-DD HH:MM:SS)
 */
function formatTimestamp(timestamp) {
  const date = new Date(timestamp * 1000);
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  const hours = String(date.getUTCHours()).padStart(2, '0');
  const minutes = String(date.getUTCMinutes()).padStart(2, '0');
  const seconds = String(date.getUTCSeconds()).padStart(2, '0');
  
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

/**
 * Fetches transactions using pagination with rate limiting
 * @param {string} account - Account name
 * @param {string} symbol - Token symbol
 * @param {number} limit - Number of transactions per page
 * @param {number} offset - Starting offset
 * @returns {Promise<Array>} Array of transactions
 */
async function fetchTransactionPage(account, symbol, limit = 1000, offset = 0) {
  const url = `https://history.hive-engine.com/accountHistory?account=${account}&symbol=${symbol}&limit=${limit}&offset=${offset}`;
  console.log(`Fetching page with offset ${offset}`);
  
  // Add delay before request to avoid rate limiting
  await delay(DELAY_BETWEEN_REQUESTS);
  
  try {
    const response = await axios.get(url);
    return response.data;
  } catch (error) {
    const status = error.response ? error.response.status : 'unknown';
    throw new Error(`HTTP error! status: ${status}`);
  }
}

/**
 * Fetches detailed transaction information with retries
 * @param {string} txId - Transaction ID
 * @param {number} retryCount - Current retry attempt
 * @returns {Promise<Object>} Detailed transaction data
 */
async function fetchTransactionDetails(txId, retryCount = 0) {
  const url = 'https://api.hive-engine.com/rpc/blockchain';
  const payload = {
    jsonrpc: '2.0',
    method: 'getTransactionInfo',
    params: { txid: txId },
    id: 1
  };

  try {
    // Add delay before request to avoid rate limiting
    await delay(DELAY_BETWEEN_REQUESTS * (retryCount + 1)); // Exponential backoff
    
    const response = await axios.post(url, payload, {
      headers: { 'Content-Type': 'application/json' }
    });
    
    return response.data.result;
  } catch (error) {
    const status = error.response ? error.response.status : 'unknown';
    console.error(`Error fetching details for transaction ${txId}: ${error.message}`);
    
    // Retry with exponential backoff if we haven't reached the maximum retries
    if (retryCount < MAX_RETRIES && (status === 503 || status === 429)) {
      console.log(`Retrying (${retryCount + 1}/${MAX_RETRIES})...`);
      return fetchTransactionDetails(txId, retryCount + 1);
    }
    
    console.log(`Max retries reached or non-retryable error. Continuing...`);
    return null;
  }
}

/**
 * Deduplicates transactions by transaction ID
 * @param {Array} transactions - Array of transactions
 * @returns {Array} Deduplicated transactions
 */
function deduplicateTransactions(transactions) {
  const uniqueTxIds = new Set();
  const uniqueTransactions = [];
  let duplicateCount = 0;
  
  // Filter for buy/sell operations
  const buyOrSellTransactions = transactions.filter(tx => 
    tx.operation === 'market_buy' || tx.operation === 'market_sell'
  );
  
  console.log(`Found ${buyOrSellTransactions.length} buy/sell operations before deduplication`);
  
  // Deduplicate by transaction ID
  for (const tx of buyOrSellTransactions) {
    if (!uniqueTxIds.has(tx.transactionId)) {
      uniqueTxIds.add(tx.transactionId);
      uniqueTransactions.push(tx);
    } else {
      duplicateCount++;
    }
  }
  
  console.log(`Removed ${duplicateCount} duplicate transactions`);
  console.log(`Remaining ${uniqueTransactions.length} unique transactions`);
  
  return uniqueTransactions;
}

/**
 * Process transactions in batches with rate limiting
 * @param {Array} transactions - Array of transactions to process
 * @returns {Promise<Array>} Enhanced transactions with details
 */
async function processTransactionsInBatches(transactions) {
  const enhancedTransactions = [];
  
  // Deduplicate transactions by transaction ID
  const uniqueMarketTransactions = deduplicateTransactions(transactions);
  
  // Split transactions into batches
  const batches = [];
  for (let i = 0; i < uniqueMarketTransactions.length; i += BATCH_SIZE) {
    batches.push(uniqueMarketTransactions.slice(i, i + BATCH_SIZE));
  }
  
  console.log(`Split ${uniqueMarketTransactions.length} market transactions into ${batches.length} batches`);
  
  // Track which transaction IDs we've processed
  const processedTxIds = new Set();
  
  // Process each batch sequentially
  for (let i = 0; i < batches.length; i++) {
    console.log(`Processing batch ${i + 1}/${batches.length}`);
    const batch = batches[i];
    
    // Process transactions in the batch one by one
    for (const tx of batch) {
      // Double-check we haven't processed this transaction already
      if (processedTxIds.has(tx.transactionId)) {
        console.log(`Skipping already processed transaction ${tx.transactionId}`);
        continue;
      }
      
      // Mark this transaction as processed
      processedTxIds.add(tx.transactionId);
      
      console.log(`Fetching details for transaction ${tx.transactionId} (${tx.operation})...`);
      const details = await fetchTransactionDetails(tx.transactionId);
      
      if (details) {
        // Parse the logs JSON string if it's a string
        details.logs = typeof details.logs === 'string' 
          ? JSON.parse(details.logs) 
          : details.logs;
        
        // Parse the payload JSON string if it's a string
        details.payload = typeof details.payload === 'string' 
          ? JSON.parse(details.payload) 
          : details.payload;
          
        enhancedTransactions.push({ ...tx, details });
      } else {
        // Skip transactions where we couldn't get details
        console.log(`Skipping transaction ${tx.transactionId} due to missing details`);
      }
    }
    
    // Add a longer delay between batches
    if (i < batches.length - 1) {
      console.log(`Batch ${i + 1} complete. Pausing before next batch...`);
      await delay(DELAY_BETWEEN_REQUESTS * 2);
    }
  }
  
  // Add non-market transactions (rewards, etc.)
  const nonMarketTransactions = transactions.filter(tx => 
    !tx.operation.startsWith('market_') || 
    (tx.operation !== 'market_buy' && tx.operation !== 'market_sell')
  );
  enhancedTransactions.push(...nonMarketTransactions);
  
  return enhancedTransactions;
}

/**
 * Fetches transactions for a specific year
 * @param {string} account - Account name
 * @param {string} symbol - Token symbol
 * @param {string} year - Year to fetch transactions for
 * @returns {Promise<Array>} Array of transactions
 */
async function fetchYearTransactions(account, symbol, year) {
  console.log(`\nSearching for ${symbol} transactions for ${account} in ${year}`);
  
  const startDate = `${year}-01-01`;
  const endDate = `${parseInt(year) + 1}-01-01`;
  
  const startTimestamp = getDateTimestamp(startDate);
  const endTimestamp = getDateTimestamp(endDate);
  
  try {
    let allTransactions = [];
    let offset = 0;
    const pageSize = 1000;
    let keepFetching = true;
    let oldestTxTimestamp = Infinity;
    
    while (keepFetching) {
      const transactions = await fetchTransactionPage(account, symbol, pageSize, offset);
      
      if (transactions.length === 0) {
        break;
      }
      
      oldestTxTimestamp = transactions[transactions.length - 1].timestamp;
      console.log(`Oldest transaction in current page: ${new Date(oldestTxTimestamp * 1000).toISOString().split('T')[0]}`);
      
      allTransactions = [...allTransactions, ...transactions];
      
      // Stop if we've gone past our start date or if we've reached a reasonable limit
      if (oldestTxTimestamp < startTimestamp || offset >= 5000) {
        keepFetching = false;
      }
      
      offset += pageSize;
    }

    console.log(`\nTotal transactions fetched: ${allTransactions.length}`);

    // Filter for year
    const yearTransactions = allTransactions.filter(tx => {
      return tx.timestamp >= startTimestamp && tx.timestamp < endTimestamp;
    });

    console.log(`Found ${yearTransactions.length} transactions for ${year}`);
    
    // Process transactions in batches with rate limiting
    console.log('\nFetching detailed information for market transactions...');
    const enhancedTransactions = await processTransactionsInBatches(yearTransactions);

    // Sort transactions by timestamp (oldest first)
    enhancedTransactions.sort((a, b) => a.timestamp - b.timestamp);

    return enhancedTransactions;
  } catch (error) {
    console.error('Error fetching transactions:', error);
    throw error;
  }
}

/**
 * Processes transactions for trade CSV
 * @param {Array} transactions - Array of transactions
 * @returns {Array} Processed trade transactions
 */
function processTradeTransactions(transactions) {
  const tradeTransactions = [];
  const processedTxIds = new Set(); // Track transaction IDs for logging
  
  for (const tx of transactions) {
    // Only include buy and sell transactions with details
    if ((tx.operation !== 'market_buy' && tx.operation !== 'market_sell') || !tx.details) {
      continue;
    }
    
    // Skip transactions with missing or invalid details
    if (!tx.details.logs || !tx.details.logs.events || !tx.details.payload) {
      console.log(`Skipping transaction ${tx.transactionId} due to missing details`);
      continue;
    }
    
    // Add to processed IDs set for logging
    processedTxIds.add(tx.transactionId);
    
    const timestamp = formatTimestamp(tx.timestamp);
    const events = tx.details.logs.events || [];
    const transferEvents = events.filter(e => e.event.includes('transfer') && e.data);
    
    if (tx.operation === 'market_buy') {
      // Extract all SWAP.HIVE transfers from account to market (what we paid)
      const hiveSent = transferEvents.filter(e => 
        e.data.from === HIVE_ACCOUNT && e.data.symbol === 'SWAP.HIVE'
      );
      
      // Extract all SWAP.HIVE transfers from market to account (refunds)
      const hiveRefunds = transferEvents.filter(e => 
        e.data.to === HIVE_ACCOUNT && e.data.symbol === 'SWAP.HIVE'
      );
      
      // Extract all TOKEN_SYMBOL transfers from market to account (what we bought)
      const tokenReceived = transferEvents.filter(e => 
        e.data.to === HIVE_ACCOUNT && e.data.symbol === TOKEN_SYMBOL
      );
      
      // Extract all SWAP.HIVE transfers from market to others (what was paid to sellers)
      const hiveTradedToSellers = transferEvents.filter(e => 
        e.data.from === 'market' && e.data.to !== HIVE_ACCOUNT && e.data.symbol === 'SWAP.HIVE'
      );
      
      // Handle case where we can identify actual trades
      if (hiveSent.length > 0 && tokenReceived.length > 0) {
        // Calculate total values
        const totalHiveSent = hiveSent.reduce((sum, e) => sum + parseFloat(e.data.quantity), 0);
        const totalHiveRefunded = hiveRefunds.reduce((sum, e) => sum + parseFloat(e.data.quantity), 0);
        const totalHiveTraded = totalHiveSent - totalHiveRefunded;
        const totalTokensReceived = tokenReceived.reduce((sum, e) => sum + parseFloat(e.data.quantity), 0);
        
        console.log(`Buy transaction ${tx.transactionId}:`);
        console.log(`- Total HIVE sent: ${totalHiveSent.toFixed(8)}`);
        console.log(`- Total HIVE refunded: ${totalHiveRefunded.toFixed(8)}`);
        console.log(`- Net HIVE traded: ${totalHiveTraded.toFixed(8)}`);
        console.log(`- Total ${TOKEN_SYMBOL} received: ${totalTokensReceived.toFixed(8)}`);
        
        // Process each token receipt as a separate entry
        for (const tokenEvent of tokenReceived) {
          const tokenAmount = parseFloat(tokenEvent.data.quantity);
          
          // Skip very small transactions
          if (tokenAmount < MIN_TRANSACTION_VALUE) {
            console.log(`Skipping small trade: ${tokenAmount.toFixed(8)} ${TOKEN_SYMBOL}`);
            continue;
          }
          
          // Calculate proportional HIVE amount for this token amount
          const hiveAmount = totalHiveTraded * (tokenAmount / totalTokensReceived);
          
          // Skip if calculated HIVE amount is too small
          if (hiveAmount < MIN_TRANSACTION_VALUE) {
            console.log(`Skipping trade with small HIVE amount: ${hiveAmount.toFixed(8)} HIVE`);
            continue;
          }
          
          tradeTransactions.push({
            timestamp,
            type: 'Handel',
            innAmount: tokenAmount.toFixed(8),
            innCurrency: TOKEN_SYMBOL,
            utAmount: hiveAmount.toFixed(8),
            utCurrency: 'HIVE',
            gebyr: '',
            gebyrCurrency: '',
            marked: 'Hive-Engine',
            notat: ''
          });
          
          console.log(`Added trade: ${tokenAmount.toFixed(8)} ${TOKEN_SYMBOL} for ${hiveAmount.toFixed(8)} HIVE`);
        }
      } else {
        console.log(`Skipping buy transaction ${tx.transactionId}: No valid transfer pairs found`);
      }
    } 
    else if (tx.operation === 'market_sell') {
      // Extract all TOKEN_SYMBOL transfers from account to market (what we sold)
      const tokenSent = transferEvents.filter(e => 
        e.data.from === HIVE_ACCOUNT && e.data.symbol === TOKEN_SYMBOL
      );
      
      // Extract all TOKEN_SYMBOL transfers from market to account (refunds)
      const tokenRefunds = transferEvents.filter(e => 
        e.data.to === HIVE_ACCOUNT && e.data.symbol === TOKEN_SYMBOL
      );
      
      // Extract all SWAP.HIVE transfers from market to account (what we received)
      const hiveReceived = transferEvents.filter(e => 
        e.data.to === HIVE_ACCOUNT && e.data.symbol === 'SWAP.HIVE'
      );
      
      // Handle case where we can identify actual trades
      if (tokenSent.length > 0 && hiveReceived.length > 0) {
        // Calculate total values
        const totalTokensSent = tokenSent.reduce((sum, e) => sum + parseFloat(e.data.quantity), 0);
        const totalTokensRefunded = tokenRefunds.reduce((sum, e) => sum + parseFloat(e.data.quantity), 0);
        const totalTokensTraded = totalTokensSent - totalTokensRefunded;
        const totalHiveReceived = hiveReceived.reduce((sum, e) => sum + parseFloat(e.data.quantity), 0);
        
        console.log(`Sell transaction ${tx.transactionId}:`);
        console.log(`- Total ${TOKEN_SYMBOL} sent: ${totalTokensSent.toFixed(8)}`);
        console.log(`- Total ${TOKEN_SYMBOL} refunded: ${totalTokensRefunded.toFixed(8)}`);
        console.log(`- Net ${TOKEN_SYMBOL} traded: ${totalTokensTraded.toFixed(8)}`);
        console.log(`- Total HIVE received: ${totalHiveReceived.toFixed(8)}`);
        
        // Create a single entry for the entire sell transaction
        // This is different from buy transactions because sell transactions are typically simpler
        if (totalTokensTraded > MIN_TRANSACTION_VALUE && totalHiveReceived > MIN_TRANSACTION_VALUE) {
          tradeTransactions.push({
            timestamp,
            type: 'Handel',
            innAmount: totalHiveReceived.toFixed(8),
            innCurrency: 'HIVE',
            utAmount: totalTokensTraded.toFixed(8),
            utCurrency: TOKEN_SYMBOL,
            gebyr: '',
            gebyrCurrency: '',
            marked: 'Hive-Engine',
            notat: ''
          });
          
          console.log(`Added trade: ${totalHiveReceived.toFixed(8)} HIVE for ${totalTokensTraded.toFixed(8)} ${TOKEN_SYMBOL}`);
        } else {
          console.log(`Skipping small sell trade: ${totalHiveReceived.toFixed(8)} HIVE for ${totalTokensTraded.toFixed(8)} ${TOKEN_SYMBOL}`);
        }
      } else {
        console.log(`Skipping sell transaction ${tx.transactionId}: No valid transfer pairs found`);
      }
    }
  }
  
  console.log(`Processed ${processedTxIds.size} unique transaction IDs, extracted ${tradeTransactions.length} trades`);
  
  return tradeTransactions;
}

/**
 * Processes transactions for rewards CSV
 * @param {Array} transactions - Array of transactions
 * @returns {Array} Processed reward transactions
 */
function processRewardTransactions(transactions) {
  const rewardTransactions = [];
  
  for (const tx of transactions) {
    if (tx.operation === 'tokens_issue' || tx.operation === 'tokens_stake') {
      // Only include transactions where HIVE_ACCOUNT is the recipient and not the sender
      if (tx.to === HIVE_ACCOUNT && tx.from !== HIVE_ACCOUNT) {
        const timestamp = formatTimestamp(tx.timestamp);
        
        rewardTransactions.push({
          timestamp,
          type: 'Inntekt',
          innAmount: tx.quantity || '',
          innCurrency: TOKEN_SYMBOL,
          utAmount: '',
          utCurrency: '',
          gebyr: '',
          gebyrCurrency: '',
          marked: 'Hive-Engine',
          notat: tx.from || ''
        });
      }
    }
  }
  
  return rewardTransactions;
}

/**
 * Converts transactions to CSV format
 * @param {Array} transactions - Array of processed transactions
 * @returns {string} CSV content
 */
function generateCSV(transactions) {
  const headers = [
    'Tidspunkt', 'Type', 'Inn', 'Inn-Valuta', 
    'Ut', 'Ut-Valuta', 'Gebyr', 'Gebyr-Valuta', 
    'Marked', 'Notat'
  ].join(',');
  
  const rows = transactions.map(tx => [
    tx.timestamp,
    tx.type,
    tx.innAmount,
    tx.innCurrency,
    tx.utAmount,
    tx.utCurrency,
    tx.gebyr,
    tx.gebyrCurrency,
    tx.marked,
    tx.notat
  ].join(','));
  
  return [headers, ...rows].join('\n');
}

/**
 * Main function
 */
async function main() {
  try {
    console.log(`Starting tax report generation for ${HIVE_ACCOUNT} - ${TOKEN_SYMBOL} - ${YEAR}`);
    
    // Fetch all transactions for the year
    const transactions = await fetchYearTransactions(HIVE_ACCOUNT, TOKEN_SYMBOL, YEAR);
    
    // Process trade transactions
    const tradeTransactions = processTradeTransactions(transactions);
    console.log(`Processed ${tradeTransactions.length} trade transactions`);
    
    // Process reward transactions
    const rewardTransactions = processRewardTransactions(transactions);
    console.log(`Processed ${rewardTransactions.length} reward transactions`);
    
    // Generate CSV content
    const tradeCSV = generateCSV(tradeTransactions);
    const rewardCSV = generateCSV(rewardTransactions);
    
    // Write to files
    const tradeFilename = `${HIVE_ACCOUNT}_he-trades_${TOKEN_SYMBOL}_${YEAR}.csv`;
    const rewardFilename = `${HIVE_ACCOUNT}_he-rewards_${TOKEN_SYMBOL}_${YEAR}.csv`;
    
    fs.writeFileSync(tradeFilename, tradeCSV);
    fs.writeFileSync(rewardFilename, rewardCSV);
    
    console.log(`\nCSV files generated successfully:`);
    console.log(`- ${tradeFilename}`);
    console.log(`- ${rewardFilename}`);
    
  } catch (error) {
    console.error('Error generating tax report:', error);
  }
}

// Run the script
main();