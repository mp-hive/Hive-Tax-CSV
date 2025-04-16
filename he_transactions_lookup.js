const fs = require('fs');
const path = require('path');
const axios = require('axios');
const csvParser = require('csv-parser');

// Configuration variables (can be easily changed)
const HIVE_ACCOUNT = 'mightpossibly'; // Default account
const YEAR = '2024'; // Default year
const TOKEN_SYMBOL = 'LEO'; // Default token

// API URL - using the successful endpoint
const HIVE_ENGINE_API = 'https://herpc.dtools.dev/blockchain';

// File paths
const inputFilePath = path.join(__dirname, `${HIVE_ACCOUNT}_${TOKEN_SYMBOL}_${YEAR}.csv`);
const outputFilePath = path.join(__dirname, `${HIVE_ACCOUNT}_he-trades_${TOKEN_SYMBOL}_${YEAR}.csv`);

// Check if output file exists and create with headers if it doesn't
function initializeOutputCsv() {
  if (!fs.existsSync(outputFilePath)) {
    const headers = 'Tidspunkt,Type,Inn,Inn-Valuta,Ut,Ut-Valuta,Gebyr,Gebyr-Valuta,Marked,Notat\n';
    fs.writeFileSync(outputFilePath, headers);
    console.log(`Created output file with headers: ${outputFilePath}`);
  } else {
    console.log(`Output file already exists: ${outputFilePath}`);
  }
}

// Function to append trades to CSV directly
function appendTradesToCsv(trades) {
  if (!trades || trades.length === 0) return;
  
  let csvData = '';
  
  for (const trade of trades) {
    csvData += `${trade.time},${trade.type},${trade.inAmount},${trade.inCurrency},${trade.outAmount},${trade.outCurrency},${trade.fee},${trade.feeCurrency},${trade.market},${trade.note}\n`;
  }
  
  fs.appendFileSync(outputFilePath, csvData);
  console.log(`Appended ${trades.length} trades to ${outputFilePath}`);
}

// Function to read transaction IDs from CSV
async function readTransactionIds() {
  const txIds = [];
  
  return new Promise((resolve, reject) => {
    fs.createReadStream(inputFilePath)
      .pipe(csvParser())
      .on('data', (row) => {
        txIds.push({
          date: row.date,
          txid: row.txid
        });
      })
      .on('end', () => {
        console.log(`Read ${txIds.length} transaction IDs from CSV`);
        resolve(txIds);
      })
      .on('error', (error) => {
        reject(error);
      });
  });
}

// Function to fetch transaction details with retry logic
async function fetchTransactionDetails(txid, retries = 3) {
  let attempts = 0;
  
  while (attempts < retries) {
    try {
      const response = await axios.post(HIVE_ENGINE_API, {
        jsonrpc: '2.0',
        id: 1,
        method: 'getTransactionInfo',
        params: {
          txid: txid
        }
      }, {
        timeout: 15000 // 15 second timeout
      });
      
      if (response.data && response.data.result) {
        return response.data.result;
      }
      
      // If we got a response but no result, retry
      attempts++;
      console.log(`No data found for transaction ${txid}, attempt ${attempts}/${retries}`);
    } catch (error) {
      attempts++;
      console.error(`Error fetching transaction ${txid}, attempt ${attempts}/${retries}: ${error.message}`);
      
      if (attempts >= retries) {
        console.error(`Failed to fetch transaction ${txid} after ${retries} attempts.`);
        return null;
      }
      
      // Wait longer between retries
      const delay = Math.pow(2, attempts) * 1000; // Exponential backoff
      console.log(`Retrying ${txid} in ${delay/1000} seconds...`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  
  return null;
}

// Function to format date to local format
function formatDate(dateString) {
  const date = new Date(dateString);
  return date.toLocaleString('no-NO', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  }).replace(',', '');
}

// Function to parse the logs JSON string into an object
function parseLogsJson(logsJsonString) {
  try {
    return JSON.parse(logsJsonString);
  } catch (error) {
    console.error('Error parsing logs JSON:', error.message);
    return { events: [] };
  }
}

// Function to process a transaction and extract individual trades
function processTransaction(tx, date, txid) {
  if (!tx) {
    return [];
  }

  const formattedDate = formatDate(date);
  const results = [];
  
  // Parse logs from JSON string to object
  const logs = parseLogsJson(tx.logs);
  const events = logs.events || [];
  
  const action = tx.action;
  const sender = tx.sender;
  
  console.log(`Processing ${action} transaction with ${events.length} events from ${sender}`);
  
  // Debug - print all transfers for inspection
  const allTransfers = events.filter(e => 
    e.event === 'transferToContract' || e.event === 'transferFromContract'
  );
  
  console.log(`Found ${allTransfers.length} transfers in this transaction`);
  allTransfers.forEach((e, i) => {
    console.log(`Transfer ${i+1}: ${e.event} ${e.data.from} -> ${e.data.to}: ${e.data.quantity} ${e.data.symbol}`);
  });
  
  // Process buy actions (our account is buying TOKEN with HIVE)
  if (action === 'buy' && sender === HIVE_ACCOUNT) {
    // Get all token transfers to us
    const tokenTransfersToUs = events.filter(event => 
      event.event === 'transferFromContract' && 
      event.data.to === HIVE_ACCOUNT && 
      event.data.symbol === TOKEN_SYMBOL
    );
    
    console.log(`Found ${tokenTransfersToUs.length} ${TOKEN_SYMBOL} transfers to us`);
    
    // Get all HIVE transfers to sellers
    const hiveTransfersToSellers = events.filter(event => 
      event.event === 'transferFromContract' && 
      event.data.to !== HIVE_ACCOUNT && 
      event.data.symbol === 'SWAP.HIVE'
    );
    
    console.log(`Found ${hiveTransfersToSellers.length} HIVE transfers to sellers`);
    
    // For each token transfer to us, try to find a corresponding seller
    tokenTransfersToUs.forEach((tokenTransfer, index) => {
      const tokenAmount = parseFloat(tokenTransfer.data.quantity);
      
      // Find the seller that most likely corresponds to this token transfer
      // We'll use the HIVE transfer that's closest to this token transfer in the event log
      const tokenEventIndex = events.findIndex(e => e === tokenTransfer);
      
      // Look for HIVE transfers to sellers nearby
      // We'll look both before and after the token transfer, with a priority on after
      let bestSellerMatch = null;
      let closestDistance = Number.MAX_SAFE_INTEGER;
      
      hiveTransfersToSellers.forEach(hiveTransfer => {
        const hiveEventIndex = events.findIndex(e => e === hiveTransfer);
        const distance = Math.abs(hiveEventIndex - tokenEventIndex);
        
        // If this HIVE transfer is closer to the token transfer than any we've seen so far
        if (distance < closestDistance) {
          closestDistance = distance;
          bestSellerMatch = hiveTransfer;
        }
      });
      
      if (bestSellerMatch) {
        const sellerAccount = bestSellerMatch.data.to;
        const hiveAmount = parseFloat(bestSellerMatch.data.quantity);
        
        console.log(`Matched: ${tokenAmount} ${TOKEN_SYMBOL} received, ${hiveAmount} HIVE paid to ${sellerAccount}`);
        
        results.push({
          time: formattedDate,
          type: 'Handel',
          inAmount: tokenAmount.toFixed(8),
          inCurrency: TOKEN_SYMBOL,
          outAmount: hiveAmount.toFixed(8),
          outCurrency: 'HIVE',
          fee: '',
          feeCurrency: '',
          market: 'Hive-Engine',
          note: `txid:${txid} seller:${sellerAccount}`
        });
        
        // Remove this HIVE transfer so it doesn't get matched again
        const indexToRemove = hiveTransfersToSellers.findIndex(e => e === bestSellerMatch);
        if (indexToRemove !== -1) {
          hiveTransfersToSellers.splice(indexToRemove, 1);
        }
      } else {
        console.log(`Warning: Could not find a matching seller for ${tokenAmount} ${TOKEN_SYMBOL} received`);
      }
    });
  } 
  else if (action === 'sell' && sender === HIVE_ACCOUNT) {
    // Get all HIVE transfers to us
    const hiveTransfersToUs = events.filter(event => 
      event.event === 'transferFromContract' && 
      event.data.to === HIVE_ACCOUNT && 
      event.data.symbol === 'SWAP.HIVE'
    );
    
    console.log(`Found ${hiveTransfersToUs.length} HIVE transfers to us`);
    
    // Get all token transfers to buyers
    const tokenTransfersToBuyers = events.filter(event => 
      event.event === 'transferFromContract' && 
      event.data.to !== HIVE_ACCOUNT && 
      event.data.symbol === TOKEN_SYMBOL
    );
    
    console.log(`Found ${tokenTransfersToBuyers.length} ${TOKEN_SYMBOL} transfers to buyers`);
    
    // For each HIVE transfer to us, try to find a corresponding buyer
    hiveTransfersToUs.forEach((hiveTransfer, index) => {
      const hiveAmount = parseFloat(hiveTransfer.data.quantity);
      
      // Find the buyer that most likely corresponds to this HIVE transfer
      const hiveEventIndex = events.findIndex(e => e === hiveTransfer);
      
      // Look for token transfers to buyers nearby
      let bestBuyerMatch = null;
      let closestDistance = Number.MAX_SAFE_INTEGER;
      
      tokenTransfersToBuyers.forEach(tokenTransfer => {
        const tokenEventIndex = events.findIndex(e => e === tokenTransfer);
        const distance = Math.abs(tokenEventIndex - hiveEventIndex);
        
        // If this token transfer is closer to the HIVE transfer than any we've seen so far
        if (distance < closestDistance) {
          closestDistance = distance;
          bestBuyerMatch = tokenTransfer;
        }
      });
      
      if (bestBuyerMatch) {
        const buyerAccount = bestBuyerMatch.data.to;
        const tokenAmount = parseFloat(bestBuyerMatch.data.quantity);
        
        console.log(`Matched: ${hiveAmount} HIVE received, ${tokenAmount} ${TOKEN_SYMBOL} paid to ${buyerAccount}`);
        
        results.push({
          time: formattedDate,
          type: 'Handel',
          inAmount: hiveAmount.toFixed(8),
          inCurrency: 'HIVE',
          outAmount: tokenAmount.toFixed(8),
          outCurrency: TOKEN_SYMBOL,
          fee: '',
          feeCurrency: '',
          market: 'Hive-Engine',
          note: `txid:${txid} buyer:${buyerAccount}`
        });
        
        // Remove this token transfer so it doesn't get matched again
        const indexToRemove = tokenTransfersToBuyers.findIndex(e => e === bestBuyerMatch);
        if (indexToRemove !== -1) {
          tokenTransfersToBuyers.splice(indexToRemove, 1);
        }
      } else {
        console.log(`Warning: Could not find a matching buyer for ${hiveAmount} HIVE received`);
      }
    });
  }
  
  // If we found trades, log them for debugging
  if (results.length > 0) {
    console.log(`Found ${results.length} individual trades:`);
    results.forEach((trade, i) => {
      if (action === 'buy') {
        console.log(`Trade ${i+1}: Bought ${trade.inAmount} ${trade.inCurrency} for ${trade.outAmount} ${trade.outCurrency} from ${trade.note.split('seller:')[1]}`);
      } else if (action === 'sell') {
        console.log(`Trade ${i+1}: Sold ${trade.outAmount} ${trade.outCurrency} for ${trade.inAmount} ${trade.inCurrency} to ${trade.note.split('buyer:')[1]}`);
      }
    });
  } else {
    console.log(`No trades identified in this transaction.`);
  }
  
  return results;
}

// Function to add delay between API calls
async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Main function to execute the script
async function main() {
  try {
    console.log(`Fetching ${TOKEN_SYMBOL} transactions for ${HIVE_ACCOUNT} in ${YEAR}`);
    
    // Initialize the output CSV file
    initializeOutputCsv();
    
    // Read transaction IDs from CSV
    const txIds = await readTransactionIds();
    
    // Process each transaction
    let successCount = 0;
    let failCount = 0;
    let totalTradesCount = 0;
    
    for (const [index, tx] of txIds.entries()) {
      console.log(`\nProcessing transaction ${index + 1}/${txIds.length}: ${tx.txid}`);
      
      const txDetails = await fetchTransactionDetails(tx.txid);
      
      if (txDetails) {
        const trades = processTransaction(txDetails, tx.date, tx.txid);
        
        if (trades.length > 0) {
          console.log(`Found ${trades.length} individual trades in transaction ${tx.txid}`);
          
          // Append trades to CSV immediately
          appendTradesToCsv(trades);
          
          totalTradesCount += trades.length;
          successCount++;
        } else {
          console.log(`No relevant trades found in transaction ${tx.txid}`);
        }
      } else {
        failCount++;
        console.error(`Failed to fetch details for transaction ${tx.txid}`);
      }
      
      // Add a delay between requests to avoid overwhelming the API
      await sleep(1000);
    }
    
    console.log(`\nProcessing complete!`);
    console.log(`Summary: ${successCount} transactions processed successfully, ${failCount} failed.`);
    console.log(`Total individual trades written to CSV: ${totalTradesCount}`);
    console.log(`Output file: ${outputFilePath}`);
    
  } catch (error) {
    console.error('Error:', error);
  }
}

// Execute the script
main();