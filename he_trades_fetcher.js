// hiveTransactionsFetcher.js
const { Client: HiveClient } = require('@hiveio/dhive');
const fs = require('fs');
const path = require('path');
const { createObjectCsvWriter } = require('csv-writer');

// Define configurable variables with default values
const HIVE_ACCOUNT = process.env.HIVE_ACCOUNT || 'mightpossibly';
const YEAR = process.env.YEAR || '2024';
const TOKEN_SYMBOL = process.env.TOKEN_SYMBOL || 'LEO';

// Initialize the Hive client with multiple API nodes for redundancy
const hiveClient = new HiveClient([
  'https://api.hive.blog',
  'https://api.deathwing.me',
  'https://hive-api.arcange.eu'
]);

// Setup CSV writer
const csvWriter = createObjectCsvWriter({
  path: `${HIVE_ACCOUNT}_${TOKEN_SYMBOL}_${YEAR}.csv`,
  header: [
    { id: 'date', title: 'date' },
    { id: 'txid', title: 'txid' }
  ]
});

async function fetchAndFilterTransactions() {
  try {
    console.log(`========================================================`);
    console.log(`Fetching ${TOKEN_SYMBOL} transactions for account ${HIVE_ACCOUNT} in ${YEAR}`);
    console.log(`========================================================`);

    let lastReadIndex = -1; // Start from most recent
    let keepReading = true;
    const MAX_BATCH_SIZE = 1000; // Maximum allowed size
    let totalFound = 0;
    let batchCount = 0;
    let processedOps = 0;
    const transactions = [];
    
    const startDate = new Date(`${YEAR}-01-01T00:00:00Z`);
    const endDate = new Date(`${YEAR}-12-31T23:59:59Z`);
    let reachedDateLimit = false;
    let inTargetYear = false;
    
    // Status display variables
    let lastLogTime = Date.now();
    const logInterval = 5000; // 5 seconds between status updates

    // Fast-forward to our target year
    console.log(`Fast-forwarding to ${YEAR}...`);
    let fastForwardMode = true;

    while (keepReading) {
      batchCount++;
      
      // Adjust batch size for final batch
      let batchSize = MAX_BATCH_SIZE;
      if (lastReadIndex !== -1 && lastReadIndex < MAX_BATCH_SIZE) {
        batchSize = lastReadIndex + 1;
      }

      // Skip if we can't make a valid request
      if (batchSize < 1) {
        break;
      }

      try {
        // Get batch of operations
        const operations = await hiveClient.database.getAccountHistory(
          HIVE_ACCOUNT,
          lastReadIndex,
          batchSize
        );

        if (operations.length === 0) {
          keepReading = false;
          continue;
        }
        
        // Check dates in this batch
        let oldestDateInBatch = null;
        let newestDateInBatch = null;
        let batchHasTargetYear = false;
        
        // First quick scan for date range in this batch
        for (const operation of operations) {
          const [blockNum, transaction] = operation;
          const timestamp = new Date(transaction.timestamp);
          
          if (!oldestDateInBatch || timestamp < oldestDateInBatch) {
            oldestDateInBatch = timestamp;
          }
          
          if (!newestDateInBatch || timestamp > newestDateInBatch) {
            newestDateInBatch = timestamp;
          }
          
          // Check if any transaction is in our target year
          if (timestamp.getFullYear() === parseInt(YEAR)) {
            batchHasTargetYear = true;
          }
        }
        
        // If we're fast-forwarding and still in the future
        if (fastForwardMode) {
          // If this batch is completely in the future (after our target year)
          if (oldestDateInBatch && oldestDateInBatch.getFullYear() > parseInt(YEAR)) {
            // Just update the index and continue - no need to process these
            const [firstIndex] = operations[0];
            lastReadIndex = firstIndex;
            
            const now = Date.now();
            if (now - lastLogTime > logInterval) {
              console.log(`Fast-forwarding... Current date: ${oldestDateInBatch.toISOString().split('T')[0]}`);
              lastLogTime = now;
            }
            
            continue;
          } else {
            // We've reached a batch that might contain our target year
            fastForwardMode = false;
            console.log(`Reached potential ${YEAR} transactions. Beginning detailed scan...`);
          }
        }
        
        // If this batch is completely before our target year, we're done
        if (oldestDateInBatch && newestDateInBatch && 
            newestDateInBatch.getFullYear() < parseInt(YEAR)) {
          console.log(`Reached transactions before ${YEAR}. Scan complete.`);
          keepReading = false;
          continue;
        }
        
        // Now we need to process this batch in detail since it might contain target year transactions
        processedOps += operations.length;
        let batchFound = 0;
        
        for (const operation of operations) {
          const [blockNum, transaction] = operation;
          const [opType, opData] = transaction.op;
          const timestamp = new Date(transaction.timestamp);
          
          // Skip if not in our target year
          if (timestamp.getFullYear() !== parseInt(YEAR)) {
            continue;
          }
          
          if (opType === 'custom_json' && 
              opData.id === 'ssc-mainnet-hive' && 
              (opData.required_auths.includes(HIVE_ACCOUNT) || opData.required_posting_auths.includes(HIVE_ACCOUNT))) {
            
            try {
              const content = JSON.parse(opData.json);
              
              // Check if the transaction involves our token
              if (content.contractName === 'market' && 
                 (content.contractAction === 'buy' || content.contractAction === 'sell') &&
                  content.contractPayload && 
                  content.contractPayload.symbol === TOKEN_SYMBOL) {
                
                transactions.push({
                  date: transaction.timestamp,
                  txid: transaction.trx_id
                });
                
                batchFound++;
              }
            } catch (parseError) {
              console.error(`Error parsing JSON from transaction: ${parseError.message}`);
            }
          }
        }

        totalFound += batchFound;

        // Update the last read index
        if (operations.length > 0) {
          const [firstIndex] = operations[0];
          
          // If we've reached the beginning or the index isn't changing
          if (firstIndex === lastReadIndex || firstIndex === 0) {
            keepReading = false;
          } else {
            lastReadIndex = firstIndex;
            
            // Log progress at intervals to avoid console spam
            const now = Date.now();
            if (now - lastLogTime > logInterval) {
              console.log(`Processed ${processedOps} operations | Total: ${totalFound} ${TOKEN_SYMBOL} transactions | Current batch: ${batchCount}`);
              console.log(`Date range in current batch: ${oldestDateInBatch.toISOString().split('T')[0]} to ${newestDateInBatch.toISOString().split('T')[0]}`);
              
              if (batchFound > 0) {
                console.log(`Found ${batchFound} ${TOKEN_SYMBOL} transactions in latest batch`);
              }
              
              lastLogTime = now;
            }
          }
        } else {
          keepReading = false;
        }

        // Small delay to prevent rate limiting
        await new Promise(resolve => setTimeout(resolve, 100));

      } catch (batchError) {
        console.error(`Error processing batch ${batchCount}:`, batchError.message);
        // If we encounter an error, we'll try again with a smaller batch or different nodes
        if (batchSize > 10) {
          // Try again with a smaller batch
          batchSize = Math.floor(batchSize / 2);
        } else {
          // If we're already at a small batch size, pause and then continue
          console.log('Waiting before retrying...');
          await new Promise(resolve => setTimeout(resolve, 5000));
        }
      }
    }

    console.log(`\n========================================================`);
    console.log(`Scan complete!`);
    console.log(`Total operations processed in detail: ${processedOps}`);
    console.log(`Total ${TOKEN_SYMBOL} transactions found: ${totalFound}`);
    
    // Write results to CSV
    if (transactions.length > 0) {
      await csvWriter.writeRecords(transactions);
      console.log(`Successfully wrote ${transactions.length} transactions to ${HIVE_ACCOUNT}_${TOKEN_SYMBOL}_${YEAR}.csv`);
    } else {
      console.log(`No ${TOKEN_SYMBOL} transactions found for ${HIVE_ACCOUNT} in ${YEAR}`);
    }
    console.log(`========================================================`);
    
  } catch (error) {
    console.error('Error in transaction fetching process:', error);
  }
}

// Run the main function
console.log(`Starting transaction fetch for ${HIVE_ACCOUNT}'s ${TOKEN_SYMBOL} transactions in ${YEAR}...`);
fetchAndFilterTransactions().catch(error => {
  console.error('Unhandled error in main process:', error);
  process.exit(1);
});