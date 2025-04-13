// operation_lookup.js
const { Client } = require('@hiveio/dhive');

// Configuration variables
const OPERATION_TYPE = 'claim_reward_balance'; // Change this to look for different operation types
const ACCOUNT_NAME = 'mightpossibly';
const LIMIT = 1; // Number of operations to find

// Initialize client with multiple API endpoints for redundancy
const client = new Client([
  'https://api.hive.blog', 
  'https://api.hivekings.com', 
  'https://anyx.io', 
  'https://api.openhive.network'
]);

async function getAccountOperations(account, operationType, limit = 3) {
  try {
    console.log(`Looking up the last ${limit} ${operationType} operations for ${account}...`);
    
    const operations = [];
    const seenTrxIds = new Set(); // Track unique transaction IDs
    let lastIndex = -1;  // Start from the most recent operations
    const batchSize = 1000;  // Maximum number of operations we can fetch at once
    let fetchCount = 0;
    const maxFetches = 300; // Limit the number of fetches to avoid infinite loops
    
    // Keep fetching batches of history until we find enough operations
    // or until we've gone through the entire history or reached max fetches
    while (operations.length < limit && lastIndex !== 0 && fetchCount < maxFetches) {
      fetchCount++;
      console.log(`Fetching batch ${fetchCount} with last index: ${lastIndex}`);
      
      // Fetch a batch of account history
      const history = await client.database.call('get_account_history', [
        account, 
        lastIndex, 
        batchSize
      ]);
      
      if (history.length === 0) {
        break;
      }
      
      // Find specified operations in this batch
      for (const item of history) {
        if (item[1].op[0] === operationType) {
          const trxId = item[1].trx_id;
          
          // Only add if we haven't seen this transaction before
          if (!seenTrxIds.has(trxId)) {
            seenTrxIds.add(trxId);
            operations.push(item);
            
            if (operations.length >= limit) {
              break;
            }
          }
        }
      }
      
      console.log(`Found ${operations.length} unique ${operationType} ops so far`);
      
      // Find the lowest index in the current batch for the next iteration
      const indices = history.map(item => item[0]);
      const minIndex = Math.min(...indices);
      
      // Set lastIndex to the operation before the minimum in this batch
      lastIndex = minIndex - 1;
      
      if (lastIndex <= 0) {
        lastIndex = 0;  // We've reached the beginning of the account's history
      }
      
      // If we've found enough or reached the beginning, stop fetching
      if (operations.length >= limit || lastIndex === 0) {
        break;
      }
    }
    
    if (operations.length === 0) {
      console.log(`No ${operationType} operations found for ${account}`);
      return [];
    }
    
    console.log(`\nFound ${operations.length} unique ${operationType} operations:`);
    operations.forEach((item, index) => {
      console.log(`\nOperation #${index + 1}:`);
      console.log(JSON.stringify(item, null, 2));
    });
    
    return operations;
    
  } catch (error) {
    console.error(`Error fetching ${operationType} operations:`, error);
    throw error;
  }
}

// Execute the function with the configured parameters
getAccountOperations(ACCOUNT_NAME, OPERATION_TYPE, LIMIT)
  .then(() => {
    console.log('\nLookup complete');
  })
  .catch(error => {
    console.error('Failed to complete lookup:', error);
  });