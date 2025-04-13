const { Client } = require('@hiveio/dhive');
const fs = require('fs');

// Configuration variables - easily adjustable
const HIVE_ACCOUNT = 'mightpossibly';
const YEAR = '2024';

// Initialize Hive client with multiple nodes for redundancy
const client = new Client([
  'https://api.hive.blog',
  'https://api.openhive.network',
  'https://api.deathwing.me'
]);

// Configure date range for the specified year
const startDate = new Date(`${YEAR}-01-01T00:00:00Z`);
const endDate = new Date(`${parseInt(YEAR) + 1}-01-01T00:00:00Z`);

// CSV header
const CSV_HEADER = 'Tidspunkt,Type,Inn,Inn-Valuta,Ut,Ut-Valuta,Gebyr,Gebyr-Valuta,Marked,Notat';

// Operation types we're interested in
const TARGET_OPERATIONS = [
  'fill_order',
  'interest',
  'transfer',
  'fill_recurrent_transfer',
  'fill_convert_request',
  'claim_reward_balance'
];

// Global props cache by day (to reduce API calls)
const globalPropsCache = {};

/**
 * Exponential backoff retry for API calls
 * @param {Function} apiCall - Function that returns a promise
 * @param {number} maxRetries - Maximum number of retries
 * @returns {Promise} - Result of the API call
 */
async function withRetry(apiCall, maxRetries = 3) {
  let lastError;
  for (let retry = 0; retry <= maxRetries; retry++) {
    try {
      return await apiCall();
    } catch (error) {
      lastError = error;
      if (retry < maxRetries) {
        // Exponential backoff
        const delay = Math.pow(2, retry) * 500 + Math.random() * 500;
        console.log(`API call failed, retrying in ${Math.round(delay)}ms...`);
        await new Promise(resolve => setTimeout(resolve, delay));
        
        // Try a different node if available
        if (client.clients.length > 1) {
          const currentIndex = client.clients.indexOf(client.currentNode);
          client.currentNode = client.clients[(currentIndex + 1) % client.clients.length];
        }
      }
    }
  }
  throw lastError;
}

/**
 * Get global properties for a specific date
 * @param {string} dateString - ISO date string
 * @returns {Promise<Object>} - Global properties
 */
async function getGlobalPropertiesForDate(dateString) {
  // Use just the date part as the cache key (no need for time precision)
  const dateKey = dateString.split('T')[0];
  
  if (globalPropsCache[dateKey]) {
    return globalPropsCache[dateKey];
  }
  
  try {
    const props = await withRetry(() => client.database.getDynamicGlobalProperties());
    globalPropsCache[dateKey] = props;
    return props;
  } catch (error) {
    console.error(`Error getting global properties for ${dateKey}:`, error.message);
    throw error;
  }
}

/**
 * Convert VESTS to Hive Power (HP)
 * @param {number} vests - Amount of VESTS to convert
 * @param {Object} props - Global properties
 * @returns {number} - Equivalent HP amount
 */
function vestsToHp(vests, props) {
  const totalVests = parseFloat(props.total_vesting_shares.split(' ')[0]);
  const totalHive = parseFloat(props.total_vesting_fund_hive.split(' ')[0]);
  const hivePerVest = totalHive / totalVests;
  return vests * hivePerVest;
}

/**
 * Format date string for CSV
 * @param {string} dateString - ISO date string
 * @returns {string} - Formatted date string
 */
function formatDate(dateString) {
  const date = new Date(dateString);
  return date.toISOString().replace('T', ' ').substring(0, 19);
}

/**
 * Parse amount and currency from string
 * @param {string} amountString - String like "1.000 HIVE"
 * @returns {Object} - Object with amount and currency
 */
function parseAmount(amountString) {
  const parts = amountString.split(' ');
  return {
    amount: parseFloat(parts[0]),
    currency: parts[1]
  };
}

/**
 * Check if an amount is considered dust
 * @param {number} amount - Amount to check
 * @param {string} currency - Currency of the amount
 * @returns {boolean} - True if the amount is dust
 */
function isDust(amount, currency) {
  return currency === 'HIVE' && amount <= 0.01;
}

/**
 * Sanitize and truncate note text for CSV compatibility
 * @param {string} note - The original note text
 * @param {number} maxLength - Maximum length (default: 200)
 * @returns {string} - Sanitized and truncated note
 */
function sanitizeNote(note, maxLength = 200) {
  if (!note) return '';
  
  // Replace any newlines, commas, or other CSV-problematic characters with spaces
  let sanitized = note.replace(/[\n\r,]/g, ' ').trim();
  
  // Collapse multiple spaces into a single space
  sanitized = sanitized.replace(/\s+/g, ' ');
  
  // Truncate if longer than maxLength
  if (sanitized.length > maxLength) {
    sanitized = sanitized.substring(0, maxLength - 3) + '...';
  }
  
  return sanitized;
}

/**
 * Format transaction object to CSV line with proper escaping
 * @param {Object} tx - Transaction object
 * @returns {string} - CSV line
 */
function formatCsvLine(tx) {
  // Function to properly escape CSV fields
  const escapeField = (field) => {
    if (field === null || field === undefined || field === '') {
      return '';
    }
    
    // Convert to string if it's not already
    const str = String(field);
    
    // If it contains quotes, commas, or newlines, wrap in quotes and escape internal quotes
    if (str.includes('"') || str.includes(',') || str.includes('\n') || str.includes('\r')) {
      return `"${str.replace(/"/g, '""')}"`;
    }
    
    return str;
  };
  
  return [
    escapeField(tx.timestamp),
    escapeField(tx.type),
    escapeField(tx.inn),
    escapeField(tx.innCurrency),
    escapeField(tx.ut),
    escapeField(tx.utCurrency),
    escapeField(tx.fee),
    escapeField(tx.feeCurrency),
    escapeField(tx.market),
    escapeField(tx.note)
  ].join(',');
}

/**
 * Fetch account history operations of a specific type within date range
 * @param {string} account - Account name
 * @param {string} opType - Operation type
 * @param {number} [batchSize=1000] - Number of operations to fetch per batch
 * @returns {Promise<Array>} - Transactions of the specified type within date range
 */
async function fetchOperationHistory(account, opType, batchSize = 1000) {
  let transactions = [];
  let start = -1;
  let done = false;
  let batchCount = 0;
  let totalFound = 0;
  let lastProgress = 0;
  
  console.log(`Fetching ${opType} operations...`);
  
  while (!done) {
    batchCount++;
    
    try {
      const history = await withRetry(() => {
        return client.call('condenser_api', 'get_account_history', [account, start, batchSize]);
      });
      
      if (!history || history.length === 0) {
        done = true;
        continue;
      }
      
      let matchCount = 0;
      let foundOlderThanStart = false;
      
      // Filter operations by type and date range
      for (const item of history) {
        const tx = item[1];
        const txDate = new Date(tx.timestamp);
        const index = item[0];
        
        // Update start position for next batch
        if (index < start || start === -1) {
          start = index - 1;
        }
        
        // Check if we've gone back too far
        if (txDate < startDate) {
          foundOlderThanStart = true;
        }
        
        // Only include matching operations within our date range
        if (txDate >= startDate && txDate < endDate && tx.op[0] === opType) {
          transactions.push(tx);
          matchCount++;
        }
      }
      
      // Only log if we found new operations or every 10 batches
      totalFound += matchCount;
      if (matchCount > 0 || batchCount % 10 === 0) {
        // Calculate progress as a percentage (approximate)
        let progress = 0;
        if (foundOlderThanStart) {
          progress = 100;
        } else if (start <= 0) {
          progress = 100;
        } else {
          // Rough estimate based on first batch
          const firstBatchIndex = Math.max(...history.map(item => item[0]));
          if (firstBatchIndex > 0) {
            progress = Math.min(100, Math.round((1 - start / firstBatchIndex) * 100));
          }
        }
        
        // Only log if progress changed significantly or we found new operations
        if (matchCount > 0 || progress >= lastProgress + 10 || progress === 100) {
          console.log(`${opType}: ${totalFound} found (${progress}% complete)`);
          lastProgress = progress;
        }
      }
      
      // If we've found operations older than our start date, we're done
      if (foundOlderThanStart || start <= 0) {
        done = true;
      }
    } catch (error) {
      console.error(`Error fetching ${opType} operations:`, error.message);
      throw error;
    }
  }
  
  if (totalFound > 0) {
    console.log(`✓ Found ${totalFound} ${opType} operations`);
  } else {
    console.log(`✓ No ${opType} operations found`);
  }
  
  return transactions;
}

/**
 * Fetch all relevant transaction types in parallel
 * @returns {Promise<Array>} - All transactions
 */
async function fetchAllTransactions() {
  // Fetch all operation types in parallel
  const operationPromises = TARGET_OPERATIONS.map(opType => fetchOperationHistory(HIVE_ACCOUNT, opType));
  const operationResults = await Promise.all(operationPromises);
  
  // Combine all results
  let allTransactions = [];
  TARGET_OPERATIONS.forEach((opType, index) => {
    allTransactions = allTransactions.concat(operationResults[index]);
  });
  
  // Sort by timestamp
  allTransactions.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  
  const total = allTransactions.length;
  console.log(`✓ Total transactions found: ${total}`);
  
  return allTransactions;
}

/**
 * Process fill_order transactions in batch
 * @param {Array} txs - Array of fill_order transactions
 * @param {Object} processed - Object to store processed transactions
 */
function processFillOrderBatch(txs, processed) {
  for (const tx of txs) {
    const operation = tx.op[1];
    const timestamp = formatDate(tx.timestamp);
    
    const currentPays = parseAmount(operation.current_pays);
    const openPays = parseAmount(operation.open_pays);
    
    // Determine if the account is buying or selling
    const isCurrentOwner = operation.current_owner === HIVE_ACCOUNT;
    
    let inn, innCurrency, ut, utCurrency;
    
    if (isCurrentOwner) {
      // Account is selling current_pays and receiving open_pays
      inn = openPays.amount;
      innCurrency = openPays.currency;
      ut = currentPays.amount;
      utCurrency = currentPays.currency;
    } else {
      // Account is selling open_pays and receiving current_pays
      inn = currentPays.amount;
      innCurrency = currentPays.currency;
      ut = openPays.amount;
      utCurrency = openPays.currency;
    }
    
    const result = {
      timestamp,
      type: 'Handel',
      inn,
      innCurrency,
      ut,
      utCurrency,
      fee: '',
      feeCurrency: '',
      market: sanitizeNote('Hive Internal Market'),
      note: ''
    };
    
    if ((innCurrency === 'HIVE' && isDust(inn, 'HIVE')) || 
        (utCurrency === 'HIVE' && isDust(ut, 'HIVE'))) {
      processed.dust.push(result);
    } else {
      processed.regular.push(result);
    }
  }
}

/**
 * Process interest transactions in batch
 * @param {Array} txs - Array of interest transactions
 * @param {Object} processed - Object to store processed transactions
 */
function processInterestBatch(txs, processed) {
  for (const tx of txs) {
    const operation = tx.op[1];
    const timestamp = formatDate(tx.timestamp);
    
    const interest = parseAmount(operation.interest);
    
    const result = {
      timestamp,
      type: 'Inntekt',
      inn: interest.amount,
      innCurrency: interest.currency,
      ut: '',
      utCurrency: '',
      fee: '',
      feeCurrency: '',
      market: sanitizeNote('Hive Blockchain'),
      note: sanitizeNote('HBD Savings Interest')
    };
    
    processed.regular.push(result);
  }
}

/**
 * Process transfer transactions in batch
 * @param {Array} txs - Array of transfer transactions
 * @param {Object} processed - Object to store processed transactions
 */
function processTransferBatch(txs, processed) {
  for (const tx of txs) {
    const operation = tx.op[1];
    const timestamp = formatDate(tx.timestamp);
    
    const amount = parseAmount(operation.amount);
    const isIncoming = operation.to === HIVE_ACCOUNT;
    
    // Create a clean note and truncate it to 200 characters
    let memo = operation.memo || '';
    // Replace any newlines or commas with spaces to prevent CSV issues
    memo = memo.replace(/[\n\r,]/g, ' ').trim();
    const note = `${operation.from} to ${operation.to}${memo ? ': ' + memo : ''}`;
    const truncatedNote = sanitizeNote(note);
    
    let result;
    
    if (isIncoming) {
      // Incoming transfer
      result = {
        timestamp,
        type: 'Inntekt',
        inn: amount.amount,
        innCurrency: amount.currency,
        ut: '',
        utCurrency: '',
        fee: '',
        feeCurrency: '',
        market: sanitizeNote('Hive Blockchain'),
        note: truncatedNote
      };
    } else {
      // Outgoing transfer
      result = {
        timestamp,
        type: 'Overføring',
        inn: '',
        innCurrency: '',
        ut: amount.amount,
        utCurrency: amount.currency,
        fee: '',
        feeCurrency: '',
        market: sanitizeNote('Hive Blockchain'),
        note: truncatedNote
      };
    }
    
    if ((result.innCurrency === 'HIVE' && isDust(result.inn, 'HIVE')) || 
        (result.utCurrency === 'HIVE' && isDust(result.ut, 'HIVE'))) {
      processed.dust.push(result);
    } else {
      processed.regular.push(result);
    }
  }
}

/**
 * Process recurrent transfer transactions in batch
 * @param {Array} txs - Array of recurrent transfer transactions
 * @param {Object} processed - Object to store processed transactions
 */
function processRecurrentTransferBatch(txs, processed) {
  for (const tx of txs) {
    const operation = tx.op[1];
    const timestamp = formatDate(tx.timestamp);
    
    const amount = parseAmount(operation.amount);
    
    // Create a clean note and truncate it to 200 characters
    let memo = operation.memo || '';
    // Replace any newlines or commas with spaces to prevent CSV issues
    memo = memo.replace(/[\n\r,]/g, ' ').trim();
    const note = `${operation.from} to ${operation.to}${memo ? ': ' + memo : ''}`;
    const truncatedNote = sanitizeNote(note);
    
    const result = {
      timestamp,
      type: 'Forbruk',
      inn: '',
      innCurrency: '',
      ut: amount.amount,
      utCurrency: amount.currency,
      fee: '',
      feeCurrency: '',
      market: sanitizeNote('Hive Blockchain'),
      note: truncatedNote
    };
    
    if (result.utCurrency === 'HIVE' && isDust(result.ut, 'HIVE')) {
      processed.dust.push(result);
    } else {
      processed.regular.push(result);
    }
  }
}

/**
 * Process convert request transactions in batch
 * @param {Array} txs - Array of convert request transactions
 * @param {Object} processed - Object to store processed transactions
 */
function processConvertRequestBatch(txs, processed) {
  for (const tx of txs) {
    const operation = tx.op[1];
    const timestamp = formatDate(tx.timestamp);
    
    const amountIn = parseAmount(operation.amount_in);
    const amountOut = parseAmount(operation.amount_out);
    
    const result = {
      timestamp,
      type: 'Handel',
      inn: amountOut.amount,
      innCurrency: amountOut.currency,
      ut: amountIn.amount,
      utCurrency: amountIn.currency,
      fee: '',
      feeCurrency: '',
      market: sanitizeNote('Hive Blockchain Conversion'),
      note: ''
    };
    
    if ((result.innCurrency === 'HIVE' && isDust(result.inn, 'HIVE')) || 
        (result.utCurrency === 'HIVE' && isDust(result.ut, 'HIVE'))) {
      processed.dust.push(result);
    } else {
      processed.regular.push(result);
    }
  }
}

/**
 * Process claim reward transactions in batch
 * @param {Array} txs - Array of claim_reward_balance transactions
 * @param {Object} processed - Object to store processed transactions
 */
async function processClaimRewardBatch(txs, processed) {
  // Group transactions by date to minimize global properties calls
  const byDate = {};
  
  for (const tx of txs) {
    const dateKey = tx.timestamp.split('T')[0];
    if (!byDate[dateKey]) byDate[dateKey] = [];
    byDate[dateKey].push(tx);
  }
  
  console.log(`Processing rewards across ${Object.keys(byDate).length} different dates`);
  
  // Process each date group
  for (const [dateKey, dateTxs] of Object.entries(byDate)) {
    // Get global properties once per date
    const props = await getGlobalPropertiesForDate(dateKey + 'T00:00:00Z');
    
    for (const tx of dateTxs) {
      const operation = tx.op[1];
      const timestamp = formatDate(tx.timestamp);
      
      // Process HBD rewards
      const hbdReward = parseAmount(operation.reward_hbd);
      if (hbdReward.amount > 0) {
        processed.regular.push({
          timestamp,
          type: 'Inntekt',
          inn: hbdReward.amount,
          innCurrency: hbdReward.currency,
          ut: '',
          utCurrency: '',
          fee: '',
          feeCurrency: '',
          market: sanitizeNote('Hive Blockchain'),
          note: sanitizeNote('Curation/Posting Rewards')
        });
      }
      
      // Process VESTS rewards (convert to HP but label as HIVE)
      const vestsReward = parseAmount(operation.reward_vests);
      if (vestsReward.amount > 0) {
        const hpAmount = vestsToHp(vestsReward.amount, props);
        const result = {
          timestamp,
          type: 'Inntekt',
          inn: hpAmount.toFixed(3),
          innCurrency: 'HIVE', // We label it as HIVE even though it's HP
          ut: '',
          utCurrency: '',
          fee: '',
          feeCurrency: '',
          market: sanitizeNote('Hive Blockchain'),
          note: sanitizeNote('Curation/Posting Rewards')
        };
        
        if (isDust(hpAmount, 'HIVE')) {
          processed.dust.push(result);
        } else {
          processed.regular.push(result);
        }
      }
    }
  }
}

/**
 * Process a batch of transactions by type
 * @param {Array} transactions - Array of transactions
 * @returns {Promise<Object>} - Processed regular and dust transactions
 */
async function processTransactionBatch(transactions) {
  const processed = {
    regular: [],
    dust: []
  };
  
  // Group transactions by operation type for more efficient processing
  const byType = {};
  for (const tx of transactions) {
    const opType = tx.op[0];
    if (!byType[opType]) byType[opType] = [];
    byType[opType].push(tx);
  }
  
  // Process each operation type
  for (const [opType, txs] of Object.entries(byType)) {
    if (txs.length > 0) {
      console.log(`Processing ${txs.length} ${opType} operations...`);
      
      switch (opType) {
        case 'claim_reward_balance':
          await processClaimRewardBatch(txs, processed);
          break;
        case 'fill_order':
          processFillOrderBatch(txs, processed);
          break;
        case 'interest':
          processInterestBatch(txs, processed);
          break;
        case 'transfer':
          processTransferBatch(txs, processed);
          break;
        case 'fill_recurrent_transfer':
          processRecurrentTransferBatch(txs, processed);
          break;
        case 'fill_convert_request':
          processConvertRequestBatch(txs, processed);
          break;
      }
    }
  }
  
  return processed;
}

/**
 * Write transactions to CSV file
 * @param {string} filename - Output filename
 * @param {Array} transactions - Transactions to write
 */
function writeTransactionsToCsv(filename, transactions) {
  // Sort transactions by timestamp (oldest to newest)
  transactions.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  
  const csvContent = [
    CSV_HEADER,
    ...transactions.map(formatCsvLine)
  ].join('\n');
  
  fs.writeFileSync(filename, csvContent);
  console.log(`✓ Wrote ${transactions.length} transactions to ${filename}`);
}

/**
 * Main function
 */
async function main() {
  console.log(`=== HIVE TRANSACTION EXPORTER ===`);
  console.log(`Account: ${HIVE_ACCOUNT} | Year: ${YEAR}`);
  
  const startTime = Date.now();
  
  try {
    // Fetch all transactions
    console.log('\n1. FETCHING TRANSACTIONS:');
    const transactions = await fetchAllTransactions();
    
    // Process transactions
    console.log('\n2. PROCESSING TRANSACTIONS:');
    const processed = await processTransactionBatch(transactions);
    
    // Write results to CSV
    console.log('\n3. WRITING OUTPUT FILES:');
    const regularFilename = `${HIVE_ACCOUNT}_hivetxs_${YEAR}.csv`;
    const dustFilename = `${HIVE_ACCOUNT}_hivetxs_${YEAR}_dust.csv`;
    
    writeTransactionsToCsv(regularFilename, processed.regular);
    writeTransactionsToCsv(dustFilename, processed.dust);
    
    const endTime = Date.now();
    const executionTimeSeconds = ((endTime - startTime) / 1000).toFixed(2);
    
    console.log(`\n=== SUMMARY ===`);
    console.log(`✓ Execution time: ${executionTimeSeconds} seconds`);
    console.log(`✓ Regular transactions: ${processed.regular.length}`);
    console.log(`✓ Dust transactions: ${processed.dust.length}`);
    console.log(`✓ Output files created: ${regularFilename}, ${dustFilename}`);
  } catch (error) {
    console.error('\n❌ ERROR:', error.message);
    process.exit(1);
  }
}

// Run the script
main();