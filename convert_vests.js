const { Client } = require('@hiveio/dhive');

// Initialize a client to connect to a Hive API node
const client = new Client('https://api.hive.blog');

/**
 * Convert VESTS to Hive Power (HP)
 * @param {number} vests - The amount of VESTS to convert
 * @returns {Promise<number>} - The equivalent amount in HP
 */
async function vestsToHp(vests) {
  try {
    // Fetch the current global properties from the blockchain
    const props = await client.database.getDynamicGlobalProperties();
    
    // Extract total VESTS and total HIVE in the vesting pool
    const totalVests = parseFloat(props.total_vesting_shares.split(' ')[0]);
    const totalHive = parseFloat(props.total_vesting_fund_hive.split(' ')[0]);
    
    // Calculate the current conversion rate
    const hivePerVest = totalHive / totalVests;
    
    // Apply the conversion rate to the input VESTS amount
    const hp = vests * hivePerVest;
    
    return hp;
  } catch (error) {
    console.error('Error converting VESTS to HP:', error);
    throw error;
  }
}

// Our test VESTS amount
const testVests = 55073.948644;

// Run the conversion and display the result
vestsToHp(testVests)
  .then(hp => {
    console.log(`Conversion Results:`);
    console.log(`${testVests.toLocaleString()} VESTS = ${hp.toFixed(3)} HP`);
    
    // Also display the current conversion rate
    console.log(`Current conversion rate: 1 VESTS = ${(hp / testVests).toFixed(6)} HP`);
  })
  .catch(err => {
    console.error('Conversion failed:', err);
  });