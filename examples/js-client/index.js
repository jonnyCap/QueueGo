#!/usr/bin/env node
/**
 * Node.js QueueGo / Blink Example: Real-Time Event Publisher & Subscriber
 */

const path = require('path');
const crypto = require('crypto');

// Import Blink JavaScript SDK
const { BlinkClient, hashTopic } = require(path.resolve(__dirname, '../../../Blink/js/blink'));

/**
 * Generates an HS256 JWT using Node.js built-in crypto (zero npm dependencies).
 */
function generateToken(signingKey, sub, queueKey, ...permissions) {
  const header = { alg: 'HS256', typ: 'JWT' };
  const payload = {
    sub,
    queue_key: queueKey,
    permissions,
  };

  const b64url = (str) =>
    Buffer.from(str)
      .toString('base64')
      .replace(/=/g, '')
      .replace(/\+/g, '-')
      .replace(/\//g, '_');

  const headerB64 = b64url(JSON.stringify(header));
  const payloadB64 = b64url(JSON.stringify(payload));
  const signingInput = `${headerB64}.${payloadB64}`;

  const sig = crypto
    .createHmac('sha256', signingKey)
    .update(signingInput)
    .digest('base64')
    .replace(/=/g, '')
    .replace(/\+/g, '-')
    .replace(/\//g, '_');

  return `${headerB64}.${payloadB64}.${sig}`;
}

async function main() {
  const brokerHost = '127.0.0.1';
  const brokerPort = 9000;
  const masterKey = 'my-master-key';
  const queueKey = 'node-demo-secret';
  const topicName = 'nodejs-events';

  console.log('==================================================');
  console.log(' QueueGo Node.js Client (Blink Protocol) Example');
  console.log('==================================================');

  // 1. Generate auth tokens
  const createToken = generateToken(masterKey, 'node-admin', queueKey, 'create');
  const clientToken = generateToken(masterKey, 'node-user', queueKey, 'subscribe', 'publish');

  // 2. Connect to broker
  console.log(`\n[1/4] Connecting to QueueGo broker at ${brokerHost}:${brokerPort}...`);
  const client = new BlinkClient({ host: brokerHost, port: brokerPort });

  try {
    await client.connect();
  } catch (err) {
    console.error(`Error: Failed to connect to broker at ${brokerHost}:${brokerPort}. Is QueueGo running?`);
    console.error('Start it with: go run cmd/queuego/main.go');
    process.exit(1);
  }

  console.log('✓ Connected successfully!');

  // 3. Create topic
  console.log(`\n[2/4] Registering topic "${topicName}" on broker...`);
  const topicID = await client.createTopic(createToken, topicName);
  console.log(`✓ Topic registered! (Hashed ID: ${topicID})`);

  // 4. Subscribe with event listener
  console.log(`\n[3/4] Subscribing to topic "${topicName}"...`);
  let messagesReceived = 0;

  await client.subscribe(clientToken, topicName, (tID, payload) => {
    messagesReceived++;
    try {
      const data = JSON.parse(payload.toString('utf8'));
      console.log(`\n  [Subscriber Received] TopicID: ${tID} | Event: ${data.event} | Msg: ${data.message}`);
    } catch {
      console.log(`\n  [Subscriber Received] Raw: ${payload.toString('utf8')}`);
    }
  });

  console.log('✓ Subscribed and listening for incoming events!');

  // 5. Publish events
  console.log(`\n[4/4] Publishing events to "${topicName}"...`);
  for (let i = 1; i <= 3; i++) {
    const eventPayload = JSON.stringify({
      event: `ORDER_CREATED_${i}`,
      orderId: 1000 + i,
      customer: `user_${i}@example.com`,
      message: `Order #${i} created via Node.js SDK`,
      timestamp: new Date().toISOString(),
    });

    await client.publish(clientToken, topicName, eventPayload);
    console.log(`  -> Published event #${i}`);
    await new Promise((r) => setTimeout(r, 100));
  }

  // Wait briefly for all deliveries
  await new Promise((r) => setTimeout(r, 300));

  console.log(`\n✓ Completed! Total messages received by subscriber: ${messagesReceived}`);
  client.disconnect();
  console.log('✓ Connection closed gracefully.\n');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
