// lib/mongo.ts
import { MongoClient } from 'mongodb';

const uri = process.env.MONGODB_URI;
if (!uri) throw new Error('MONGODB_URI not set');

let client: MongoClient;
let clientPromise: Promise<MongoClient>;

declare global {
  // eslint-disable-next-line no-var
  var _mongoClientPromise: Promise<MongoClient> | undefined;
}

// Retry logic in case MongoDB connection fails
async function connectWithRetry(): Promise<MongoClient> {
  const MAX_RETRIES = 5;
  let attempts = 0;
  let lastError: Error | null = null;

  while (attempts < MAX_RETRIES) {
    try {
      const client = new MongoClient(uri, {
        maxPoolSize: 10,
        serverSelectionTimeoutMS: 10000,
        socketTimeoutMS: 45000,
      });
      await client.connect();
      return client;
    } catch (err: any) {
      lastError = err;
      attempts++;
      console.error(`MongoDB connection failed (attempt ${attempts}/${MAX_RETRIES}):`, err);
      if (attempts < MAX_RETRIES) {
        await new Promise(resolve => setTimeout(resolve, 5000)); // wait for 5 seconds before retrying
      }
    }
  }

  // After max retries, throw the last error encountered
  throw lastError || new Error('MongoDB connection failed after maximum retry attempts');
}

if (!global._mongoClientPromise) {
  global._mongoClientPromise = connectWithRetry();
  
  // Optional: log connection events
  global._mongoClientPromise
    .then(client => {
      client.on('serverOpening', (event) => console.log('MongoDB server opening', event));
      client.on('serverClosed', (event) => console.warn('MongoDB server closed', event));
      client.on('topologyClosed', () => console.warn('MongoDB topology closed'));
      client.on('topologyOpening', () => console.log('MongoDB topology opening'));
      client.on('error', (err) => console.error('MongoDB error', err));
    })
    .catch(err => {
      console.error('MongoDB client failed to connect after retries', err);
    });
}

clientPromise = global._mongoClientPromise;

export default clientPromise;