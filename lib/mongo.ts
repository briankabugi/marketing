import { MongoClient } from 'mongodb';

const uri = process.env.MONGODB_URI;
if (!uri) throw new Error('MONGODB_URI not set');

let client: MongoClient;
let clientPromise: Promise<MongoClient>;

declare global {
  // eslint-disable-next-line no-var
  var _mongoClientPromise: Promise<MongoClient> | undefined;
}

if (!global._mongoClientPromise) {
  client = new MongoClient(uri, {
    maxPoolSize: 10,
    serverSelectionTimeoutMS: 5000,
    socketTimeoutMS: 45000,
    // useUnifiedTopology is default in modern drivers
  });

  // Connect immediately but reuse promise globally
  global._mongoClientPromise = client.connect();

  // Optional: log connection events
  client.on('serverOpening', (event) => console.log('MongoDB server opening', event));
  client.on('serverClosed', (event) => console.warn('MongoDB server closed', event));
  client.on('topologyClosed', () => console.warn('MongoDB topology closed'));
  client.on('topologyOpening', () => console.log('MongoDB topology opening'));
  client.on('error', (err) => console.error('MongoDB error', err));
}

clientPromise = global._mongoClientPromise;

export default clientPromise;
