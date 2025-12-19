import { MongoClient } from 'mongodb';

const uri = process.env.MONGODB_URI!;
if (!uri) throw new Error('MONGODB_URI not set');

let client: MongoClient;
let clientPromise: Promise<MongoClient>;

declare global {
  var _mongoClientPromise: Promise<MongoClient>;
}

if (global._mongoClientPromise) {
  clientPromise = global._mongoClientPromise;
} else {
  client = new MongoClient(uri);
  clientPromise = client.connect();
  global._mongoClientPromise = clientPromise;
}

export default clientPromise;
