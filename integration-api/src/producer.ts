import { Kafka } from 'kafkajs';

// Initialize Kafka client pointing to local broker
const kafka = new Kafka({
  clientId: 'integration-api',
  brokers: ['localhost:9092'],
});

export const producer = kafka.producer();

export const connectProducer = async () => {
  try {
    await producer.connect();
    console.log('✅ Kafka Producer connected successfully');
  } catch (error) {
    console.error('❌ Failed to connect Kafka Producer:', error);
    process.exit(1);
  }
};