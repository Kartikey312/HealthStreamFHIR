// processing-service/src/index.ts
import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'processing-service',
  brokers: ['localhost:9092']
});

// This service only needs a consumer, no producer!
const consumer = kafka.consumer({ groupId: 'processing-group' });

const startService = async () => {
  try {
    await consumer.connect();
    console.log('✅ Processing Service connected to Kafka');

    // Subscribe to the final JSON response topic
    await consumer.subscribe({ topic: 'json.response', fromBeginning: true });
    console.log('⚙️ Listening for messages on "json.response"...');

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) return;

        const finalData = JSON.parse(message.value.toString());
        
        console.log(`\n======================================================`);
        console.log(`🎉 END-TO-END FLOW COMPLETE!`);
        console.log(`======================================================`);
        console.log(`Updating Internal Database for Patient: ${finalData.internalPatientId}`);
        console.log(`Hospital External ID: ${finalData.externalReferenceId}`);
        console.log(`Status: ${finalData.syncStatus}`);
        console.log(`Timestamp: ${finalData.completedAt}`);
        console.log(`======================================================\n`);
      },
    });
  } catch (error) {
    console.error('❌ Fatal error:', error);
  }
};

startService();