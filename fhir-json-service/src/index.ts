// fhir-json-service/src/index.ts
import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'fhir-json-service',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'fhir-json-group' });
const producer = kafka.producer();

const startService = async () => {
  try {
    await consumer.connect();
    await producer.connect();
    console.log('✅ FHIR->JSON Service connected to Kafka');

    // Subscribe to the hospital's response topic
    await consumer.subscribe({ topic: 'fhir.incoming', fromBeginning: true });
    console.log('⚙️ Listening for messages on "fhir.incoming"...');

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) return;

        const incomingData = JSON.parse(message.value.toString());
        console.log(`\n📥 Received Hospital Response:`, incomingData);

        // Map the FHIR-like response back to a clean internal JSON format
        const finalJsonResponse = {
          internalPatientId: incomingData.originalId,
          externalReferenceId: incomingData.hospitalSystemId,
          syncStatus: incomingData.status === "201 Created" ? "SUCCESS" : "FAILED",
          completedAt: incomingData.timestamp
        };

        console.log(`🔄 Transformed to Internal JSON:`, finalJsonResponse);

        // Publish to the final topic
        await producer.send({
          topic: 'json.response',
          messages: [
            { 
              key: message.key, 
              value: JSON.stringify(finalJsonResponse) 
            }
          ]
        });
        console.log('📤 Published final result to topic: json.response');
      },
    });
  } catch (error) {
    console.error('❌ Fatal error:', error);
  }
};

startService();