// json-fhir-service/src/index.ts
import { Kafka } from 'kafkajs';

// 1. Initialize Kafka Client
const kafka = new Kafka({
  clientId: 'json-fhir-service',
  brokers: ['localhost:9092']
});

// 2. Create Consumer and Producer
// We need a Consumer to read from json.request and a Producer to write to fhir.outgoing
const consumer = kafka.consumer({ groupId: 'json-fhir-group' });
const producer = kafka.producer();

const startService = async () => {
  try {
    // Connect both
    await consumer.connect();
    await producer.connect();
    console.log('✅ Connected to Kafka');

    // Subscribe to the incoming topic
    await consumer.subscribe({ topic: 'json.request', fromBeginning: true });
    console.log('⚙️ Listening for messages on "json.request"...');

    // Process messages as they arrive
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) return;

        const rawData = message.value.toString();
        console.log(`\n📥 Received raw JSON: ${rawData}`);

        try {
          const parsedData = JSON.parse(rawData);

          // 3. Transform JSON to Fake FHIR format
          const fhirPayload = {
            resourceType: "Patient",
            id: parsedData.patientId || "unknown",
            name: [
              {
                use: "official",
                text: parsedData.name || "Unknown Patient"
              }
            ],
            active: parsedData.status === "new_admission"
          };

          console.log(`🔄 Transformed to FHIR:`, fhirPayload);

          // 4. Publish to the next topic (fhir.outgoing)
          await producer.send({
            topic: 'fhir.outgoing',
            messages: [
              { 
                key: message.key, // Keep the same trace key
                value: JSON.stringify(fhirPayload) 
              }
            ]
          });
          console.log('📤 Published to topic: fhir.outgoing');

        } catch (error) {
          console.error('❌ Failed to process message:', error);
        }
      },
    });
  } catch (error) {
    console.error('❌ Fatal error:', error);
  }
};

startService();