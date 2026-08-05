// communication-service/src/index.ts
import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'communication-service',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'communication-group' });
const producer = kafka.producer();

const startService = async () => {
  try {
    await consumer.connect();
    await producer.connect();
    console.log('✅ Communication Service connected to Kafka');

    // Subscribe to the outgoing FHIR topic
    await consumer.subscribe({ topic: 'fhir.outgoing', fromBeginning: true });
    console.log('⚙️ Listening for messages on "fhir.outgoing"...');

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) return;

        const fhirData = JSON.parse(message.value.toString());
        console.log(`\n🏥 Received FHIR payload to send to hospital:`, fhirData.id);
        console.log(`⏳ Simulating API call to External Hospital Server...`);

        // Simulate network delay (2 seconds)
        await new Promise(resolve => setTimeout(resolve, 2000));

        // Create a fake successful response from the hospital
        const hospitalResponse = {
          originalId: fhirData.id,
          hospitalSystemId: `HOSP-${Math.floor(Math.random() * 10000)}`,
          status: "201 Created",
          timestamp: new Date().toISOString()
        };

        console.log(`✅ Hospital responded successfully:`, hospitalResponse);

        // Publish the response to the incoming topic
        await producer.send({
          topic: 'fhir.incoming',
          messages: [
            { 
              key: message.key, 
              value: JSON.stringify(hospitalResponse) 
            }
          ]
        });
        console.log('📤 Published hospital response to topic: fhir.incoming');
      },
    });
  } catch (error) {
    console.error('❌ Fatal error:', error);
  }
};

startService();