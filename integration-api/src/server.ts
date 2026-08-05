import express, { Request, Response } from 'express';
import { producer, connectProducer } from './producer';

const app = express();
app.use(express.json());

const PORT = 3000;
const TOPIC = 'json.request';

app.post('/patient', async (req: Request, res: Response): Promise<void> => {
  try {
    const patientData = req.body;

    // Validate simple payload existence
    if (!patientData || Object.keys(patientData).length === 0) {
      res.status(400).json({ error: 'Request body cannot be empty' });
      return;
    }

    // Publish event to Kafka
    await producer.send({
      topic: TOPIC,
      messages: [
        {
          // Use patient ID as message key if present, otherwise fallback to timestamp
          key: patientData.id ? String(patientData.id) : String(Date.now()),
          value: JSON.stringify(patientData),
        },
      ],
    });

    console.log(`📤 [${TOPIC}] Published patient event:`, patientData);

    res.status(202).json({
      status: 'ACCEPTED',
      message: 'Patient data published to Kafka successfully',
      data: patientData,
    });
  } catch (error) {
    console.error('❌ Error publishing to Kafka:', error);
    res.status(500).json({ error: 'Failed to publish message to Kafka' });
  }
});

const startServer = async () => {
  await connectProducer();
  app.listen(PORT, () => {
    console.log(`🚀 Integration API listening on http://localhost:${PORT}`);
  });
};

startServer();