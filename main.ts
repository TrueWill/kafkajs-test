import {type EachMessagePayload, Kafka} from 'kafkajs';

async function processMessage(payload: EachMessagePayload): Promise<void> {
  console.log(payload.message.value.toString());
};

async function main() {
  const kafka = new Kafka({
    clientId: 'kafkajs-test',
    brokers: ['localhost:9092'],
  });
  
  const consumer = kafka.consumer({ groupId: 'my-group' });

  await consumer.connect();
  await consumer.subscribe({ topic: 'my-topic', fromBeginning: false });

  await consumer.run({
    eachMessage: processMessage,
  });

  // Note - not currently calling consumer.disconnect().
}

main().catch(console.error);
