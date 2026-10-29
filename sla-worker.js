import { Kafka } from 'kafkajs';
import crypto from 'crypto';

const kafka = new Kafka({
  clientId: 'rutasur-sla-service',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'grupo-sla-detector' });

async function run() {
  await producer.connect();
  await consumer.connect();

  // Escucha directamente los eventos de envíos
  await consumer.subscribe({ topic: 'envios-eventos', fromBeginning: false });
  console.log('👀 [SLA Worker] Escuchando envíos para evaluar SLA en tiempo real...');

  await consumer.run({
    eachMessage: async ({ message }) => {
      const payload = JSON.parse(message.value.toString());
      const { eventType, data } = payload;

      if (!data?.slaLimite) return;

      const ahora = Date.now();
      const limite = new Date(data.slaLimite).getTime();
      const diffMinutos = Math.round((limite - ahora) / (1000 * 60));

      console.log(`⏱️  [Evaluación] Envío ${data.trackingId} - Tiempo restante: ${diffMinutos} min`);

      // Si le queda menos de 60 minutos, disparamos alerta temprana
      if (diffMinutos <= 60) {
        const eventoAlerta = {
          eventId: crypto.randomUUID(),
          eventType: 'RiesgoSlaDetectado',
          timestamp: new Date().toISOString(),
          data: {
            trackingId: data.trackingId,
            tiempoRestanteMinutos: diffMinutos,
            estadoActual: data.estado || eventType,
            slaLimite: data.slaLimite
          }
        };

        await producer.send({
          topic: 'alertas-sla',
          messages: [{ key: data.trackingId, value: JSON.stringify(eventoAlerta) }]
        });

        console.log(`🚨 [ALERTA DISPARADA] Riesgo de SLA en paquete ${data.trackingId} (Quedan ${diffMinutos} min)`);
      }
    }
  });
}

run().catch(console.error);