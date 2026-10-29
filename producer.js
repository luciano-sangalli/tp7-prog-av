import { Kafka } from 'kafkajs';
import crypto from 'crypto';

const kafka = new Kafka({
  clientId: 'rutasur-producer',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

async function emitirEvento(tipo, trackingId, datosExtra = {}) {
  await producer.connect();

  const evento = {
    eventId: crypto.randomUUID(),
    eventType: tipo,
    timestamp: new Date().toISOString(),
    data: {
      trackingId,
      ...datosExtra
    }
  };

  await producer.send({
    topic: 'envios-eventos',
    messages: [
      {
        key: trackingId,
        value: JSON.stringify(evento)
      }
    ]
  });

  console.log(`📤 [Kafka] Evento publicado: ${tipo} para paquete ${trackingId}`);
  await producer.disconnect();
}

async function simularEnvioCritico() {
  const trackingId = 'RS-CRITICO-999';

  console.log('Emitiendo envío con SLA comprometido...');
  await emitirEvento('EnvioCreado', trackingId, {
    origen: 'Centro Distribución Sur',
    destino: 'Av. Corrientes 1400',
    destinatario: 'Lucía Fernández',
    estado: 'EN_DISTRIBUCION',
    // Vence en 20 minutos a partir de ahora
    slaLimite: new Date(Date.now() + 20 * 60 * 1000).toISOString()
  });
}

simularEnvioCritico().catch(console.error);