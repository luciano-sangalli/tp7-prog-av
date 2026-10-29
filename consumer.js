import { Kafka } from 'kafkajs';
import mongoose from 'mongoose';

// 1. Conexión a MongoDB
await mongoose.connect('mongodb://localhost:27017/rutasur');
console.log(' Conectado a MongoDB');

// 2. Esquema de lectura para Tracking
const EnvioSchema = new mongoose.Schema({
  trackingId: { type: String, unique: true, required: true },
  estado: String,
  origen: String,
  destino: String,
  destinatario: String,
  slaLimite: Date,
  historialEventos: [
    {
      eventId: String,
      eventType: String,
      timestamp: Date
    }
  ],
  ultimaActualizacion: Date
});

const Envio = mongoose.model('Envio', EnvioSchema);

// 3. Conexión y consumidor de Kafka
const kafka = new Kafka({
  clientId: 'rutasur-consumer-tracking',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'grupo-proyeccion-tracking' });

async function run() {
  await consumer.connect();
  console.log(' Consumidor de Kafka listo y escuchando...');

  await consumer.subscribe({ topic: 'envios-eventos', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const payload = JSON.parse(message.value.toString());
      const { eventId, eventType, timestamp, data } = payload;

      console.log(`\n [Kafka] Evento recibido: ${eventType} (ID: ${data.trackingId})`);

      // Upsert: si no existe lo crea, si existe actualiza el estado y agrega al historial
      const actualizado = await Envio.findOneAndUpdate(
        { trackingId: data.trackingId },
        {
          $set: {
            estado: data.estado || eventType,
            ultimaActualizacion: new Date(timestamp),
            ...(data.origen && { origen: data.origen }),
            ...(data.destino && { destino: data.destino }),
            ...(data.destinatario && { destinatario: data.destinatario }),
            ...(data.slaLimite && { slaLimite: new Date(data.slaLimite) })
          },
          $push: {
            historialEventos: {
              eventId,
              eventType,
              timestamp: new Date(timestamp)
            }
          }
        },
        { upsert: true, new: true }
      );

      console.log(` [MongoDB] Documento guardado para tracking: ${actualizado.trackingId} con estado: ${actualizado.estado}`);
    }
  });
}

run().catch(console.error);