import express from 'express';
import cors from 'cors';
import mongoose from 'mongoose';
import { Kafka } from 'kafkajs';
import crypto from 'crypto';

const app = express();
app.use(cors());
app.use(express.json());

// Conexión a MongoDB
await mongoose.connect('mongodb://localhost:27017/rutasur');
console.log('✅ [API REST] Conectada a MongoDB');

// Kafka Producer para simular eventos desde la UI
const kafka = new Kafka({ clientId: 'rutasur-api-producer', brokers: ['localhost:9092'] });
const producer = kafka.producer();
await producer.connect();

// Modelos existentes
const Envio = mongoose.models.Envio || mongoose.model('Envio', new mongoose.Schema({
  trackingId: String,
  estado: String,
  origen: String,
  destino: String,
  destinatario: String,
  slaLimite: Date,
  historialEventos: Array,
  ultimaActualizacion: Date
}));

const Alerta = mongoose.models.Alerta || mongoose.model('Alerta', new mongoose.Schema({
  alertaId: String,
  trackingId: String,
  tipoAlerta: String,
  tiempoRestanteMinutos: Number,
  generadaEn: Date,
  resuelta: Boolean
}));

// Endpoints de lectura
app.get('/api/envios', async (req, res) => {
  const envios = await Envio.find().sort({ ultimaActualizacion: -1 });
  res.json(envios);
});

app.get('/api/alertas', async (req, res) => {
  const alertas = await Alerta.find({ resuelta: false }).sort({ generadaEn: -1 });
  res.json(alertas);
});

// Endpoint para disparar eventos a Kafka desde los botones de la UI
app.post('/api/eventos', async (req, res) => {
  const { eventType, trackingId, data } = req.body;
  const evento = {
    eventId: crypto.randomUUID(),
    eventType,
    timestamp: new Date().toISOString(),
    data: { trackingId, ...data }
  };

  await producer.send({
    topic: 'envios-eventos',
    messages: [{ key: trackingId, value: JSON.stringify(evento) }]
  });

  res.json({ ok: true, eventId: evento.eventId });
});

app.listen(4000, () => {
  console.log('🚀 Servidor API corriendo en http://localhost:4000');
});