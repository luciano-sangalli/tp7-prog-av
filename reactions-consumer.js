import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'rutasur-reacciones-fallidas',
  brokers: ['localhost:9092']
});

// 1. Reacción 1: Servicio de Notificaciones al Destinatario
async function startNotificationService() {
  const consumer = kafka.consumer({ groupId: 'grupo-notificaciones-cliente' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'envios-eventos', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evento = JSON.parse(message.value.toString());
      if (evento.eventType === 'EntregaFallida') {
        const { trackingId, motivo, destinatario } = evento.data;
        console.log(`\n📨 [NOTIFICACIONES] Enviando SMS/Email a ${destinatario || 'cliente'}: "No pudimos entregar tu paquete ${trackingId}. Motivo: ${motivo}. Reprogramá la entrega en el link."`);
      }
    }
  });
}

// 2. Reacción 2: Servicio de Reasignación Logística / Devolución
async function startReroutingService() {
  const consumer = kafka.consumer({ groupId: 'grupo-reasignacion-rutas' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'envios-eventos', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evento = JSON.parse(message.value.toString());
      if (evento.eventType === 'EntregaFallida') {
        const { trackingId, intentoNumero = 1 } = evento.data;
        if (intentoNumero < 3) {
          console.log(`🔄 [LOGÍSTICA/RUTAS] Reencolando envío ${trackingId} para reintento automático al depósito central (Intento ${intentoNumero} de 3).`);
        } else {
          console.log(`📦 [LOGÍSTICA/RUTAS] Envío ${trackingId} superó los 3 intentos. Marcado para retorno definitivo a origen.`);
        }
      }
    }
  });
}

// 3. Reacción 3: Servicio de Auditoría y Penalización de SLA
async function startAuditService() {
  const consumer = kafka.consumer({ groupId: 'grupo-auditoria-sla' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'envios-eventos', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evento = JSON.parse(message.value.toString());
      if (evento.eventType === 'EntregaFallida') {
        const { trackingId, motivo, conductorId = 'CH-404' } = evento.data;
        console.log(`⚖️  [AUDITORÍA/SLA] Registrando incidente operativo sobre envío ${trackingId}. Chofer: ${conductorId} | Causa: "${motivo}". Impacto en métrica de efectividad registrado.`);
      }
    }
  });
}

// Iniciar los tres consumidores en paralelo
console.log('🚀 Inicializando las 3 reacciones desacopladas...');
await Promise.all([
  startNotificationService(),
  startReroutingService(),
  startAuditService()
]);
console.log('✅ Las 3 reacciones están suscritas y escuchando eventos en paralelo.');