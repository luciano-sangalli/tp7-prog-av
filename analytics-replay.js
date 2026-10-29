import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'rutasur-analytics-engine',
  brokers: ['localhost:9092']
});

// Usamos un groupId nuevo y único para forzar la lectura desde el offset 0
const consumer = kafka.consumer({
  groupId: `grupo-analitica-replay-${Date.now()}`
});

const metricas = {
  totalEventos: 0,
  eventosPorTipo: {},
  enviosRegistrados: new Set(),
  fallasOperativas: 0,
  alertasSlaGeneradas: 0,
  inicioHistorial: null,
  finHistorial: null
};

async function ejecutarReprocesamiento() {
  console.log(' Iniciando Reprocesamiento Histórico de Eventos (Event Replay)...');
  await consumer.connect();

  // Suscribirse desde el inicio a todos los tópicos
  await consumer.subscribe({ topic: 'envios-eventos', fromBeginning: true });
  await consumer.subscribe({ topic: 'alertas-sla', fromBeginning: true });

  console.log(' Leyendo todo el log inmutable de Kafka desde el inicio...\n');

  let temporizadorFin;

  await consumer.run({
    eachMessage: async ({ message }) => {
      const payload = JSON.parse(message.value.toString());
      const { eventType, timestamp, data } = payload;

      metricas.totalEventos++;
      metricas.eventosPorTipo[eventType] = (metricas.eventosPorTipo[eventType] || 0) + 1;

      if (data?.trackingId) {
        metricas.enviosRegistrados.add(data.trackingId);
      }

      if (eventType === 'EntregaFallida') {
        metricas.fallasOperativas++;
      }

      if (eventType === 'RiesgoSlaDetectado') {
        metricas.alertasSlaGeneradas++;
      }

      if (!metricas.inicioHistorial) metricas.inicioHistorial = timestamp;
      metricas.finHistorial = timestamp;

      // Al no recibir eventos por 2.5 segundos, finaliza y muestra reporte
      clearTimeout(temporizadorFin);
      temporizadorFin = setTimeout(async () => {
        imprimirReporte();
        await consumer.disconnect();
        process.exit(0);
      }, 2500);
    }
  });
}

function imprimirReporte() {
  console.log('====================================================');
  console.log('       INFORME ANALÍTICO DE OPERACIONES (KAFKA)     ');
  console.log('====================================================');
  console.log(`• Total de eventos reprocesados: ${metricas.totalEventos}`);
  console.log(`• Paquetes únicos registrados:   ${metricas.enviosRegistrados.size}`);
  console.log(`• Entregas fallidas totales:     ${metricas.fallasOperativas}`);
  console.log(`• Alertas de SLA emitidas:       ${metricas.alertasSlaGeneradas}`);
  console.log('----------------------------------------------------');
  console.log('Desglose por tipo de evento:');
  console.table(metricas.eventosPorTipo);
  console.log('----------------------------------------------------');
  const tasaFalla = metricas.enviosRegistrados.size > 0
    ? ((metricas.fallasOperativas / metricas.enviosRegistrados.size) * 100).toFixed(1)
    : 0;
  console.log(` Tasa de incidencia de fallas: ${tasaFalla}%`);
  console.log(` Rango temporal: ${metricas.inicioHistorial} -> ${metricas.finHistorial}`);
  console.log('====================================================\n');
  console.log(' Reprocesamiento completado con éxito.');
}

ejecutarReprocesamiento().catch(console.error);