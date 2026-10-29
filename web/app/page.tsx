'use client';
import { useState, useEffect } from 'react';

export default function Dashboard() {
  const [envios, setEnvios] = useState<any[]>([]);
  const [alertas, setAlertas] = useState<any[]>([]);
  const [cargando, setCargando] = useState(true);

  async function cargarDatos() {
    try {
      const [resEnvios, resAlertas] = await Promise.all([
        fetch('http://localhost:4000/api/envios'),
        fetch('http://localhost:4000/api/alertas')
      ]);
      const dataEnvios = await resEnvios.json();
      const dataAlertas = await resAlertas.json();

      if (Array.isArray(dataEnvios)) setEnvios(dataEnvios);
      if (Array.isArray(dataAlertas)) setAlertas(dataAlertas);
    } catch (e) {
      console.error("Error al obtener datos:", e);
    } finally {
      setCargando(false);
    }
  }

  useEffect(() => {
    cargarDatos();
    const interval = setInterval(cargarDatos, 2000);
    return () => clearInterval(interval);
  }, []);

  async function dispararEvento(eventType: string, trackingId: string, extraData: any = {}) {
    await fetch('http://localhost:4000/api/eventos', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ eventType, trackingId, data: extraData })
    });
  }

  return (
    <div className="min-h-screen bg-slate-900 text-slate-100 p-8 font-sans">
      <header className="mb-8 border-b border-slate-700 pb-4 flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-cyan-400">RutaSur Logistics</h1>
          <p className="text-slate-400 text-sm">Monitoreo de Envíos y Eventos en Tiempo Real</p>
        </div>
        <div className="flex items-center gap-3">
          <span className="inline-block w-3 h-3 rounded-full bg-emerald-500 animate-pulse"></span>
          <span className="text-xs text-emerald-400 font-semibold tracking-wide uppercase">En vivo</span>
        </div>
      </header>

      {/* BOTONERA DE SIMULACIÓN INTERACTIVA */}
      <section className="mb-8 bg-slate-800/40 border border-slate-700/80 rounded-xl p-4">
        <h3 className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-3">Simulación de Eventos en Vivo (Kafka):</h3>
        <div className="flex flex-wrap gap-2">
          <button
            onClick={() => dispararEvento('EnvioCreado', `RS-${Math.floor(1000 + Math.random() * 9000)}`, {
              destinatario: 'Cliente Nuevo',
              destino: 'Av. Libertador 2200',
              estado: 'CREADO'
            })}
            className="px-3 py-1.5 bg-cyan-600 hover:bg-cyan-500 text-white text-xs font-semibold rounded-lg transition"
          >
            + Crear Nuevo Envío
          </button>

          <button
            onClick={() => dispararEvento('EntregaFallida', 'RS-2026-001', {
              estado: 'ENTREGA_FALLIDA',
              motivo: 'Dirección inaccesible',
              intentoNumero: 2
            })}
            className="px-3 py-1.5 bg-amber-600 hover:bg-amber-500 text-white text-xs font-semibold rounded-lg transition"
          >
            ⚠️ Simular Fallo (RS-2026-001)
          </button>

          <button
            onClick={() => dispararEvento('EnvioCreado', `RS-URGENTE-${Math.floor(100 + Math.random() * 900)}`, {
              destinatario: 'Entrega Prioritaria',
              destino: 'Centro Logístico Sur',
              estado: 'EN_DISTRIBUCION',
              slaLimite: new Date(Date.now() + 15 * 60 * 1000).toISOString()
            })}
            className="px-3 py-1.5 bg-rose-600 hover:bg-rose-500 text-white text-xs font-semibold rounded-lg transition"
          >
            🚨 Disparar Envío con Riesgo SLA
          </button>
        </div>
      </section>

      {/* ALERTAS ACTIVAS */}
      {alertas.length > 0 && (
        <section className="mb-8">
          <h2 className="text-lg font-semibold text-rose-400 mb-3 flex items-center gap-2">
            ⚠️ Alertas Activas de Riesgo de SLA ({alertas.length})
          </h2>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {alertas.map((alerta) => (
              <div key={alerta.alertaId || alerta._id} className="bg-rose-950/40 border border-rose-500/50 rounded-xl p-4 flex flex-col justify-between">
                <div>
                  <div className="flex justify-between items-start mb-2">
                    <span className="font-mono font-bold text-rose-200">{alerta.trackingId}</span>
                    <span className="bg-rose-500/20 text-rose-300 text-xs px-2 py-0.5 rounded font-semibold">RIESGO SLA</span>
                  </div>
                  <p className="text-sm text-rose-100/80">
                    Tiempo restante: <strong className="text-white">{alerta.tiempoRestanteMinutos} min</strong>
                  </p>
                </div>
                <span className="text-xs text-slate-400 mt-3 block">
                  Generada: {new Date(alerta.generadaEn).toLocaleTimeString()}
                </span>
              </div>
            ))}
          </div>
        </section>
      )}

      {/* TABLA DE TRACKING */}
      <section className="bg-slate-800/60 border border-slate-700 rounded-xl p-6 shadow-xl">
        <h2 className="text-lg font-semibold text-slate-200 mb-4">Seguimiento de Paquetes</h2>
        {cargando ? (
          <p className="text-slate-400 text-sm">Cargando envíos...</p>
        ) : envios.length === 0 ? (
          <p className="text-slate-500 text-sm">No hay envíos registrados.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left border-collapse text-sm">
              <thead>
                <tr className="border-b border-slate-700 text-slate-400 text-xs uppercase tracking-wider">
                  <th className="py-3 px-4">Tracking ID</th>
                  <th className="py-3 px-4">Estado</th>
                  <th className="py-3 px-4">Destinatario</th>
                  <th className="py-3 px-4">Destino</th>
                  <th className="py-3 px-4">Eventos Acumulados</th>
                  <th className="py-3 px-4">Última Actualización</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-700/60">
                {envios.map((e) => (
                  <tr key={e.trackingId} className="hover:bg-slate-700/30 transition-colors">
                    <td className="py-3 px-4 font-mono font-medium text-cyan-300">{e.trackingId}</td>
                    <td className="py-3 px-4">
                      <span className={`px-2.5 py-1 rounded text-xs font-semibold ${
                        e.estado === 'ENTREGA_FALLIDA' ? 'bg-amber-500/20 text-amber-300 border border-amber-500/30' :
                        e.estado === 'EN_DISTRIBUCION' ? 'bg-blue-500/20 text-blue-300 border border-blue-500/30' :
                        e.estado === 'CREADO' ? 'bg-slate-600/30 text-slate-300' : 'bg-emerald-500/20 text-emerald-300'
                      }`}>
                        {e.estado}
                      </span>
                    </td>
                    <td className="py-3 px-4 text-slate-300">{e.destinatario || '-'}</td>
                    <td className="py-3 px-4 text-slate-400">{e.destino || '-'}</td>
                    <td className="py-3 px-4 text-slate-300 font-mono">
                      {e.historialEventos ? e.historialEventos.length : 1}
                    </td>
                    <td className="py-3 px-4 text-slate-400 text-xs">
                      {e.ultimaActualizacion ? new Date(e.ultimaActualizacion).toLocaleTimeString() : '-'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}