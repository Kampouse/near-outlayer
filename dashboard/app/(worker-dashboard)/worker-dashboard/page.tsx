'use client';

import { useEffect, useState, useRef } from 'react';
import Link from 'next/link';
import { fetchStatus, fetchHistory, DaemonStatus, ExecutionRecord, streamUrl } from '@/lib/worker-api';

function formatUptime(secs: number): string {
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = secs % 60;
  return `${h}h ${m}m ${s}s`;
}

export default function WorkerDashboard() {
  const [status, setStatus] = useState<DaemonStatus | null>(null);
  const [history, setHistory] = useState<ExecutionRecord[]>([]);
  const [logs, setLogs] = useState<string[]>([]);
  const [error, setError] = useState('');
  const logRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    fetchStatus().then(setStatus).catch(e => setError(e.message));
    fetchHistory().then(setHistory).catch(() => {});

    const interval = setInterval(() => {
      fetchStatus().then(setStatus).catch(() => {});
      fetchHistory().then(setHistory).catch(() => {});
    }, 5000);

    // SSE for live logs
    const es = new EventSource(streamUrl());
    es.onmessage = (e) => {
      setLogs(prev => [...prev.slice(-99), e.data]);
    };
    es.onerror = () => {};

    return () => { clearInterval(interval); es.close(); };
  }, []);

  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
  }, [logs]);

  const successCount = history.filter(h => h.success).length;
  const successRate = history.length > 0 ? ((successCount / history.length) * 100).toFixed(1) : '—';

  const navLinks = [
    { href: '/worker-dashboard', label: 'Overview', active: true },
    { href: '/worker-dashboard/executions', label: 'Executions' },
    { href: '/worker-dashboard/storage', label: 'Storage' },
    { href: '/worker-dashboard/contract', label: 'Contract' },
    { href: '/worker-dashboard/logs', label: 'Live Log' },
  ];

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100">
      {/* Header */}
      <div className="border-b border-gray-800 bg-gray-900">
        <div className="max-w-7xl mx-auto px-4 py-4 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Link href="/" className="text-sm text-gray-400 hover:text-white">← Back</Link>
            <h1 className="text-xl font-bold text-green-400 font-mono">layerd</h1>
            <span className="flex items-center gap-1.5 text-sm">
              <span className={`w-2 h-2 rounded-full ${status?.running ? 'bg-green-400 animate-pulse' : 'bg-red-500'}`} />
              <span className={status?.running ? 'text-green-400' : 'text-red-400'}>
                {status?.running ? 'Running' : 'Offline'}
              </span>
            </span>
          </div>
          <nav className="flex gap-1">
            {navLinks.map(l => (
              <Link
                key={l.href}
                href={l.href}
                className={`px-3 py-1.5 rounded text-sm font-mono ${
                  l.active ? 'bg-gray-800 text-green-400' : 'text-gray-400 hover:text-white hover:bg-gray-800'
                }`}
              >
                {l.label}
              </Link>
            ))}
          </nav>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 py-6 space-y-6">
        {error && (
          <div className="bg-red-900/30 border border-red-700 rounded-lg p-4 text-red-300 font-mono text-sm">
            Connection failed: {error}. Is layerd running with --dashboard?
          </div>
        )}

        {/* Status Cards */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
            <div className="text-xs text-gray-500 font-mono mb-1">UPTIME</div>
            <div className="text-2xl font-bold text-white font-mono">
              {status ? formatUptime(status.uptime_secs) : '—'}
            </div>
          </div>
          <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
            <div className="text-xs text-gray-500 font-mono mb-1">POLL COUNT</div>
            <div className="text-2xl font-bold text-white font-mono">{status?.poll_count ?? '—'}</div>
          </div>
          <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
            <div className="text-xs text-gray-500 font-mono mb-1">EXECUTIONS</div>
            <div className="text-2xl font-bold text-white font-mono">{history.length}</div>
          </div>
          <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
            <div className="text-xs text-gray-500 font-mono mb-1">SUCCESS RATE</div>
            <div className={`text-2xl font-bold font-mono ${
              successRate === '—' ? 'text-gray-500' : Number(successRate) >= 80 ? 'text-green-400' : 'text-red-400'
            }`}>
              {successRate}%
            </div>
          </div>
        </div>

        {/* Config info */}
        {status && (
          <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
            <div className="text-xs text-gray-500 font-mono mb-2">CONFIG</div>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-sm font-mono">
              <div><span className="text-gray-500">Contract:</span> <span className="text-yellow-400">{status.contract_id}</span></div>
              <div><span className="text-gray-500">Account:</span> <span className="text-blue-400">{status.account_id}</span></div>
              <div><span className="text-gray-500">RPC:</span> <span className="text-gray-300">{status.rpc_url}</span></div>
              <div><span className="text-gray-500">Poll:</span> <span className="text-gray-300">{status.poll_interval_secs}s</span></div>
            </div>
          </div>
        )}

        {/* Live log */}
        <div className="bg-gray-900 border border-gray-800 rounded-lg">
          <div className="px-4 py-3 border-b border-gray-800">
            <h2 className="text-sm font-bold font-mono text-gray-300">LIVE LOG</h2>
          </div>
          <div ref={logRef} className="p-4 max-h-48 overflow-y-auto font-mono text-xs text-gray-400 space-y-0.5">
            {logs.length === 0 ? (
              <div className="text-gray-600">Waiting for events...</div>
            ) : logs.map((l, i) => (
              <div key={i} className={
                l.includes('❌') ? 'text-red-400' :
                l.includes('✅') ? 'text-green-400' :
                l.includes('🏃') ? 'text-yellow-400' : 'text-gray-400'
              }>
                <span className="text-gray-600">›</span> {l}
              </div>
            ))}
          </div>
        </div>

        {/* Recent executions */}
        <div className="bg-gray-900 border border-gray-800 rounded-lg">
          <div className="px-4 py-3 border-b border-gray-800 flex justify-between items-center">
            <h2 className="text-sm font-bold font-mono text-gray-300">RECENT EXECUTIONS</h2>
            <Link href="/worker-dashboard/executions" className="text-xs text-green-400 hover:underline font-mono">View all →</Link>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm font-mono">
              <thead>
                <tr className="text-gray-500 text-xs">
                  <th className="text-left px-4 py-2">REQ ID</th>
                  <th className="text-left px-4 py-2">STATUS</th>
                  <th className="text-left px-4 py-2">TIME</th>
                  <th className="text-left px-4 py-2">INSTR</th>
                  <th className="text-left px-4 py-2">WHEN</th>
                  <th className="text-left px-4 py-2">INPUT</th>
                </tr>
              </thead>
              <tbody>
                {history.slice(-10).reverse().map((r, i) => (
                  <tr key={i} className="border-t border-gray-800/50 hover:bg-gray-800/30">
                    <td className="px-4 py-2">
                      <Link href={`/worker-dashboard/executions/${r.request_id}`} className="text-blue-400 hover:underline">
                        #{r.request_id}
                      </Link>
                    </td>
                    <td className="px-4 py-2">
                      <span className={r.success ? 'text-green-400' : 'text-red-400'}>
                        {r.success ? '✓' : '✗'}
                      </span>
                    </td>
                    <td className="px-4 py-2 text-gray-300">{r.execution_time_ms}ms</td>
                    <td className="px-4 py-2 text-gray-300">{r.instructions.toLocaleString()}</td>
                    <td className="px-4 py-2 text-gray-400">{r.timestamp}</td>
                    <td className="px-4 py-2 text-gray-400 max-w-[200px] truncate">{r.input}</td>
                  </tr>
                ))}
                {history.length === 0 && (
                  <tr><td colSpan={6} className="px-4 py-8 text-center text-gray-600">No executions yet</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
