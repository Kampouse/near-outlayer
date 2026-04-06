'use client';

import { useEffect, useState, useCallback } from 'react';
import Link from 'next/link';

interface HistoryRecord {
  request_id: number;
  input: string;
  output: string;
  execution_time_ms: number;
  instructions: number;
  timestamp: string;
  success: boolean;
  resolve_tx_hash: string | null;
}

export default function TransactionsPage() {
  const [records, setRecords] = useState<HistoryRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [connected, setConnected] = useState(false);

  const load = useCallback(async () => {
    try {
      const res = await fetch('/worker-api/history');
      if (res.ok) {
        const data = await res.json();
        setRecords(data.reverse());
      }
    } catch (e) {
      console.error('Failed to load history:', e);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  // SSE — reactive updates, no polling
  useEffect(() => {
    const es = new EventSource('/worker-api/stream');
    es.onopen = () => setConnected(true);
    es.onerror = () => setConnected(false);
    es.onmessage = () => { load(); }; // reload history on any event
    return () => es.close();
  }, [load]);

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100">
      <div className="border-b border-gray-800 bg-gray-900">
        <div className="max-w-7xl mx-auto px-4 py-4 flex items-center gap-4">
          <Link href="/worker-dashboard" className="text-sm text-gray-400 hover:text-white">← Dashboard</Link>
          <h1 className="text-xl font-bold text-green-400 font-mono">Transactions</h1>
          <span className={`ml-auto text-xs font-mono ${connected ? 'text-green-400' : 'text-gray-500'}`}>
            {connected ? '● LIVE' : '○ DISCONNECTED'}
          </span>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 py-6">
        {loading ? (
          <div className="text-center py-12 text-gray-600 font-mono">Loading...</div>
        ) : records.length === 0 ? (
          <div className="text-center py-12 text-gray-600 font-mono">No transactions yet</div>
        ) : (
          <div className="space-y-3">
            {records.map((r) => (
              <div key={r.request_id} className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
                <div className="px-4 py-3 flex items-center gap-3 border-b border-gray-800/50">
                  <span className="text-xs font-mono">{r.success ? '✅' : '❌'}</span>
                  <span className="text-cyan-400 font-mono text-sm font-bold">#{r.request_id}</span>
                  <span className="text-gray-500 text-xs">{r.timestamp}</span>
                  <span className="text-gray-600 text-xs ml-auto">{r.execution_time_ms}ms</span>
                </div>
                <div className="px-4 py-2 space-y-1">
                  <div className="text-xs font-mono">
                    <span className="text-gray-500">Input:</span>{' '}
                    <span className="text-yellow-300">{r.input}</span>
                  </div>
                  <div className="text-xs font-mono">
                    <span className="text-gray-500">Output:</span>{' '}
                    <pre className="text-green-300 whitespace-pre-wrap break-all mt-1 max-h-32 overflow-auto text-[11px]">
                      {r.output.length > 300 ? r.output.slice(0, 300) + '...' : r.output}
                    </pre>
                  </div>
                  <div className="flex gap-4 text-xs font-mono text-gray-500">
                    <span>{r.instructions.toLocaleString()} instructions</span>
                  </div>
                  {r.resolve_tx_hash && (
                    <div className="text-xs font-mono mt-1">
                      <span className="text-gray-500">Tx:</span>{' '}
                      <a
                        href={`https://testnet.near.rocks/tx/${r.resolve_tx_hash}`}
                        target="_blank"
                        className="text-blue-400 hover:underline"
                      >
                        {r.resolve_tx_hash.slice(0, 20)}...
                      </a>
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
