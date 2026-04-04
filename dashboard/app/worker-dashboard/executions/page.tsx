'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { fetchHistory, ExecutionRecord } from '@/lib/worker-api';

export default function ExecutionsPage() {
  const [history, setHistory] = useState<ExecutionRecord[]>([]);
  const [filter, setFilter] = useState<'all' | 'success' | 'fail'>('all');

  useEffect(() => {
    fetchHistory().then(setHistory).catch(() => {});
    const iv = setInterval(() => fetchHistory().then(setHistory).catch(() => {}), 5000);
    return () => clearInterval(iv);
  }, []);

  const filtered = history.filter(r => {
    if (filter === 'success') return r.success;
    if (filter === 'fail') return !r.success;
    return true;
  });

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100">
      <div className="border-b border-gray-800 bg-gray-900">
        <div className="max-w-7xl mx-auto px-4 py-4 flex items-center gap-4">
          <Link href="/worker-dashboard" className="text-sm text-gray-400 hover:text-white">← Dashboard</Link>
          <h1 className="text-xl font-bold text-green-400 font-mono">Executions</h1>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 py-6">
        {/* Filters */}
        <div className="flex gap-2 mb-4">
          {(['all', 'success', 'fail'] as const).map(f => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={`px-3 py-1.5 rounded text-sm font-mono ${
                filter === f ? 'bg-green-900/50 text-green-400 border border-green-700' : 'bg-gray-800 text-gray-400 border border-gray-700'
              }`}
            >
              {f === 'all' ? 'All' : f === 'success' ? '✓ Success' : '✗ Failed'}
            </button>
          ))}
          <span className="ml-auto text-sm text-gray-500 font-mono">{filtered.length} results</span>
        </div>

        <div className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
          <table className="w-full text-sm font-mono">
            <thead>
              <tr className="text-gray-500 text-xs border-b border-gray-800">
                <th className="text-left px-4 py-3">REQ ID</th>
                <th className="text-left px-4 py-3">STATUS</th>
                <th className="text-left px-4 py-3">TIME</th>
                <th className="text-left px-4 py-3">INSTRUCTIONS</th>
                <th className="text-left px-4 py-3">TIMESTAMP</th>
                <th className="text-left px-4 py-3">INPUT</th>
                <th className="text-left px-4 py-3">OUTPUT</th>
              </tr>
            </thead>
            <tbody>
              {filtered.reverse().map((r, i) => (
                <tr key={i} className="border-t border-gray-800/50 hover:bg-gray-800/30">
                  <td className="px-4 py-3">
                    <Link href={`/worker-dashboard/executions/${r.request_id}`} className="text-blue-400 hover:underline">
                      #{r.request_id}
                    </Link>
                  </td>
                  <td className="px-4 py-3">
                    <span className={`px-2 py-0.5 rounded text-xs ${r.success ? 'bg-green-900/50 text-green-400' : 'bg-red-900/50 text-red-400'}`}>
                      {r.success ? 'SUCCESS' : 'FAILED'}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-gray-300">{r.execution_time_ms}ms</td>
                  <td className="px-4 py-3 text-gray-300">{r.instructions.toLocaleString()}</td>
                  <td className="px-4 py-3 text-gray-400">{r.timestamp}</td>
                  <td className="px-4 py-3 text-gray-400 max-w-[200px] truncate">{r.input}</td>
                  <td className="px-4 py-3 text-gray-400 max-w-[200px] truncate">{r.output}</td>
                </tr>
              ))}
              {filtered.length === 0 && (
                <tr><td colSpan={7} className="px-4 py-12 text-center text-gray-600">No executions found</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
