'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { fetchHistory, ExecutionRecord } from '@/lib/worker-api';
import { useParams } from 'next/navigation';

export default function ExecutionDetailPage() {
  const params = useParams();
  const id = Number(params.id);
  const [record, setRecord] = useState<ExecutionRecord | null>(null);
  const [error, setError] = useState('');

  useEffect(() => {
    fetchHistory().then(h => {
      const r = h.find(r => r.request_id === id);
      if (r) setRecord(r);
      else setError('Execution not found');
    }).catch(e => setError(e.message));
  }, [id]);

  if (error) {
    return (
      <div className="min-h-screen bg-gray-950 text-gray-100">
        <div className="max-w-7xl mx-auto px-4 py-8">
          <Link href="/worker-dashboard/executions" className="text-sm text-gray-400 hover:text-white">← Executions</Link>
          <div className="mt-8 text-red-400 font-mono">{error}</div>
        </div>
      </div>
    );
  }

  if (!record) {
    return (
      <div className="min-h-screen bg-gray-950 text-gray-100 flex items-center justify-center">
        <div className="text-gray-500 font-mono">Loading...</div>
      </div>
    );
  }

  let parsedOutput = '';
  try { parsedOutput = JSON.stringify(JSON.parse(record.output), null, 2); } catch { parsedOutput = record.output; }
  let parsedInput = '';
  try { parsedInput = JSON.stringify(JSON.parse(record.input), null, 2); } catch { parsedInput = record.input; }

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100">
      <div className="border-b border-gray-800 bg-gray-900">
        <div className="max-w-7xl mx-auto px-4 py-4 flex items-center gap-4">
          <Link href="/worker-dashboard/executions" className="text-sm text-gray-400 hover:text-white">← Executions</Link>
          <h1 className="text-xl font-bold text-green-400 font-mono">Execution #{id}</h1>
          <span className={record.success ? 'text-green-400 font-mono' : 'text-red-400 font-mono'}>
            {record.success ? '✓ Success' : '✗ Failed'}
          </span>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 py-6 space-y-6">
        {/* Metrics */}
        <div className="grid grid-cols-4 gap-4">
          <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
            <div className="text-xs text-gray-500 font-mono mb-1">TIME</div>
            <div className="text-xl font-bold text-white font-mono">{record.execution_time_ms}ms</div>
          </div>
          <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
            <div className="text-xs text-gray-500 font-mono mb-1">INSTRUCTIONS</div>
            <div className="text-xl font-bold text-white font-mono">{record.instructions.toLocaleString()}</div>
          </div>
          <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
            <div className="text-xs text-gray-500 font-mono mb-1">REQUEST ID</div>
            <div className="text-xl font-bold text-yellow-400 font-mono">#{record.request_id}</div>
          </div>
          <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
            <div className="text-xs text-gray-500 font-mono mb-1">TIMESTAMP</div>
            <div className="text-sm font-bold text-gray-300 font-mono">{record.timestamp}</div>
          </div>
        </div>

        {/* Input */}
        <div className="bg-gray-900 border border-gray-800 rounded-lg">
          <div className="px-4 py-3 border-b border-gray-800">
            <h2 className="text-sm font-bold font-mono text-gray-300">INPUT</h2>
          </div>
          <pre className="p-4 text-sm font-mono text-blue-300 overflow-x-auto">{parsedInput}</pre>
        </div>

        {/* Output */}
        <div className="bg-gray-900 border border-gray-800 rounded-lg">
          <div className="px-4 py-3 border-b border-gray-800">
            <h2 className="text-sm font-bold font-mono text-gray-300">OUTPUT</h2>
          </div>
          <pre className="p-4 text-sm font-mono text-green-300 overflow-x-auto">{parsedOutput}</pre>
        </div>
      </div>
    </div>
  );
}
