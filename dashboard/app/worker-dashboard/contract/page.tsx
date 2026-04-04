'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { fetchContract, ContractState } from '@/lib/worker-api';

export default function ContractPage() {
  const [state, setState] = useState<ContractState | null>(null);
  const [error, setError] = useState('');

  useEffect(() => {
    fetchContract().then(setState).catch(e => setError(e.message));
    const iv = setInterval(() => fetchContract().then(setState).catch(() => {}), 10000);
    return () => clearInterval(iv);
  }, []);

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100">
      <div className="border-b border-gray-800 bg-gray-900">
        <div className="max-w-7xl mx-auto px-4 py-4 flex items-center gap-4">
          <Link href="/worker-dashboard" className="text-sm text-gray-400 hover:text-white">← Dashboard</Link>
          <h1 className="text-xl font-bold text-green-400 font-mono">Contract</h1>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 py-6 space-y-6">
        {error && (
          <div className="bg-red-900/30 border border-red-700 rounded-lg p-4 text-red-300 font-mono text-sm">
            {error}
          </div>
        )}

        {state && (
          <>
            <div className="grid grid-cols-2 gap-4">
              <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
                <div className="text-xs text-gray-500 font-mono mb-1">CONTRACT</div>
                <div className="text-lg font-bold text-yellow-400 font-mono">{state.contract_id}</div>
              </div>
              <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
                <div className="text-xs text-gray-500 font-mono mb-1">PENDING REQUESTS</div>
                <div className={`text-2xl font-bold font-mono ${state.pending_count > 0 ? 'text-yellow-400' : 'text-green-400'}`}>
                  {state.pending_count}
                </div>
              </div>
            </div>

            {state.pending_request_ids.length > 0 && (
              <div className="bg-gray-900 border border-gray-800 rounded-lg p-4">
                <div className="text-xs text-gray-500 font-mono mb-3">PENDING IDS</div>
                <div className="flex flex-wrap gap-2">
                  {state.pending_request_ids.map(id => (
                    <Link
                      key={id}
                      href={`/worker-dashboard/executions/${id}`}
                      className="px-3 py-1 bg-gray-800 border border-gray-700 rounded text-yellow-400 font-mono text-sm hover:bg-gray-700"
                    >
                      #{id}
                    </Link>
                  ))}
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
