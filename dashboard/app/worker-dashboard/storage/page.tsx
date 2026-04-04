'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { fetchStorage, StorageEntry } from '@/lib/worker-api';

export default function StoragePage() {
  const [entries, setEntries] = useState<StorageEntry[]>([]);
  const [error, setError] = useState('');

  useEffect(() => {
    fetchStorage().then(setEntries).catch(e => setError(e.message));
    const iv = setInterval(() => fetchStorage().then(setEntries).catch(() => {}), 10000);
    return () => clearInterval(iv);
  }, []);

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100">
      <div className="border-b border-gray-800 bg-gray-900">
        <div className="max-w-7xl mx-auto px-4 py-4 flex items-center gap-4">
          <Link href="/worker-dashboard" className="text-sm text-gray-400 hover:text-white">← Dashboard</Link>
          <h1 className="text-xl font-bold text-green-400 font-mono">Storage</h1>
          <span className="text-sm text-gray-500 font-mono">{entries.length} keys</span>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 py-6">
        {error && (
          <div className="bg-red-900/30 border border-red-700 rounded-lg p-4 text-red-300 font-mono text-sm mb-4">
            {error}
          </div>
        )}

        <div className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
          <table className="w-full text-sm font-mono">
            <thead>
              <tr className="text-gray-500 text-xs border-b border-gray-800">
                <th className="text-left px-4 py-3">KEY (decoded)</th>
                <th className="text-left px-4 py-3">HEX</th>
                <th className="text-left px-4 py-3">SIZE</th>
              </tr>
            </thead>
            <tbody>
              {entries.map((e, i) => (
                <tr key={i} className="border-t border-gray-800/50 hover:bg-gray-800/30">
                  <td className="px-4 py-2 text-blue-400">{e.name}</td>
                  <td className="px-4 py-2 text-gray-600 text-xs">{e.hex_name.slice(0, 32)}...</td>
                  <td className="px-4 py-2 text-gray-400">{e.size} bytes</td>
                </tr>
              ))}
              {entries.length === 0 && (
                <tr><td colSpan={3} className="px-4 py-8 text-center text-gray-600">No storage entries</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
