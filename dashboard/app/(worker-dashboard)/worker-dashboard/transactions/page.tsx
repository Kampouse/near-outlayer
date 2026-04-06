'use client';

import { useEffect, useState, useCallback, useRef } from 'react';
import Link from 'next/link';
import { fetchStatus } from '@/lib/worker-api';

interface RpcTx {
  hash: string;
  signer_id: string;
  receiver_id: string;
  block_height: number;
  timestamp: string;
  actions: { type: string; method?: string; deposit?: string; args?: string }[];
  status: string;
  gas_used?: string;
  fee?: string;
  logs?: string[];
  label?: string;
}

async function rpcCall(url: string, method: string, params: unknown): Promise<unknown> {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: '1', method, params }),
  });
  const data = await res.json();
  if (data.error) throw new Error(JSON.stringify(data.error));
  return data.result;
}

function decodeB64(s: string): string {
  try {
    const bytes = atob(s);
    const decoded = new TextDecoder().decode(Uint8Array.from(bytes, c => c.charCodeAt(0)));
    try { return JSON.stringify(JSON.parse(decoded), null, 2); } catch { return decoded; }
  } catch { return s; }
}

function fmtNear(yocto: string): string {
  const n = parseFloat(yocto);
  if (n === 0) return '0 Ⓝ';
  return (n / 1e24).toFixed(6) + ' Ⓝ';
}

function fmtGas(gas: string): string {
  return (parseFloat(gas) / 1e12).toFixed(3) + ' TGas';
}

// Fetch full tx details by hash
async function fetchTxByHash(hash: string, rpcUrl: string): Promise<RpcTx | null> {
  try {
    const txStatus: any = await rpcCall(rpcUrl, 'EXPERIMENTAL_tx_status', [hash]);
    const tx = txStatus.transaction;
    if (!tx) return null;

    const signer_id = tx.signer_id as string;
    const receiver_id = tx.receiver_id as string;
    const blockHash = tx.block_hash as string;

    const block: any = await rpcCall(rpcUrl, 'block', { block_id: blockHash });
    const block_height = block.header.height as number;
    const timestamp = new Date(parseFloat(block.header.timestamp) / 1e6);

    const actions = (tx.actions || []).map((a: any) => {
      const obj = a as Record<string, any>;
      const kind = Object.keys(obj)[0];
      const d = obj[kind];
      const parsed: any = { type: kind };
      if (kind === 'FunctionCall') {
        parsed.method = d.method_name;
        if (d.deposit && d.deposit !== '0') parsed.deposit = fmtNear(d.deposit);
        if (d.args) { try { parsed.args = decodeB64(d.args); } catch {} }
      } else if (kind === 'Transfer') {
        parsed.deposit = fmtNear(d.deposit);
      }
      return parsed;
    });

    let status = '⏳ Pending';
    const st = txStatus.status;
    if (st.SuccessValue !== undefined) status = '✅ Success';
    else if (st.Failure) status = '❌ Failed';

    let totalGas = 0;
    let totalFee = BigInt(0);
    const txLogs: string[] = [];
    for (const ro of txStatus.receipts_outcome || []) {
      totalGas += ro.outcome.gas_burnt || 0;
      totalFee += BigInt(ro.outcome.tokens_burnt || '0');
      for (const log of ro.outcome.logs || []) { if (log) txLogs.push(log); }
    }

    return {
      hash, signer_id, receiver_id, block_height,
      timestamp: timestamp.toLocaleTimeString(),
      actions, status,
      gas_used: totalGas.toString(),
      fee: totalFee.toString(),
      logs: txLogs.length > 0 ? txLogs : undefined,
    };
  } catch { return null; }
}

// Known tx hashes — updated from daemon history + SSE events
const knownHashes: string[] = [
  // Seed with the tx we already know about
  '9ncLqnK7e5Rw5BLp5wFb6HZvkcSr1zNe2oMhXwPU9wY5',
  '3DJpxt1pt8w9a6m1qZVMzd2yTfhXHr9TWYSqyS9tXEDC',
];

export default function TransactionsPage() {
  const [txs, setTxs] = useState<RpcTx[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [contractId, setContractId] = useState('');
  const [rpcUrl, setRpcUrl] = useState('');
  const [lastBlock, setLastBlock] = useState(0);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const seenRef = useRef<Set<string>>(new Set());

  useEffect(() => {
    fetchStatus().then(s => {
      setContractId(s.contract_id);
      setRpcUrl(s.rpc_url);
    }).catch(() => {});
  }, []);

  const load = useCallback(async () => {
    if (!rpcUrl) return;
    try {
      // Get latest block height for display
      const block: any = await rpcCall(rpcUrl, 'block', { finality: 'final' });
      setLastBlock(block.header.height);

      // Collect hashes from daemon history
      const hashes = new Set<string>(knownHashes);
      try {
        const historyRes = await fetch(`${process.env.NEXT_PUBLIC_WORKER_API_URL || '/worker-api'}/history`);
        if (historyRes.ok) {
          const history = await historyRes.json() as any[];
          for (const rec of history) {
            if (rec.resolve_tx_hash) hashes.add(rec.resolve_tx_hash);
            // Also try to find request tx hash from SSE events
          }
        }
      } catch {}

      // Fetch details for each hash (parallel, skip already seen)
      const newHashes = [...hashes].filter(h => !seenRef.current.has(h));
      const allHashes = [...seenRef.current, ...newHashes].slice(0, 50);

      const results = await Promise.allSettled(
        allHashes.map(h => fetchTxByHash(h, rpcUrl))
      );

      const validTxs = results
        .filter((r): r is PromiseFulfilledResult<RpcTx> => r.status === 'fulfilled' && r.value !== null)
        .map(r => r.value)
        .sort((a, b) => b.block_height - a.block_height);

      seenRef.current = new Set(allHashes);
      setTxs(validTxs);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [rpcUrl]);

  useEffect(() => {
    load();
    if (!autoRefresh) return;
    const iv = setInterval(load, 8000);
    return () => clearInterval(iv);
  }, [load, autoRefresh]);

  // SSE listener for real-time resolve events
  useEffect(() => {
    const sseUrl = `${process.env.NEXT_PUBLIC_WORKER_API_URL || 'http://127.0.0.1:8082/api'}/stream`;
    const es = new EventSource(sseUrl);
    es.onmessage = (e) => {
      const data = e.data as string;
      // If SSE event contains a tx hash, add it
      const match = data.match(/Tx:\s*(\w+)/);
      if (match) {
        knownHashes.unshift(match[1]);
        load(); // trigger refresh
      }
    };
    es.onerror = () => {};
    return () => es.close();
  }, [load]);

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100">
      <div className="border-b border-gray-800 bg-gray-900">
        <div className="max-w-7xl mx-auto px-4 py-4 flex items-center gap-4">
          <Link href="/worker-dashboard" className="text-sm text-gray-400 hover:text-white">← Dashboard</Link>
          <h1 className="text-xl font-bold text-green-400 font-mono">On-Chain Transactions</h1>
          <span className="ml-auto text-xs text-gray-500 font-mono">
            Block #{lastBlock} | {contractId}
          </span>
          <button
            onClick={() => setAutoRefresh(!autoRefresh)}
            className={`px-3 py-1 rounded text-xs font-mono ${autoRefresh ? 'bg-green-900/50 text-green-400' : 'bg-gray-800 text-gray-400'}`}
          >
            {autoRefresh ? '● LIVE' : '○ PAUSED'}
          </button>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 py-6">
        {error && (
          <div className="bg-red-900/30 border border-red-700 rounded-lg p-3 text-red-300 font-mono text-xs mb-4">
            {error}
          </div>
        )}

        {loading ? (
          <div className="text-center py-12 text-gray-600 font-mono">Loading transactions...</div>
        ) : txs.length === 0 ? (
          <div className="text-center py-12 text-gray-600 font-mono">
            No transactions yet. Submit a request to see it here.
          </div>
        ) : (
          <div className="space-y-3">
            {txs.map((tx) => (
              <div key={tx.hash} className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
                <div className="px-4 py-3 flex items-center gap-3 border-b border-gray-800/50">
                  <span className="text-xs font-mono">{tx.status}</span>
                  <span className="text-blue-400 font-mono text-xs truncate max-w-[200px]" title={tx.hash}>
                    {tx.hash.slice(0, 16)}...
                  </span>
                  <span className="text-gray-500 text-xs">Block #{tx.block_height}</span>
                  <span className="text-gray-600 text-xs ml-auto">{tx.timestamp}</span>
                </div>

                <div className="px-4 py-2 flex items-center gap-2 text-xs font-mono">
                  <span className="text-yellow-400">{tx.signer_id}</span>
                  <span className="text-gray-600">→</span>
                  <span className="text-purple-400">{tx.receiver_id}</span>
                  {tx.fee && <span className="ml-auto text-gray-500">Fee: {fmtNear(tx.fee)}</span>}
                  {tx.gas_used && <span className="text-gray-600 ml-2">Gas: {fmtGas(tx.gas_used)}</span>}
                </div>

                <div className="px-4 py-2 space-y-2">
                  {tx.actions.map((a, j) => (
                    <div key={j} className="text-xs font-mono">
                      <div className="flex items-center gap-2">
                        <span className="bg-gray-800 px-2 py-0.5 rounded text-cyan-400">{a.type}</span>
                        {a.method && <span className="text-green-400 font-bold">{a.method}()</span>}
                        {a.deposit && <span className="text-yellow-300">{a.deposit}</span>}
                      </div>
                      {a.args && (
                        <pre className="mt-1 p-2 bg-gray-950 rounded text-gray-400 overflow-x-auto max-h-32 text-[11px]">
                          {a.args.length > 500 ? a.args.slice(0, 500) + '...' : a.args}
                        </pre>
                      )}
                    </div>
                  ))}
                </div>

                {tx.logs && tx.logs.length > 0 && (
                  <div className="px-4 py-2 border-t border-gray-800/50">
                    <div className="text-xs text-gray-500 font-mono mb-1">LOGS</div>
                    <div className="space-y-0.5">
                      {tx.logs.map((log, j) => (
                        <div key={j} className="text-[10px] font-mono text-gray-500 truncate" title={log}>
                          {log.length > 200 ? log.slice(0, 200) + '...' : log}
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
