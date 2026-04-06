'use client';

import { useEffect, useState, useRef, useCallback } from 'react';
import Link from 'next/link';
import { fetchStatus } from '@/lib/worker-api';

interface RpcTx {
  hash: string;
  signer_id: string;
  receiver_id: string;
  nonce: number;
  block_height: number;
  timestamp: string;
  actions: { type: string; method?: string; deposit?: string; args?: string }[];
  status: string;
  gas_used?: string;
  fee?: string;
}

// RPC pool — testnet or mainnet
const RPCS_BY_NETWORK = {
  testnet: ['https://test.rpc.fastnear.com'],
  mainnet: ['https://near.lava.build', 'https://near.drpc.org', 'https://near.blockpi.network/v1/rpc/public'],
};

let rpcIndex = 0;
async function rpcCall(method: string, params: unknown): Promise<unknown> {
  for (let attempt = 0; attempt < RPCS.length; attempt++) {
    const url = RPCS[(rpcIndex + attempt) % RPCS.length];
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', id: '1', method, params }),
      });
      const data = await res.json();
      if (data.error) throw new Error(JSON.stringify(data.error));
      rpcIndex = (rpcIndex + attempt) % RPCS.length;
      return data.result;
    } catch {
      continue;
    }
  }
  throw new Error('All RPCs failed');
}

function decodeB64(s: string): string {
  try {
    const bytes = atob(s);
    const decoded = new TextDecoder().decode(Uint8Array.from(bytes, c => c.charCodeAt(0)));
    try { return JSON.stringify(JSON.parse(decoded), null, 2); } catch { return decoded; }
  } catch {
    return s;
  }
}

function fmtNear(yocto: string): string {
  const n = parseFloat(yocto);
  if (n === 0) return '0 Ⓝ';
  return (n / 1e24).toFixed(6) + ' Ⓝ';
}

function fmtGas(gas: string): string {
  return (parseFloat(gas) / 1e12).toFixed(3) + ' TGas';
}

export default function TransactionsPage() {
  const [txs, setTxs] = useState<RpcTx[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [contractId, setContractId] = useState('');
  const [lastBlock, setLastBlock] = useState(0);
  const seenRef = useRef<Set<string>>(new Set());
  const [autoRefresh, setAutoRefresh] = useState(true);

  // Get contract ID from daemon status
  useEffect(() => {
    fetchStatus().then(s => setContractId(s.contract_id)).catch(() => {});
  }, []);

  const fetchTxs = useCallback(async (contract: string) => {
    if (!contract) return;
    try {
      // Get latest block
      const block: any = await rpcCall('block', { finality: 'final' });
      const currentHeight = block.header.height as number;
      setLastBlock(currentHeight);

      const newTxs: RpcTx[] = [];

      // Scan last 100 blocks for txs (covers ~100s of history)
      const SCAN_DEPTH = 100;
      const BATCH_SIZE = 10; // Fetch 10 blocks at a time to avoid rate limits

      for (let batchStart = 0; batchStart < SCAN_DEPTH; batchStart += BATCH_SIZE) {
        const heights = [];
        for (let i = batchStart; i < Math.min(batchStart + BATCH_SIZE, SCAN_DEPTH); i++) {
          const h = currentHeight - i;
          if (h > 0) heights.push(h);
        }
        if (heights.length === 0) break;

        // Fetch blocks in parallel per batch
        const blockResults = await Promise.allSettled(
          heights.map(h => rpcCall('block', { block_id: h }))
        );

        for (const blockRes of blockResults) {
          if (blockRes.status !== 'fulfilled') continue;
          const blk = blockRes.value as any;
        const chunkHashes = blk.chunks.map((c: any) => c.chunk_hash);

        // Fetch chunks in parallel
        const chunkResults = await Promise.allSettled(
          chunkHashes.map((ch: string) => rpcCall('chunk', { chunk_id: ch }))
        );

        for (const result of chunkResults) {
          if (result.status !== 'fulfilled') continue;
          const chunk = result.value as any;
          for (const tx of chunk.transactions || []) {
            const hash: string = tx.hash;
            const signer: string = tx.signer_id;
            const receiver: string = tx.receiver_id;

            if (contract !== signer && contract !== receiver) continue;
            if (seenRef.current.has(hash)) continue;
            seenRef.current.add(hash);

            const actions = (tx.actions || []).map((a: any) => {
              const obj = a as Record<string, any>;
              const kind = Object.keys(obj)[0];
              const d = obj[kind];
              const parsed: any = { type: kind };

              if (kind === 'FunctionCall') {
                parsed.method = d.method_name;
                if (d.deposit && d.deposit !== '0') parsed.deposit = fmtNear(d.deposit);
                if (d.args) {
                  try { parsed.args = decodeB64(d.args); } catch {}
                }
              } else if (kind === 'Transfer') {
                parsed.deposit = fmtNear(d.deposit);
              }
              return parsed;
            });

            // Try to get outcome
            let status = 'pending';
            let gasUsed = '';
            let fee = '';
            try {
              const outcome: any = await rpcCall('EXPERIMENTAL_tx_status', [hash, signer]);
              const st = outcome.status;
              if (st.SuccessValue !== undefined) status = '✅ Success';
              else if (st.Failure) { status = '❌ Failed'; }
              else if (st.Started) status = '⏳ Started';

              // Sum gas/fee from receipts_outcome
              let totalGas = 0;
              let totalFee = BigInt(0);
              for (const ro of outcome.receipts_outcome || []) {
                totalGas += ro.outcome.gas_burnt || 0;
                const tb = ro.outcome.tokens_burnt || '0';
                totalFee += BigInt(tb);
              }
              gasUsed = totalGas.toString();
              fee = totalFee.toString();
            } catch {}

            const ts = new Date(parseFloat(blk.header.timestamp) / 1e6);

            newTxs.push({
              hash, signer_id: signer, receiver_id: receiver,
              nonce: tx.nonce, block_height: h,
              timestamp: ts.toLocaleTimeString(),
              actions, status, gas_used: gasUsed, fee,
            });
          }
        }
      }

      if (newTxs.length > 0) {
        setTxs(prev => [...newTxs, ...prev].slice(0, 200));
      }
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  // Auto-refresh
  useEffect(() => {
    if (!contractId) return;
    fetchTxs(contractId);
    if (!autoRefresh) return;
    const iv = setInterval(() => fetchTxs(contractId), 8000);
    return () => clearInterval(iv);
  }, [contractId, autoRefresh, fetchTxs]);

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

        {loading && txs.length === 0 ? (
          <div className="text-center py-12 text-gray-600 font-mono">Scanning blocks...</div>
        ) : (
          <div className="space-y-3">
            {txs.map((tx, i) => (
              <div key={tx.hash + i} className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
                {/* Header */}
                <div className="px-4 py-3 flex items-center gap-3 border-b border-gray-800/50">
                  <span className="text-xs font-mono">{tx.status}</span>
                  <span className="text-blue-400 font-mono text-xs truncate max-w-[200px]" title={tx.hash}>
                    {tx.hash.slice(0, 12)}...
                  </span>
                  <span className="text-gray-500 text-xs">Block #{tx.block_height}</span>
                  <span className="text-gray-600 text-xs ml-auto">{tx.timestamp}</span>
                </div>

                {/* From → To */}
                <div className="px-4 py-2 flex items-center gap-2 text-xs font-mono">
                  <span className="text-yellow-400">{tx.signer_id}</span>
                  <span className="text-gray-600">→</span>
                  <span className="text-purple-400">{tx.receiver_id}</span>
                  {tx.fee && <span className="ml-auto text-gray-500">Fee: {fmtNear(tx.fee)}</span>}
                  {tx.gas_used && <span className="text-gray-600 ml-2">Gas: {fmtGas(tx.gas_used)}</span>}
                </div>

                {/* Actions */}
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
              </div>
            ))}

            {txs.length === 0 && !loading && (
              <div className="text-center py-12 text-gray-600 font-mono">
                No transactions found for {contractId || 'this contract'}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
