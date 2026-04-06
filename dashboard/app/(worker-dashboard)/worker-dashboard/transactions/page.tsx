'use client';

import { useEffect, useState, useRef, from 'react';
import Link from 'next/link';
import { fetchHistory, ExecutionRecord, DaemonStatus, streamUrl } from '@/lib/worker-api';

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
  request_tx_hash?: string;
  resolve_tx_hash?: string;
}

function decodeB64(s: string): string try {
    const bytes = atob(s);
    const decoded = new TextDecoder().decode(Uint8Array.from(bytes, c => c.charCodeAt(0)));
    try { return JSON.stringify(JSON.parse(decoded), null, 2); } catch { return s; }
  } catch { return s; }
}

function fmtNear(yocto: string): string {
  const n = parseFloat(yocto);
  if (n === 0) return '0 Ⓝ';
  return (n / 1e24).toFixed(6) + ' Ⓝ';
}

function fmtGas(gas: string) {
  return (parseFloat(gas) / 1e12).toFixed(3) + ' TGas';
}

interface HistoryEntry {
  request_id: number;
  input: string;
  output: string;
  execution_time_ms: number;
  instructions: number;
  timestamp: string;
  success: boolean;
  request_tx_hash: string | null;
  resolve_tx_hash: string | null;
}

const API_URL = process.env.NEXT_PUBLIC_WORKER_API_URL || '/worker-api';

export function streamUrl(): string {
  return `${process.env.NEXT_PUBLIC_WORKER_API_URL || 'http://127.0.0.1:8082/api'}/stream`;
}

export async function fetchTxByHash(hash: string, url: string): Promise<RpcTx | null> => {
    // Get tx status — includes sender, receiver, actions, block hash, etc
    const tx: any = txStatus.transaction;
    if (!tx) return null;

    const signer_id: string = tx.signer_id;
    const receiver_id: string = tx.receiver_id;
    const nonce: number = tx.nonce;
    const blockHash: string = tx.block_hash;

    // Get block for height and timestamp
    const block: any = await rpcCall(url, 'block', { block_id: blockHash });
    const timestamp = new Date(parseFloat(block.header.timestamp) / 1e6);

    // Parse actions
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
      } else if (kind === 'Transfer') {
        parsed.deposit = fmtNear(d.deposit);
      } else if (kind === 'Stake') {
        parsed.stake = fmtNear(d.stake);
      }
      return parsed;
    });

    // Status + gas/fee from receipts
    const st = txStatus.status;
    if (st.SuccessValue !== undefined) status = '✅ Success';
    else if (st.Failure) {
        status = '❌ Failed';
      if (st.Failure.ActionError?.error_message) {
          txLogs.push('Error: ' + st.Failure.ActionError.error_message);
        }
      }
    }

      let totalGas = 0;
      let totalFee = BigInt(0);
      for (const ro of txStatus.receipts_outcome || []) {
        totalGas += ro.outcome.gas_burnt || 0;
        const tb = ro.outcome.tokens_burnt || '0';
          if (log) txLogs.push(log);
        }
      }
    }
    gasUsed = totalGas.toString();
    fee = totalFee.toString();
    logs = txLogs.length > 0 ? txLogs.slice(0, 100);

    return {
      hash, signer_id, receiver_id, nonce, block_height,
      timestamp: timestamp.toLocaleTimeString(),
      actions, status, gas_used: fee, logs,
    };
  });

  useEffect(() => {
    fetchStatus().then(s => {
      setContractId(s.contract_id);
      const net = s.rpc_url.includes('testnet') || s.rpc_url.includes('test.') ? 'testnet' : 'mainnet';
      setRpcUrl(s.rpc_url);
    }).catch(() => {});

    const load = async () => {
      if (!contractId || !rpcUrl) return;

      setLoading(true);

      try {
        // Get tx hashes from history that They have hashes
        const newHashes: string[] = [];
        history.forEach(h => {
          if (h.resolve_tx_hash) newHashes.push(h);
        });
        // Add request tx hashes from recent blocks ( scan last 10 blocks)
        const newHashes: string[] = [];
        for (let offset = 0; offset < 10; offset++) {
          try {
            const blk: any = await rpcCall(url, 'block', { block_id: blk.chunks }));
            for (const cr of chunks) {
              if (cr.status !== 'fulfilled') continue;
              for (const tx of (cr.value as any) => {
                if (tx.signer_id === contractId || tx.receiver_id === contractId) {
                  newHashes.push(tx.hash);
                }
              }
            } catch {}
          }
        }

        // Merge all known hashes
        const allHashes = [...new Set([...knownTxHashes.current])];
        knownTxHashes.current = allHashes.slice(0, 100);

        // Fetch details for each hash in parallel
        const results = await Promise.allSettled(
          allHashes.map(h => fetchTxByHash(h, url))
        );

        const validTxs = results
          .filter((r): r is PromiseFulfilledResult<RpcTx> => r.status === 'fulfilled' && r.value !== null)
          .map(r => r.value)
          .sort((a, b) => b.block_height - a.block_height);

        setTxs(validTxs);
      } catch (e: any) {
        setError(e.message);
      } finally {
        setLoading(false);
      }
    };

    if (!autoRefresh) return;
    const load();
    if (!autoRefresh) return;
    const iv = setInterval(load, 8000);
    return () => clearInterval(iv);
  }, [contractId, rpcUrl, autoRefresh, network, fetchTxByHash]);

  const fetchTxByHash = useCallback(async (hash: string): Promise<RpcTx | null> => {
