const API_URL = process.env.NEXT_PUBLIC_WORKER_API_URL || 'http://127.0.0.1:8082';

export interface DaemonStatus {
  running: boolean;
  uptime_secs: number;
  poll_count: number;
  last_poll_time: string | null;
  contract_id: string;
  account_id: string;
  rpc_url: string;
  poll_interval_secs: number;
  dashboard_addr: string | null;
}

export interface ExecutionRecord {
  request_id: number;
  input: string;
  output: string;
  execution_time_ms: number;
  instructions: number;
  timestamp: string;
  success: boolean;
}

export interface StorageEntry {
  name: string;
  hex_name: string;
  size: number;
}

export interface ContractState {
  pending_request_ids: number[];
  pending_count: number;
  contract_id: string;
}

export async function fetchStatus(): Promise<DaemonStatus> {
  const res = await fetch(`${API_URL}/api/status`);
  if (!res.ok) throw new Error('Failed to fetch status');
  return res.json();
}

export async function fetchHistory(): Promise<ExecutionRecord[]> {
  const res = await fetch(`${API_URL}/api/history`);
  if (!res.ok) throw new Error('Failed to fetch history');
  return res.json();
}

export async function fetchStorage(): Promise<StorageEntry[]> {
  const res = await fetch(`${API_URL}/api/storage`);
  if (!res.ok) throw new Error('Failed to fetch storage');
  return res.json();
}

export async function fetchContract(): Promise<ContractState> {
  const res = await fetch(`${API_URL}/api/contract`);
  if (!res.ok) throw new Error('Failed to fetch contract');
  return res.json();
}

export function streamUrl(): string {
  return `${API_URL}/api/stream`;
}
