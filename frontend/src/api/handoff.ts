import { request } from './client'

export type HandoffRecord = Record<string, any>

async function postJson<T = HandoffRecord>(path: string, payload: unknown = {}): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  const data = await response.json().catch(() => ({}))
  if (!response.ok) {
    throw new Error(data?.message || data?.detail || `接口返回 ${response.status}`)
  }
  return data as T
}

async function getJson<T = HandoffRecord>(path: string): Promise<T> {
  const response = await request(path)
  const data = await response.json().catch(() => ({}))
  if (!response.ok) {
    throw new Error(data?.message || data?.detail || `接口返回 ${response.status}`)
  }
  return data as T
}

export const handoffApi = {
  exportPackage: (payload: HandoffRecord) => postJson('/api/snapshot-handoff/packages/export', payload),
  verifyPackage: (pkg: HandoffRecord) => postJson('/api/snapshot-handoff/packages/verify', { package: pkg }),
  importPackage: (pkg: HandoffRecord) => postJson('/api/snapshot-handoff/packages/import', { package: pkg }),
  startImport: (pkg: HandoffRecord) => postJson('/api/snapshot-handoff/imports', pkg),
  progress: (uploadId: string) => getJson(`/api/snapshot-handoff/imports/${encodeURIComponent(uploadId)}`),
  uploadSegment: (uploadId: string, segmentNo: number, segment: HandoffRecord) =>
    postJson(`/api/snapshot-handoff/imports/${encodeURIComponent(uploadId)}/segments/${segmentNo}`, segment),
  interrupt: (uploadId: string) =>
    postJson(`/api/snapshot-handoff/imports/${encodeURIComponent(uploadId)}/interrupt`, {}),
  resume: (uploadId: string) =>
    postJson(`/api/snapshot-handoff/imports/${encodeURIComponent(uploadId)}/resume`, {}),
  complete: (uploadId: string) =>
    postJson(`/api/snapshot-handoff/imports/${encodeURIComponent(uploadId)}/complete`, {}),
  registerAlternative: (candidateId: string, newPackageNo: string) =>
    postJson(`/api/snapshot-handoff/candidates/${encodeURIComponent(candidateId)}/register-alternative`, {
      new_package_no: newPackageNo,
    }),
  baselines: () => getJson('/api/snapshot-handoff/baselines'),
  conflicts: () => getJson('/api/snapshot-handoff/conflicts'),
  windows: () => getJson('/api/snapshot-handoff/backfill-windows'),
  openWindow: (packageNo: string) => postJson('/api/snapshot-handoff/backfill-windows', { package_no: packageNo }),
  compare: (firstWindowId: string, secondWindowId: string) =>
    postJson('/api/snapshot-handoff/backfill-windows/compare', {
      first_window_id: firstWindowId,
      second_window_id: secondWindowId,
    }),
}
