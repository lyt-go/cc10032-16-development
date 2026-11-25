<template>
  <section class="handoff-panel" data-testid="snapshot-handoff-panel">
    <div class="handoff-head">
      <div>
        <h3>快照交接包 / 分段核验 / 回填基准</h3>
        <p>导出完整业务行与分段凭证；导入后只有完整且已核验的版本会进入回填基准清单。</p>
      </div>
      <button class="btn ghost" type="button" data-testid="handoff-refresh" @click="refreshAll">刷新清单</button>
    </div>

    <div class="handoff-grid">
      <label class="field">
        <span>交接包编号</span>
        <input v-model="form.packageNo" data-testid="handoff-package-no" />
      </label>
      <label class="field">
        <span>源表</span>
        <input v-model="form.sourceTable" data-testid="handoff-source-table" />
      </label>
      <label class="field">
        <span>快照版本</span>
        <input v-model="form.version" data-testid="handoff-version" />
      </label>
      <label class="field">
        <span>维护员</span>
        <input v-model="form.maintainer" data-testid="handoff-maintainer" />
      </label>
      <label class="field narrow">
        <span>分段数</span>
        <input v-model.number="form.segmentCount" type="number" min="1" max="20" data-testid="handoff-segment-count" />
      </label>
      <label class="field grow">
        <span>业务键</span>
        <input v-model="form.businessKey" data-testid="handoff-business-key" />
      </label>
    </div>

    <label class="field">
      <span>完整快照行（JSON 数组）</span>
      <textarea v-model="form.rowsText" rows="5" data-testid="handoff-rows-json"></textarea>
    </label>

    <div class="actions">
      <button class="btn primary" type="button" data-testid="handoff-export" :disabled="busy" @click="exportPackage">
        生成并导出交接包
      </button>
      <label class="btn" data-testid="handoff-import-file-label">
        选择交接包导入
        <input type="file" accept="application/json,.json" hidden data-testid="handoff-import-file" @change="onFileSelected" />
      </label>
      <button class="btn" type="button" data-testid="handoff-quick-import" :disabled="!packageData || busy" @click="quickImport">
        一键完整导入当前包
      </button>
      <button class="btn" type="button" data-testid="handoff-start-segments" :disabled="!packageData || busy" @click="startSegments">
        开始分段导入
      </button>
    </div>

    <div v-if="packageSummary" class="notice ok" data-testid="handoff-package-summary">
      已生成 {{ packageSummary.package_no }} / {{ packageSummary.snapshot_version }}：
      {{ packageSummary.row_count }} 行、{{ packageSummary.segment_count }} 段，
      凭证 {{ shortHash(packageSummary.content_credential) }}
      <a v-if="downloadUrl" :href="downloadUrl" :download="`${packageSummary.package_no}.json`">重新下载 JSON</a>
    </div>

    <div v-if="progress" class="progress-box" data-testid="handoff-progress">
      <div class="progress-head">
        <strong>上传候选 {{ progress.candidate_id }}</strong>
        <span :data-status="progress.status">{{ progress.status }} · {{ progress.progress_percent }}%</span>
      </div>
      <p>{{ progress.message }}；已核验段：{{ progress.verified_segment_nos?.join('、') || '无' }}</p>
      <div class="actions compact">
        <button class="btn" type="button" data-testid="handoff-upload-next" :disabled="!progress.next_segment_no || busy" @click="uploadNext">
          传第 {{ progress.next_segment_no || '-' }} 段
        </button>
        <button class="btn ghost" type="button" data-testid="handoff-interrupt" :disabled="busy" @click="interrupt">模拟中断</button>
        <button class="btn ghost" type="button" data-testid="handoff-resume" :disabled="busy" @click="resume">恢复进度</button>
        <button class="btn primary" type="button" data-testid="handoff-complete" :disabled="(progress.missing_segment_nos?.length ?? 1) > 0 || busy" @click="complete">
          完成并登记
        </button>
      </div>
    </div>

    <div class="backfill-bar">
      <label class="field grow">
        <span>按编号发起回填窗口</span>
        <input v-model="windowPackageNo" data-testid="handoff-window-package-no" />
      </label>
      <button class="btn primary" type="button" data-testid="handoff-open-window" :disabled="busy" @click="openWindow">
        实际发起回填
      </button>
    </div>

    <div v-if="windowResult" class="window-result" data-testid="handoff-window-result" :data-ok="String(Boolean(windowResult.ok))">
      <h4>回填窗口 {{ windowResult.window_id }} · {{ windowResult.status }}</h4>
      <p>{{ windowResult.message }}</p>
      <div v-if="windowResult.key_results?.length" class="key-table-wrap">
        <table class="key-table">
          <thead><tr><th>业务键</th><th>结果</th><th>差异</th></tr></thead>
          <tbody>
            <tr v-for="item in windowResult.key_results" :key="item.key" :data-matched="String(item.matched)">
              <td>{{ item.key }}</td>
              <td>{{ item.status }}</td>
              <td>{{ item.differences?.length ? JSON.stringify(item.differences) : '一致' }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div v-if="comparison" class="notice" data-testid="handoff-comparison">{{ comparison.summary }}</div>

    <div class="handoff-columns">
      <div>
        <h4>回填基准清单（{{ baselines.length }}）</h4>
        <ul data-testid="handoff-baselines">
          <li v-for="item in baselines" :key="item.baseline_id">
            <strong>{{ item.package_no }}</strong> / {{ item.snapshot_version }} /
            {{ item.source_table }} / {{ item.row_count }} 行 /
            {{ shortHash(item.content_credential) }}
          </li>
        </ul>
      </div>
      <div>
        <h4>冲突与隔离（{{ conflicts.length }}）</h4>
        <ul data-testid="handoff-conflicts">
          <li v-for="(item, index) in conflicts" :key="`${item.at}-${index}`" class="conflict-item">
            <strong>{{ item.package_no }}</strong>：{{ item.status || item.type }}
            <br />实际保留：{{ item.actual_retained_candidate?.candidate_id || item.retained_candidate?.baseline_id || '—' }}
            <br />败方下一步：{{ item.loser_next_step || item.next_action_for_loser }}
            <button
              v-if="item.loser_candidate?.candidate_id"
              class="link"
              type="button"
              data-testid="handoff-register-alternative"
              @click="registerAlternative(item.loser_candidate.candidate_id, item.alternative_package_no)"
            >
              败方以 {{ item.alternative_package_no || '新编号' }} 另行登记
            </button>
          </li>
        </ul>
      </div>
    </div>

    <p v-if="errorMessage" class="error-text" data-testid="handoff-error">{{ errorMessage }}</p>
    <p v-if="infoMessage" class="info-text" data-testid="handoff-info">{{ infoMessage }}</p>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import { handoffApi, type HandoffRecord } from '@/api/handoff'

const emit = defineEmits<{ changed: [] }>()

const defaultRows = [
  { id: 5001, order_no: 'HO-0001', amount: 100, batch: 'ui-demo' },
  { id: 5002, order_no: 'HO-0002', amount: 200, batch: 'ui-demo' },
  { id: 5003, order_no: 'HO-0003', amount: 300, batch: 'ui-demo' },
  { id: 5004, order_no: 'HO-0004', amount: 400, batch: 'ui-demo' },
]

const form = reactive({
  packageNo: 'SNAP-HANDOFF-UI',
  sourceTable: 'dwd_orders_handoff',
  version: 'v1',
  maintainer: 'maintainer-ui',
  segmentCount: 2,
  businessKey: 'id',
  rowsText: JSON.stringify(defaultRows, null, 2),
})

const busy = ref(false)
const errorMessage = ref('')
const infoMessage = ref('')
const packageData = ref<HandoffRecord | null>(null)
const packageSummary = ref<HandoffRecord | null>(null)
const downloadUrl = ref('')
const uploadId = ref('')
const progress = ref<HandoffRecord | null>(null)
const windowPackageNo = ref(form.packageNo)
const windowResult = ref<HandoffRecord | null>(null)
const previousWindowId = ref('')
const comparison = ref<HandoffRecord | null>(null)
const baselines = ref<HandoffRecord[]>([])
const conflicts = ref<HandoffRecord[]>([])

function shortHash(value?: string): string {
  return value ? String(value).slice(0, 12) : '—'
}

function packagePayload(): HandoffRecord {
  let rows: unknown
  try {
    rows = JSON.parse(form.rowsText)
  } catch (error) {
    throw new Error(`快照行 JSON 无法解析：${error instanceof Error ? error.message : ''}`)
  }
  if (!Array.isArray(rows)) throw new Error('快照行必须是 JSON 数组')
  return {
    package_no: form.packageNo,
    source_table: form.sourceTable,
    snapshot_version: form.version,
    maintainer: form.maintainer,
    segment_count: Number(form.segmentCount),
    business_key: form.businessKey,
    rows,
  }
}

function rememberWindow(result: HandoffRecord) {
  if (windowResult.value?.window_id) previousWindowId.value = windowResult.value.window_id
  windowResult.value = result
  if (previousWindowId.value && result.window_id && previousWindowId.value !== result.window_id) {
    void compareWindows(previousWindowId.value, result.window_id)
  }
}

async function compareWindows(first: string, second: string) {
  try {
    comparison.value = await handoffApi.compare(first, second)
  } catch {
    comparison.value = null
  }
}

async function refreshAll() {
  const [baselineResult, conflictResult] = await Promise.all([
    handoffApi.baselines(),
    handoffApi.conflicts(),
  ])
  baselines.value = baselineResult.items || []
  conflicts.value = conflictResult.items || []
}

async function exportPackage() {
  busy.value = true
  errorMessage.value = ''
  infoMessage.value = ''
  try {
    const pkg = await handoffApi.exportPackage(packagePayload())
    packageData.value = pkg
    packageSummary.value = pkg
    windowPackageNo.value = String(pkg.package_no)
    const blob = new Blob([JSON.stringify(pkg, null, 2)], { type: 'application/json' })
    if (downloadUrl.value) URL.revokeObjectURL(downloadUrl.value)
    downloadUrl.value = URL.createObjectURL(blob)
    infoMessage.value = '交接包已生成，可下载或直接开始分段导入。'
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  } finally {
    busy.value = false
  }
}

function onFileSelected(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  if (!file) return
  const reader = new FileReader()
  reader.onload = () => {
    try {
      const pkg = JSON.parse(String(reader.result))
      packageData.value = pkg
      packageSummary.value = pkg
      form.packageNo = pkg.package_no || form.packageNo
      form.sourceTable = pkg.source_table || form.sourceTable
      form.version = pkg.snapshot_version || form.version
      windowPackageNo.value = form.packageNo
      infoMessage.value = `已载入交接包 ${form.packageNo}`
    } catch (error) {
      errorMessage.value = error instanceof Error ? error.message : '交接包解析失败'
    }
  }
  reader.readAsText(file)
}

async function startSegments() {
  if (!packageData.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    const result = await handoffApi.startImport(packageData.value)
    uploadId.value = result.upload_id
    progress.value = result
    infoMessage.value = '分段导入已开始。'
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '开始导入失败'
  } finally {
    busy.value = false
  }
}

async function uploadNext() {
  if (!packageData.value || !progress.value?.next_segment_no) return
  const segmentNo = Number(progress.value.next_segment_no)
  const segment = (packageData.value.segments || []).find((item: HandoffRecord) => item.segment_no === segmentNo)
  if (!segment) {
    errorMessage.value = `交接包中找不到第 ${segmentNo} 段`
    return
  }
  busy.value = true
  try {
    progress.value = await handoffApi.uploadSegment(uploadId.value, segmentNo, segment)
    if (progress.value.registration?.baseline) await refreshAll()
    emit('changed')
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分段上传失败'
  } finally {
    busy.value = false
  }
}

async function interrupt() {
  progress.value = await handoffApi.interrupt(uploadId.value)
}

async function resume() {
  progress.value = await handoffApi.resume(uploadId.value)
}

async function complete() {
  busy.value = true
  try {
    const result = await handoffApi.complete(uploadId.value)
    if (!result.ok) throw new Error(result.message || '登记失败')
    progress.value = result
    infoMessage.value = result.message || '登记完成'
    await refreshAll()
    emit('changed')
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '完成登记失败'
  } finally {
    busy.value = false
  }
}

async function quickImport() {
  if (!packageData.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    const result = await handoffApi.importPackage(packageData.value)
    if (!result.ok) {
      conflicts.value = (await handoffApi.conflicts()).items || []
      throw new Error(result.message || '导入未登记，可能存在冲突或隔离')
    }
    infoMessage.value = result.message || '完整导入成功'
    windowPackageNo.value = String(result.package_no || form.packageNo)
    await refreshAll()
    emit('changed')
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '一键导入失败'
    await refreshAll()
  } finally {
    busy.value = false
  }
}

async function openWindow() {
  busy.value = true
  errorMessage.value = ''
  try {
    const result = await handoffApi.openWindow(windowPackageNo.value)
    rememberWindow(result)
    if (!result.ok) infoMessage.value = result.message
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回填窗口发起失败'
  } finally {
    busy.value = false
  }
}

async function registerAlternative(candidateId: string, suggested?: string) {
  const newPackageNo = window.prompt('请输入另行登记的新编号', suggested || `${form.packageNo}-ALT`)
  if (!newPackageNo) return
  busy.value = true
  try {
    const result = await handoffApi.registerAlternative(candidateId, newPackageNo)
    if (!result.ok) throw new Error(result.message || '另行登记失败')
    infoMessage.value = result.message
    windowPackageNo.value = newPackageNo
    await refreshAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '另行登记失败'
  } finally {
    busy.value = false
  }
}

onMounted(() => {
  void refreshAll().catch((error) => {
    errorMessage.value = error instanceof Error ? error.message : '交接清单加载失败'
  })
})
</script>

<style scoped>
.handoff-panel { margin: 0 0 20px; padding: 18px; border: 1px solid #d9e2ec; border-radius: 14px; background: #f8fafc; }
.handoff-head { display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; }
.handoff-head h3, .handoff-columns h4 { margin: 0 0 6px; }
.handoff-head p { margin: 0; color: #52616f; font-size: 13px; }
.handoff-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; margin-top: 14px; }
.field { display: flex; flex-direction: column; gap: 6px; font-size: 13px; color: #334e68; }
.field input, .field textarea { border: 1px solid #cbd5e1; border-radius: 8px; padding: 8px 10px; font: inherit; background: white; }
.field.grow { grid-column: span 1; }
.field.narrow { max-width: 140px; }
.actions { display: flex; flex-wrap: wrap; gap: 10px; margin: 12px 0; }
.actions.compact { margin: 10px 0 0; }
.btn[disabled] { opacity: .55; cursor: not-allowed; }
.notice { padding: 10px 12px; border-radius: 8px; background: #eef6ff; color: #1e4e8c; font-size: 13px; }
.notice.ok { background: #e8f8ef; color: #176b3a; }
.progress-box, .window-result { margin: 12px 0; padding: 12px; border: 1px solid #d9e2ec; border-radius: 10px; background: white; }
.progress-head { display: flex; justify-content: space-between; gap: 12px; }
.backfill-bar { display: flex; gap: 12px; align-items: end; margin: 14px 0; }
.backfill-bar .field { flex: 1; }
.key-table-wrap { max-height: 240px; overflow: auto; border: 1px solid #e2e8f0; border-radius: 8px; }
.key-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.key-table th, .key-table td { padding: 6px 8px; border-bottom: 1px solid #edf2f7; text-align: left; vertical-align: top; }
.handoff-columns { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-top: 14px; }
.handoff-columns ul { margin: 0; padding-left: 18px; font-size: 13px; }
.handoff-columns li { margin-bottom: 10px; }
.conflict-item { color: #7a2e0e; }
.error-text { color: #c53030; font-size: 13px; }
.info-text { color: #2f855a; font-size: 13px; }
@media (max-width: 900px) { .handoff-grid, .handoff-columns { grid-template-columns: 1fr; } .backfill-bar { flex-direction: column; align-items: stretch; } }
</style>
