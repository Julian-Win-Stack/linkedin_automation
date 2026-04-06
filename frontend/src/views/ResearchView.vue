<script setup lang="ts">
import { computed, onBeforeUnmount, ref } from "vue";
import { isSelectedUser } from "../../../src/shared/selectedUser";
import type { SelectedUser } from "../../../src/shared/selectedUser";

const API_URL = import.meta.env.VITE_API_URL?.trim() || "";
const SELECTED_USER_STORAGE_KEY = "selected-user";
const QUEUE_POLL_INTERVAL_MS = 2000;

type QueueItemStatus = "queued" | "running" | "done" | "error" | "cancelled";
type QueueItem = {
  queueItemId: string;
  queueOrder: number;
  queueLabel: string;
  status: QueueItemStatus;
  createdAtMs: number;
  updatedAtMs: number;
  startedAtMs: number | null;
  completedAtMs: number | null;
  summary: Record<string, number> | null;
  warnings: string[];
  skippedCompanies: string[];
  rejectedCompanies: string[];
  rejectedReason: string | null;
  errorMessage: string | null;
  progressMessage: string | null;
  currentRow: number | null;
  totalRows: number | null;
  hasCsv: boolean;
  hasPdf: boolean;
};

type VisibleQueueItem = QueueItem & { displayQueueLabel: string };

const fileInput = ref<HTMLInputElement | null>(null);
const selectedFile = ref<File | null>(null);
const isDragActive = ref(false);
const isSubmitting = ref(false);
const error = ref<string | null>(null);
const queueItems = ref<QueueItem[]>([]);
const selectedUser = ref<SelectedUser | null>(null);
const queuePollIntervalId = ref<number | null>(null);
const weeklySuccessTotals = ref({ linkedin: 0, email: 0 });

const canAddToQueue = computed(() => !isSubmitting.value && !!selectedFile.value && !!selectedUser.value);
const selectedUserLabel = computed(() => {
  if (!selectedUser.value) return null;
  if (selectedUser.value === "raihan") return "Raihan";
  if (selectedUser.value === "cherry") return "Cherry";
  return "Julian";
});

const activeQueueCount = computed(() =>
  queueItems.value.filter((item) => item.status === "queued" || item.status === "running").length
);

const clearableFinishedQueueCount = computed(() =>
  queueItems.value.filter((item) => item.status === "done" || item.status === "error").length
);

function toQueueLabel(order: number): string {
  const mod100 = order % 100;
  if (mod100 >= 11 && mod100 <= 13) {
    return `${order}th queue`;
  }
  const mod10 = order % 10;
  if (mod10 === 1) return `${order}st queue`;
  if (mod10 === 2) return `${order}nd queue`;
  if (mod10 === 3) return `${order}rd queue`;
  return `${order}th queue`;
}

const visibleQueueItems = computed<VisibleQueueItem[]>(() =>
  {
    return queueItems.value
      .filter((item) => {
        if (item.status === "cancelled") {
          return false;
        }
        return true;
      })
      .map((item, index) => ({
        ...item,
        displayQueueLabel: toQueueLabel(index + 1),
      }));
  }
);

const storedUser = localStorage.getItem(SELECTED_USER_STORAGE_KEY);
if (isSelectedUser(storedUser)) {
  selectedUser.value = storedUser;
  void refreshQueueItems();
  void refreshWeeklySuccessTotals();
  startQueuePolling();
}

function statusBadgeClass(status: QueueItemStatus): string {
  if (status === "running") return "border-indigo-400/40 bg-indigo-500/15 text-indigo-200";
  if (status === "queued") return "border-zinc-500/40 bg-zinc-600/20 text-zinc-200";
  if (status === "done") return "border-emerald-400/40 bg-emerald-500/15 text-emerald-200";
  if (status === "cancelled") return "border-amber-400/40 bg-amber-500/15 text-amber-200";
  return "border-rose-400/40 bg-rose-500/15 text-rose-200";
}

function statusLabel(status: QueueItemStatus): string {
  if (status === "queued") return "Queued";
  if (status === "running") return "Running";
  if (status === "done") return "Completed";
  if (status === "cancelled") return "Cancelled";
  return "Error";
}

function toFriendlyStageMessage(message: string): string {
  const lowered = message.toLowerCase();
  if (lowered.includes("starting engineer and sre pre-filter")) {
    return "Preparing the company checks.";
  }
  if (lowered.includes("engineer/sre pre-filter")) {
    return "Checking if each company is a good fit.";
  }
  if (lowered.includes("observability research")) {
    return "Researching each company's observability setup.";
  }
  if (lowered.includes("apollo stage")) {
    return "Finding and selecting the best contacts.";
  }
  if (lowered.includes("missing email search update")) {
    return "Looking up missing work emails.";
  }
  if (lowered.includes("missing") && lowered.includes("email")) {
    return "Looking up missing work emails.";
  }
  return message;
}

function progressLabel(item: QueueItem): string | null {
  if (item.status === "queued") {
    return "Waiting in queue.";
  }
  if (item.status === "running") {
    const base = item.progressMessage
      ? toFriendlyStageMessage(item.progressMessage)
      : "Processing this queue item.";
    if (item.currentRow !== null && item.totalRows !== null) {
      return `${base} (${item.currentRow} of ${item.totalRows} companies)`;
    }
    return base;
  }
  if (item.status === "done") {
    return "Completed. Results are ready to download.";
  }
  if (item.status === "cancelled") {
    return "Stopped by user.";
  }
  if (item.status === "error") {
    return item.errorMessage ? `Stopped due to an error: ${item.errorMessage}` : "Stopped due to an error.";
  }
  return null;
}

function setSelectedUser(user: SelectedUser): void {
  selectedUser.value = user;
  localStorage.setItem(SELECTED_USER_STORAGE_KEY, user);
  error.value = null;
  queueItems.value = [];
  void refreshQueueItems();
  void refreshWeeklySuccessTotals();
  startQueuePolling();
}

async function logoutSelectedUser(): Promise<void> {
  selectedUser.value = null;
  localStorage.removeItem(SELECTED_USER_STORAGE_KEY);
  queueItems.value = [];
  selectedFile.value = null;
  error.value = null;
  weeklySuccessTotals.value = { linkedin: 0, email: 0 };
  clearQueuePolling();
}

function getCurrentWeekStartMsLocal(nowMs = Date.now()): number {
  const now = new Date(nowMs);
  now.setHours(0, 0, 0, 0);
  const dayOfWeek = now.getDay();
  const daysSinceSaturday = (dayOfWeek + 1) % 7;
  return now.getTime() - daysSinceSaturday * 24 * 60 * 60 * 1000;
}

async function refreshWeeklySuccessTotals(): Promise<void> {
  if (!selectedUser.value) {
    weeklySuccessTotals.value = { linkedin: 0, email: 0 };
    return;
  }
  try {
    const query = new URLSearchParams({
      selectedUser: selectedUser.value,
      weekStartMs: String(getCurrentWeekStartMsLocal()),
    });
    const response = await fetch(`${API_URL}/weekly-counts?${query.toString()}`);
    if (!response.ok) {
      throw new Error(`Failed to fetch weekly counts (${response.status})`);
    }
    const payload = (await response.json()) as { linkedinCount?: number; emailCount?: number };
    weeklySuccessTotals.value = {
      linkedin: Number(payload.linkedinCount ?? 0),
      email: Number(payload.emailCount ?? 0),
    };
  } catch {
    weeklySuccessTotals.value = { linkedin: 0, email: 0 };
  }
}

async function refreshQueueItems(): Promise<void> {
  if (!selectedUser.value) return;
  try {
    const query = new URLSearchParams({ selectedUser: selectedUser.value });
    const response = await fetch(`${API_URL}/queue?${query.toString()}`);
    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new Error(text || `Failed to fetch queue (${response.status})`);
    }
    const payload = (await response.json()) as { items?: QueueItem[] };
    queueItems.value = payload.items ?? [];
    const hasRecentlyFinished = queueItems.value.some((item) => item.status === "done");
    if (hasRecentlyFinished) {
      void refreshWeeklySuccessTotals();
    }
  } catch (fetchError) {
    error.value = fetchError instanceof Error ? fetchError.message : String(fetchError);
  }
}

function clearQueuePolling(): void {
  if (queuePollIntervalId.value !== null) {
    clearInterval(queuePollIntervalId.value);
    queuePollIntervalId.value = null;
  }
}

function startQueuePolling(): void {
  clearQueuePolling();
  if (!selectedUser.value) return;
  queuePollIntervalId.value = window.setInterval(() => {
    void refreshQueueItems();
  }, QUEUE_POLL_INTERVAL_MS);
}

onBeforeUnmount(() => {
  clearQueuePolling();
});

function setSelectedCsvFile(file: File | null): void {
  if (file && !file.name.endsWith(".csv")) {
    error.value = "Please select a .csv file.";
    return;
  }
  error.value = null;
  selectedFile.value = file;
}

function onFileChange(event: Event): void {
  const target = event.target as HTMLInputElement;
  setSelectedCsvFile(target.files?.[0] ?? null);
}

function openFilePicker(): void {
  if (!selectedUser.value || isSubmitting.value) return;
  fileInput.value?.click();
}

function onDropZoneDragOver(event: DragEvent): void {
  if (!selectedUser.value || isSubmitting.value) return;
  event.preventDefault();
  isDragActive.value = true;
}

function onDropZoneDragLeave(event: DragEvent): void {
  event.preventDefault();
  isDragActive.value = false;
}

function onDropZoneDrop(event: DragEvent): void {
  if (!selectedUser.value || isSubmitting.value) return;
  event.preventDefault();
  isDragActive.value = false;
  setSelectedCsvFile(event.dataTransfer?.files?.[0] ?? null);
}

function onDropZoneKeydown(event: KeyboardEvent): void {
  if (event.key === "Enter" || event.key === " ") {
    event.preventDefault();
    openFilePicker();
  }
}

async function addToQueue(): Promise<void> {
  if (!selectedUser.value) {
    error.value = "Please select a user before using the app.";
    return;
  }
  if (!selectedFile.value) {
    error.value = "Please select a CSV file.";
    return;
  }

  error.value = null;
  isSubmitting.value = true;
  const formData = new FormData();
  formData.append("csv", selectedFile.value);
  formData.append("selectedUser", selectedUser.value);
  formData.append("weekStartMs", String(getCurrentWeekStartMsLocal()));

  try {
    const response = await fetch(`${API_URL}/research`, {
      method: "POST",
      body: formData,
    });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({ error: response.statusText }));
      throw new Error(payload.error ?? "Failed to enqueue CSV.");
    }
    selectedFile.value = null;
    if (fileInput.value) {
      fileInput.value.value = "";
    }
    await refreshQueueItems();
  } catch (queueError) {
    error.value = queueError instanceof Error ? queueError.message : String(queueError);
  } finally {
    isSubmitting.value = false;
  }
}

async function cancelQueueItem(queueItemId: string): Promise<void> {
  try {
    const response = await fetch(`${API_URL}/queue/${queueItemId}/cancel`, { method: "POST" });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({ error: response.statusText }));
      throw new Error(payload.error ?? "Failed to cancel queue item.");
    }
    await refreshQueueItems();
  } catch (cancelError) {
    error.value = cancelError instanceof Error ? cancelError.message : String(cancelError);
  }
}

async function cancelAllQueueItems(): Promise<void> {
  if (!selectedUser.value || isSubmitting.value) {
    return;
  }
  try {
    const response = await fetch(`${API_URL}/queue/cancel-all`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ selectedUser: selectedUser.value }),
    });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({ error: response.statusText }));
      throw new Error(payload.error ?? "Failed to cancel queue.");
    }
    await refreshQueueItems();
    await refreshWeeklySuccessTotals();
  } catch (cancelError) {
    error.value = cancelError instanceof Error ? cancelError.message : String(cancelError);
  }
}

async function clearFinishedQueueItems(): Promise<void> {
  if (!selectedUser.value || clearableFinishedQueueCount.value === 0 || isSubmitting.value) {
    return;
  }
  try {
    const response = await fetch(`${API_URL}/queue/clear-finished`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ selectedUser: selectedUser.value }),
    });
    if (!response.ok) {
      const payload = await response.json().catch(() => ({ error: response.statusText }));
      throw new Error(payload.error ?? "Failed to clear finished queues.");
    }
    await refreshQueueItems();
  } catch (clearError) {
    error.value = clearError instanceof Error ? clearError.message : String(clearError);
  }
}
</script>

<template>
  <div class="relative min-h-screen bg-[radial-gradient(circle_at_top,#111a2a_0%,#090d16_45%,#05070c_100%)] px-4 text-zinc-200">
    <div v-if="selectedUserLabel" class="absolute right-4 top-4 flex flex-col items-end gap-1.5">
      <div class="rounded-full border border-indigo-400/40 bg-[#121a2c]/90 px-3 py-1.5 text-xs font-semibold tracking-wide text-indigo-200 shadow-[0_8px_24px_rgba(0,0,0,0.35)]">
        User: {{ selectedUserLabel }}
      </div>
      <div
        class="w-full rounded-xl border border-indigo-400/25 bg-[#10192b]/90 px-3 py-2 text-center text-[11px] text-indigo-100 shadow-[0_8px_20px_rgba(0,0,0,0.28)]"
      >
        <p class="font-medium">LinkedIn count: {{ weeklySuccessTotals.linkedin }}</p>
        <p class="mt-1 font-medium">Email count: {{ weeklySuccessTotals.email }}</p>
      </div>
      <button
        class="rounded-full border border-zinc-500/50 bg-[#0f1728]/90 px-2.5 py-1 text-[11px] font-medium tracking-wide text-zinc-300 transition hover:border-zinc-400/70 hover:bg-[#16233a]"
        @click="logoutSelectedUser"
      >
        Log out
      </button>
    </div>

    <div class="mx-auto flex min-h-screen w-full max-w-5xl items-start py-12">
      <div class="w-full space-y-4">
        <h1 class="text-2xl font-semibold tracking-tight text-zinc-100">Start Outbound</h1>

        <div
          class="rounded-xl border border-[#1d2537] bg-[#0d1320]/90 p-4 shadow-[0_18px_50px_rgba(0,0,0,0.45)] space-y-3"
          :class="!selectedUser ? 'pointer-events-none opacity-40 select-none blur-[1px]' : ''"
        >
          <div
            class="rounded-lg border border-dashed bg-[#0a1220]/50 p-3 transition"
            :class="
              isDragActive
                ? 'border-indigo-400/70 bg-indigo-500/10'
                : selectedFile
                  ? 'border-zinc-500/90 bg-zinc-900/40'
                  : 'border-zinc-600/80 hover:border-zinc-500/90'
            "
            role="button"
            tabindex="0"
            @click="openFilePicker"
            @keydown="onDropZoneKeydown"
            @dragover="onDropZoneDragOver"
            @dragleave="onDropZoneDragLeave"
            @drop="onDropZoneDrop"
          >
            <span class="sr-only">Choose csv</span>
            <input
              ref="fileInput"
              type="file"
              accept=".csv"
              class="sr-only"
              :disabled="!selectedUser || isSubmitting"
              @change="onFileChange"
            />
            <template v-if="selectedFile">
              <p class="truncate text-sm font-medium text-zinc-200">{{ selectedFile.name }}</p>
              <p class="mt-1 text-xs text-zinc-500">Ready to enqueue</p>
            </template>
            <template v-else>
              <p class="text-sm font-medium text-zinc-200">Drop CSV here or click to upload</p>
              <p class="mt-1 text-xs text-zinc-500">You can enqueue up to 10 active files</p>
            </template>
          </div>

          <div class="flex items-center justify-between gap-3">
            <p class="text-xs text-zinc-400">Active queue items: {{ activeQueueCount }}/10</p>
            <div class="flex items-center gap-2">
              <button
                class="rounded-md border border-rose-400/30 bg-rose-500/10 px-4 py-2 text-sm font-semibold text-rose-100 transition hover:border-rose-300/50 hover:bg-rose-500/20 disabled:cursor-not-allowed disabled:opacity-40"
                :disabled="isSubmitting || activeQueueCount === 0"
                @click="cancelAllQueueItems"
              >
                Cancel all
              </button>
              <button
                class="rounded-md border border-emerald-400/30 bg-emerald-500/10 px-4 py-2 text-sm font-semibold text-emerald-100 transition hover:border-emerald-300/50 hover:bg-emerald-500/20 disabled:cursor-not-allowed disabled:opacity-40"
                :disabled="clearableFinishedQueueCount === 0"
                @click="clearFinishedQueueItems"
              >
                Clear finished queues
              </button>
              <button
                class="rounded-md bg-indigo-700 px-4 py-2 text-sm font-semibold text-white transition hover:bg-indigo-600 disabled:opacity-40"
                :disabled="!canAddToQueue"
                @click="addToQueue"
              >
                {{ isSubmitting ? "Adding..." : "Add to Queue" }}
              </button>
            </div>
          </div>
          <p v-if="error" class="text-sm text-red-400">{{ error }}</p>
        </div>

        <div class="rounded-xl border border-[#1d2537] bg-[#0d1320]/90 p-4 shadow-[0_18px_50px_rgba(0,0,0,0.45)]">
          <div class="mb-3 flex items-center justify-between">
            <h2 class="text-sm font-semibold tracking-tight text-zinc-100">Queue Timeline</h2>
          </div>

          <div v-if="visibleQueueItems.length === 0" class="rounded-lg border border-zinc-800 bg-[#0a1220]/40 p-4 text-sm text-zinc-400">
            No queue items yet. Upload a CSV and click <strong>Add to Queue</strong>.
          </div>

          <div v-else class="space-y-3">
            <div
              v-for="item in visibleQueueItems"
              :key="item.queueItemId"
              class="rounded-lg border border-zinc-800 bg-[#0a1220]/55 p-3"
            >
              <div class="flex flex-wrap items-center justify-between gap-2">
                <div class="flex items-center gap-2">
                  <p class="text-sm font-semibold text-zinc-100">{{ item.displayQueueLabel }}</p>
                  <span class="rounded-full border px-2 py-0.5 text-[11px] font-medium" :class="statusBadgeClass(item.status)">
                    {{ statusLabel(item.status) }}
                  </span>
                </div>
                <p class="text-[11px] text-zinc-500">Created {{ new Date(item.createdAtMs).toLocaleString() }}</p>
              </div>

              <div v-if="progressLabel(item)" class="mt-2 flex items-center gap-2 text-xs text-indigo-200">
                <p>{{ progressLabel(item) }}</p>
                <span v-if="item.status === 'running'" class="relative inline-flex h-2.5 w-2.5" aria-label="Running">
                  <span class="absolute inline-flex h-full w-full animate-ping rounded-full bg-emerald-300/80"></span>
                  <span class="relative inline-flex h-2.5 w-2.5 rounded-full bg-emerald-300"></span>
                </span>
              </div>
              <p v-if="item.errorMessage" class="mt-2 text-xs text-rose-300">{{ item.errorMessage }}</p>

              <div v-if="item.summary" class="mt-2 grid grid-cols-2 gap-2 text-xs text-zinc-300">
                <p>LinkedIn pushed: {{ item.summary.totalLinkedinCampaignSuccessful ?? 0 }}</p>
                <p>Email pushed: {{ item.summary.totalEmailCampaignSuccessful ?? 0 }}</p>
                <p>LinkedIn failed: {{ item.summary.totalLinkedinCampaignFailed ?? 0 }}</p>
                <p>Email failed: {{ item.summary.totalEmailCampaignFailed ?? 0 }}</p>
              </div>

              <div class="mt-3 flex flex-wrap gap-2">
                <a
                  v-if="item.hasCsv"
                  :href="`${API_URL}/queue/${item.queueItemId}/csv`"
                  class="inline-flex items-center rounded-md bg-emerald-700 px-3 py-1.5 text-xs font-semibold text-white hover:bg-emerald-600"
                >
                  Download CSV
                </a>
                <a
                  v-if="item.hasPdf"
                  :href="`${API_URL}/queue/${item.queueItemId}/pdf`"
                  class="inline-flex items-center rounded-md bg-indigo-700 px-3 py-1.5 text-xs font-semibold text-white hover:bg-indigo-600"
                >
                  Download PDF
                </a>
                <button
                  v-if="item.status === 'queued' || item.status === 'running'"
                  class="inline-flex items-center rounded-md border border-zinc-700 bg-zinc-900/60 px-3 py-1.5 text-xs font-semibold text-zinc-300 hover:bg-zinc-800/70"
                  @click="cancelQueueItem(item.queueItemId)"
                >
                  Cancel
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <div
      v-if="!selectedUser"
      class="absolute inset-0 z-20 flex items-center justify-center bg-[#05070c]/65 backdrop-blur-[2px]"
    >
      <div class="w-full max-w-sm rounded-2xl border border-[#2a3550] bg-[#0e1728]/95 p-5 shadow-[0_22px_60px_rgba(0,0,0,0.5)]">
        <h2 class="text-base font-semibold text-zinc-100">Select User</h2>
        <p class="mt-1 text-sm text-zinc-400">
          Choose a user to open their personal queue worker.
        </p>
        <div class="mt-4 grid grid-cols-3 gap-2">
          <button
            class="rounded-lg border border-indigo-400/30 bg-indigo-500/10 px-3 py-2 text-sm font-semibold text-indigo-100 transition hover:bg-indigo-500/20"
            @click="setSelectedUser('raihan')"
          >
            Raihan
          </button>
          <button
            class="rounded-lg border border-fuchsia-400/30 bg-fuchsia-500/10 px-3 py-2 text-sm font-semibold text-fuchsia-100 transition hover:bg-fuchsia-500/20"
            @click="setSelectedUser('cherry')"
          >
            Cherry
          </button>
          <button
            class="rounded-lg border border-cyan-400/30 bg-cyan-500/10 px-3 py-2 text-sm font-semibold text-cyan-100 transition hover:bg-cyan-500/20"
            @click="setSelectedUser('julian')"
          >
            Julian
          </button>
        </div>
      </div>
    </div>
  </div>
</template>
