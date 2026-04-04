<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import { isSelectedUser } from "../../../src/shared/selectedUser";
import type { SelectedUser } from "../../../src/shared/selectedUser";

const API_URL = import.meta.env.VITE_API_URL?.trim() || "";
const SELECTED_USER_STORAGE_KEY = "selected-user";

const fileInput = ref<HTMLInputElement | null>(null);
const selectedFile = ref<File | null>(null);
const isDragActive = ref(false);
const isLoading = ref(false);
const error = ref<string | null>(null);
const warnings = ref<string[]>([]);
const progressMessage = ref<string | null>(null);
const resultBlob = ref<Blob | null>(null);
const downloadUrl = ref<string | null>(null);
const summary = ref<Record<string, number> | null>(null);
const skippedCompanies = ref<string[]>([]);
const rejectedCompanies = ref<string[]>([]);
const rejectedReason = ref<string | null>(null);
const abortControllerRef = ref<AbortController | null>(null);
const pollingIntervalId = ref<number | null>(null);
const pollingSwitchTimeoutId = ref<number | null>(null);
const selectedUser = ref<SelectedUser | null>(null);
const currentJobId = ref<string | null>(null);
const completedJobId = ref<string | null>(null);
const weeklySuccessTotals = ref({ linkedin: 0, email: 0 });

const canRun = computed(() => !isLoading.value && !!selectedFile.value && !!selectedUser.value);
const workflowSignal = "Upload → Qualify → Outreach";
const selectedUserLabel = computed(() => {
  if (!selectedUser.value) {
    return null;
  }
  if (selectedUser.value === "raihan") {
    return "Raihan";
  }
  if (selectedUser.value === "cherry") {
    return "Cherry";
  }
  return "Julian";
});

const storedUser = localStorage.getItem(SELECTED_USER_STORAGE_KEY);
if (isSelectedUser(storedUser)) {
  selectedUser.value = storedUser;
  void refreshWeeklySuccessTotals();
}

function setSelectedUser(user: SelectedUser): void {
  selectedUser.value = user;
  localStorage.setItem(SELECTED_USER_STORAGE_KEY, user);
  void refreshWeeklySuccessTotals();
}

async function logoutSelectedUser(): Promise<void> {
  await cancelAndReset();
  selectedUser.value = null;
  localStorage.removeItem(SELECTED_USER_STORAGE_KEY);
  weeklySuccessTotals.value = { linkedin: 0, email: 0 };
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
    const response = await fetch(`${API_URL}/weekly-counts?${query.toString()}`, {
      method: "GET",
    });
    if (!response.ok) {
      const text = await response.text().catch(() => "");
      throw new Error(text || `Failed to fetch weekly counts (${response.status}).`);
    }
    const payload = (await response.json()) as {
      linkedinCount?: number;
      emailCount?: number;
    };
    weeklySuccessTotals.value = {
      linkedin: Number(payload.linkedinCount ?? 0),
      email: Number(payload.emailCount ?? 0),
    };
  } catch {
    // Weekly counters are secondary UI signals; keep the page usable on failure.
    weeklySuccessTotals.value = { linkedin: 0, email: 0 };
  }
}

watch(resultBlob, (blob) => {
  if (downloadUrl.value) {
    URL.revokeObjectURL(downloadUrl.value);
    downloadUrl.value = null;
  }
  if (blob) {
    downloadUrl.value = URL.createObjectURL(blob);
  }
});

onBeforeUnmount(() => {
  if (downloadUrl.value) {
    URL.revokeObjectURL(downloadUrl.value);
  }
});

function resetState(): void {
  error.value = null;
  warnings.value = [];
  progressMessage.value = null;
  resultBlob.value = null;
  summary.value = null;
  skippedCompanies.value = [];
  rejectedCompanies.value = [];
  rejectedReason.value = null;
  completedJobId.value = null;
}

function setSelectedCsvFile(file: File | null): void {
  if (file && !file.name.endsWith(".csv")) {
    error.value = "Please select a .csv file.";
    return;
  }
  selectedFile.value = file;
  resetState();
}

function onFileChange(event: Event): void {
  const target = event.target as HTMLInputElement;
  const file = target.files?.[0] ?? null;
  setSelectedCsvFile(file);
}

function openFilePicker(): void {
  if (!selectedUser.value || isLoading.value) {
    return;
  }
  fileInput.value?.click();
}

function onDropZoneDragOver(event: DragEvent): void {
  if (!selectedUser.value || isLoading.value) {
    return;
  }
  event.preventDefault();
  isDragActive.value = true;
}

function onDropZoneDragLeave(event: DragEvent): void {
  event.preventDefault();
  isDragActive.value = false;
}

function onDropZoneDrop(event: DragEvent): void {
  if (!selectedUser.value || isLoading.value) {
    return;
  }
  event.preventDefault();
  isDragActive.value = false;
  const droppedFile = event.dataTransfer?.files?.[0] ?? null;
  setSelectedCsvFile(droppedFile);
}

function onDropZoneKeydown(event: KeyboardEvent): void {
  if (event.key === "Enter" || event.key === " ") {
    event.preventDefault();
    openFilePicker();
  }
}

function clearPolling(): void {
  if (pollingIntervalId.value !== null) {
    clearInterval(pollingIntervalId.value);
    pollingIntervalId.value = null;
  }
  if (pollingSwitchTimeoutId.value !== null) {
    clearTimeout(pollingSwitchTimeoutId.value);
    pollingSwitchTimeoutId.value = null;
  }
}

async function runResearch(): Promise<void> {
  if (!selectedUser.value) {
    error.value = "Please select a user before using the app.";
    return;
  }
  if (!selectedFile.value) {
    error.value = "Please select a CSV file.";
    return;
  }

  resetState();
  isLoading.value = true;
  abortControllerRef.value = new AbortController();

  const formData = new FormData();
  formData.append("csv", selectedFile.value);
  formData.append("selectedUser", selectedUser.value);
  formData.append("weekStartMs", String(getCurrentWeekStartMsLocal()));

  try {
    const startResponse = await fetch(`${API_URL}/research`, {
      method: "POST",
      body: formData,
      signal: abortControllerRef.value.signal,
    });
    if (!startResponse.ok) {
      const payload = await startResponse.json().catch(() => ({ error: startResponse.statusText }));
      throw new Error(payload.error ?? "Failed to start research job.");
    }

    const startPayload = (await startResponse.json()) as { jobId?: string };
    if (!startPayload.jobId) {
      throw new Error("Job ID missing in start response.");
    }
    currentJobId.value = startPayload.jobId;

    await pollJob(startPayload.jobId);
  } catch (unknownError) {
    if ((unknownError as Error).name !== "AbortError") {
      error.value = unknownError instanceof Error ? unknownError.message : String(unknownError);
    }
  } finally {
    clearPolling();
    isLoading.value = false;
    progressMessage.value = null;
    currentJobId.value = null;
  }
}

async function pollJob(jobId: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    let pollingInFlight = false;
    const FAST_INTERVAL_MS = 1000;
    const SLOW_INTERVAL_MS = 3000;
    const FAST_PHASE_DURATION_MS = 30_000;

    const stop = () => {
      clearPolling();
      resolve();
    };

    const pollOnce = async (): Promise<"continue" | "stop"> => {
      const response = await fetch(`${API_URL}/status/${jobId}`, {
        method: "GET",
        signal: abortControllerRef.value?.signal,
      });

      if (!response.ok) {
        const text = await response.text().catch(() => "");
        throw new Error(text || `Polling failed (${response.status}).`);
      }

      const payload = (await response.json()) as
        | { status: "processing" | "pending"; message?: string; warnings?: string[] }
        | {
            status: "done";
            csv: string;
            warnings?: string[];
            skippedCompanies?: string[];
            rejectedCompanies?: string[];
            rejectedReason?: string;
            summary?: Record<string, number>;
          }
        | { status: "error"; error: string };

      if (payload.status === "processing" || payload.status === "pending") {
        progressMessage.value = payload.message ?? "Processing...";
        warnings.value = Array.from(new Set([...(warnings.value ?? []), ...(payload.warnings ?? [])]));
        return "continue";
      }

      if (payload.status === "error") {
        error.value = payload.error;
        return "stop";
      }

      const donePayload = payload as {
        status: "done";
        csv: string;
        warnings?: string[];
        skippedCompanies?: string[];
        rejectedCompanies?: string[];
        rejectedReason?: string;
        summary?: Record<string, number>;
      };

      const binary = atob(donePayload.csv);
      const bytes = new Uint8Array(binary.length);
      for (let i = 0; i < binary.length; i += 1) {
        bytes[i] = binary.charCodeAt(i);
      }
      resultBlob.value = new Blob([bytes], { type: "text/csv" });
      warnings.value = donePayload.warnings ?? [];
      skippedCompanies.value = donePayload.skippedCompanies ?? [];
      rejectedCompanies.value = donePayload.rejectedCompanies ?? [];
      rejectedReason.value = donePayload.rejectedReason ?? null;
      summary.value = donePayload.summary ?? null;
      completedJobId.value = jobId;
      await refreshWeeklySuccessTotals();
      return "stop";
    };

    const startInterval = (intervalMs: number) => {
      if (pollingIntervalId.value !== null) {
        clearInterval(pollingIntervalId.value);
      }
      pollingIntervalId.value = window.setInterval(() => {
        if (pollingInFlight) {
          return;
        }
        pollingInFlight = true;
        void pollOnce()
          .then((state) => {
            if (state === "stop") {
              stop();
            }
          })
          .catch((pollError) => reject(pollError))
          .finally(() => {
            pollingInFlight = false;
          });
      }, intervalMs);
    };

    startInterval(FAST_INTERVAL_MS);
    pollingSwitchTimeoutId.value = window.setTimeout(() => {
      startInterval(SLOW_INTERVAL_MS);
      pollingSwitchTimeoutId.value = null;
    }, FAST_PHASE_DURATION_MS);

    void pollOnce()
      .then((state) => {
        if (state === "stop") {
          stop();
        }
      })
      .catch((pollError) => reject(pollError));
  });
}

async function cancelAndReset(): Promise<void> {
  const activeJobId = currentJobId.value;
  if (activeJobId) {
    try {
      await fetch(`${API_URL}/cancel/${activeJobId}`, { method: "POST" });
    } catch {
      // Best effort cancel; local reset still runs.
    }
  }
  abortControllerRef.value?.abort();
  clearPolling();
  isLoading.value = false;
  progressMessage.value = null;
  currentJobId.value = null;
  selectedFile.value = null;
  resetState();
  if (fileInput.value) {
    fileInput.value.value = "";
  }
}
</script>

<template>
  <div
    class="relative min-h-screen bg-[radial-gradient(circle_at_top,#111a2a_0%,#090d16_45%,#05070c_100%)] px-4 text-zinc-200"
  >
    <div v-if="selectedUserLabel" class="absolute right-4 top-4 flex flex-col items-end gap-1.5">
      <div
        class="rounded-full border border-indigo-400/40 bg-[#121a2c]/90 px-3 py-1.5 text-xs font-semibold tracking-wide text-indigo-200 shadow-[0_8px_24px_rgba(0,0,0,0.35)]"
      >
        User: {{ selectedUserLabel }}
      </div>
      <div class="w-full rounded-xl border border-indigo-400/25 bg-[#10192b]/90 px-3 py-2 text-[11px] text-indigo-100 shadow-[0_8px_20px_rgba(0,0,0,0.28)]">
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

    <div class="mx-auto flex min-h-screen w-full max-w-2xl items-center py-12">
      <div class="w-full space-y-3">
        <h1 class="text-2xl font-semibold tracking-tight text-zinc-100">Start outbound</h1>

        <div
          class="rounded-xl border border-[#1d2537] bg-[#0d1320]/90 p-4 shadow-[0_18px_50px_rgba(0,0,0,0.45)] space-y-2.5"
          :class="!selectedUser ? 'pointer-events-none opacity-40 select-none blur-[1px]' : ''"
        >
          <p class="text-[11px] font-medium tracking-wide text-zinc-500">{{ workflowSignal }}</p>

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
              :disabled="!selectedUser || isLoading"
              @change="onFileChange"
            />
            <template v-if="selectedFile">
              <p class="truncate text-sm font-medium text-zinc-200">{{ selectedFile.name }}</p>
              <p class="mt-1 text-xs text-zinc-500">CSV only</p>
            </template>
            <template v-else>
              <p class="text-sm font-medium text-zinc-200">Drop CSV here or click to upload</p>
              <p class="mt-1 text-xs text-zinc-500">CSV only</p>
            </template>
          </div>

          <div class="grid grid-cols-2 gap-2">
            <button
              class="rounded-md bg-indigo-700 px-3 py-2 text-sm font-semibold text-white transition hover:bg-indigo-600 disabled:opacity-40"
              :disabled="!canRun"
              @click="runResearch"
            >
              Start Outbound
            </button>
            <button
              class="rounded-md border border-zinc-700 bg-zinc-900/60 px-3 py-2 text-sm font-semibold text-zinc-300 transition hover:bg-zinc-800/70 disabled:opacity-40"
              :disabled="!selectedUser"
              @click="cancelAndReset"
            >
              Cancel
            </button>
          </div>

          <div
            v-if="isLoading"
            class="rounded-lg border border-[#1f2a44] bg-[#0a1220] px-3 py-2.5 shadow-[0_8px_24px_rgba(0,0,0,0.28)]"
          >
            <div class="flex items-center gap-3 text-zinc-300">
              <span
                class="inline-block h-3.5 w-3.5 animate-spin rounded-full border-2 border-indigo-400/40 border-t-indigo-400"
                aria-hidden="true"
              />
              <p class="text-sm font-medium tracking-tight text-zinc-300">Building pipeline...</p>
            </div>
            <p class="mt-2 text-sm font-normal tracking-tight text-zinc-400">
              {{ progressMessage ?? "Processing..." }}
            </p>
          </div>
          <p v-if="error" class="text-sm text-red-400">{{ error }}</p>

          <div v-if="warnings.length > 0" class="rounded-md border border-amber-800 bg-amber-950/40 p-3">
            <p class="text-sm font-medium text-amber-300 mb-2">Warnings</p>
            <p v-for="(warning, index) in warnings" :key="index" class="text-xs text-amber-200">
              {{ warning }}
            </p>
          </div>

          <div v-if="summary" class="rounded-md border border-zinc-700 bg-zinc-900/50 p-3 text-sm space-y-1">
            <p v-if="skippedCompanies.length > 0" class="text-red-300">
              {{
                `Skipped ${skippedCompanies.length} because both Website URL and Apollo Account Id were missing.`
              }}
            </p>
            <ul v-if="skippedCompanies.length > 0" class="list-disc pl-5 text-xs text-red-300 space-y-1">
              <li v-for="company in skippedCompanies" :key="company">
                {{ company }}
              </li>
            </ul>
            <p>
              <strong>LinkedIn campaign pushed:</strong> {{ summary.totalLinkedinCampaignSuccessful ?? 0 }}
            </p>
            <p>
              <strong>Failed to push to LinkedIn campaigns:</strong> {{ summary.totalLinkedinCampaignFailed ?? 0 }}
            </p>
            <p>
              <strong>Email campaign pushed:</strong> {{ summary.totalEmailCampaignSuccessful ?? 0 }}
            </p>
            <p>
              <strong>Failed to push to email campaigns:</strong> {{ summary.totalEmailCampaignFailed ?? 0 }}
            </p>
          </div>

          <div
            v-if="rejectedCompanies.length > 0"
            class="rounded-md border border-zinc-700 bg-zinc-900/50 p-3 text-sm space-y-2"
          >
            <p class="font-medium">Rejected Companies</p>
            <ul class="list-disc pl-5 text-xs text-zinc-300 space-y-1">
              <li v-for="company in rejectedCompanies" :key="company">
                {{ company }}
              </li>
            </ul>
          </div>

          <div class="flex flex-wrap gap-2">
            <a
              v-if="downloadUrl"
              :href="downloadUrl"
              download="Results to import to Apollo.csv"
              class="inline-block rounded-md bg-emerald-700 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-600"
            >
              Download passed & rejected companies for Apollo
            </a>
          </div>
          <a
            v-if="completedJobId"
            :href="`${API_URL}/pdf/${completedJobId}`"
            class="inline-flex items-center gap-2 rounded-md border border-indigo-500/30 bg-indigo-600 px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-indigo-500 transition-colors"
          >
            <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 20 20" fill="currentColor">
              <path fill-rule="evenodd" d="M6 2a2 2 0 00-2 2v12a2 2 0 002 2h8a2 2 0 002-2V7.414A2 2 0 0015.414 6L12 2.586A2 2 0 0010.586 2H6zm5 6a1 1 0 10-2 0v3.586l-1.293-1.293a1 1 0 10-1.414 1.414l3 3a1 1 0 001.414 0l3-3a1 1 0 00-1.414-1.414L11 11.586V8z" clip-rule="evenodd" />
            </svg>
            Download campaign push report (PDF)
          </a>
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
          Choose a user to unlock the app and route to the correct Lemlist campaigns.
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
