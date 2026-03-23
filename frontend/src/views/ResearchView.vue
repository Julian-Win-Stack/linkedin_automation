<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";

const API_URL = import.meta.env.VITE_API_URL ?? "http://localhost:3000";

const fileInput = ref<HTMLInputElement | null>(null);
const selectedFile = ref<File | null>(null);
const isLoading = ref(false);
const error = ref<string | null>(null);
const warnings = ref<string[]>([]);
const progressMessage = ref<string | null>(null);
const resultBlob = ref<Blob | null>(null);
const downloadUrl = ref<string | null>(null);
const summary = ref<Record<string, number> | null>(null);
const rejectedCompanies = ref<string[]>([]);
const rejectedReason = ref<string | null>(null);
const abortControllerRef = ref<AbortController | null>(null);
const pollingIntervalId = ref<number | null>(null);
const pollingSwitchTimeoutId = ref<number | null>(null);

const canRun = computed(() => !isLoading.value && !!selectedFile.value);

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
  rejectedCompanies.value = [];
  rejectedReason.value = null;
}

function onFileChange(event: Event): void {
  const target = event.target as HTMLInputElement;
  const file = target.files?.[0] ?? null;
  if (file && !file.name.endsWith(".csv")) {
    error.value = "Please select a .csv file.";
    return;
  }
  selectedFile.value = file;
  resetState();
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
  if (!selectedFile.value) {
    error.value = "Please select a CSV file.";
    return;
  }

  resetState();
  isLoading.value = true;
  abortControllerRef.value = new AbortController();

  const formData = new FormData();
  formData.append("csv", selectedFile.value);

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

    await pollJob(startPayload.jobId);
  } catch (unknownError) {
    if ((unknownError as Error).name !== "AbortError") {
      error.value = unknownError instanceof Error ? unknownError.message : String(unknownError);
    }
  } finally {
    clearPolling();
    isLoading.value = false;
    progressMessage.value = null;
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
      rejectedCompanies.value = donePayload.rejectedCompanies ?? [];
      rejectedReason.value = donePayload.rejectedReason ?? null;
      summary.value = donePayload.summary ?? null;
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

function cancelCurrentJob(): void {
  abortControllerRef.value?.abort();
  clearPolling();
  isLoading.value = false;
  progressMessage.value = null;
}

function restart(): void {
  cancelCurrentJob();
  selectedFile.value = null;
  resetState();
  if (fileInput.value) {
    fileInput.value.value = "";
  }
}
</script>

<template>
  <div
    class="flex min-h-screen items-center justify-center bg-[radial-gradient(circle_at_top,_#111a2a_0%,_#090d16_45%,_#05070c_100%)] px-4 text-zinc-200"
  >
    <div class="w-full max-w-md rounded-xl border border-[#1d2537] bg-[#0d1320]/90 p-3 shadow-[0_18px_50px_rgba(0,0,0,0.45)] space-y-3">
      <label class="block">
        <span class="sr-only">Choose csv</span>
        <input
          ref="fileInput"
          type="file"
          accept=".csv"
          class="block w-full text-sm text-zinc-300 file:mr-4 file:rounded-md file:border-0 file:bg-indigo-600 file:px-3 file:py-2 file:text-sm file:font-semibold file:text-white hover:file:bg-indigo-500"
          @change="onFileChange"
        />
      </label>

      <div class="grid grid-cols-3 gap-2">
        <button
          class="rounded-md border border-zinc-600 px-3 py-1.5 text-xs font-medium text-zinc-200 hover:bg-zinc-800/50 disabled:opacity-40"
          @click="restart"
        >
          Restart
        </button>
        <button
          class="rounded-md bg-indigo-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-indigo-500 disabled:opacity-40"
          :disabled="!canRun"
          @click="runResearch"
        >
          Research
        </button>
        <button
          class="rounded-md border border-red-700 px-3 py-1.5 text-xs text-red-300 disabled:opacity-40"
          :disabled="!isLoading"
          @click="cancelCurrentJob"
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
          <p class="text-sm font-medium tracking-tight text-zinc-300">Researching...</p>
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
        <p><strong>Total rows:</strong> {{ summary.totalRows ?? 0 }}</p>
        <p><strong>Eligible companies:</strong> {{ summary.eligibleCompanyCount ?? 0 }}</p>
        <p><strong>Rejected companies:</strong> {{ summary.rejectedCompanyCount ?? 0 }}</p>
        <p><strong>Apollo processed:</strong> {{ summary.apolloProcessedCompanyCount ?? 0 }}</p>
        <p><strong>Total SRE found:</strong> {{ summary.totalSreFound ?? 0 }}</p>
        <p><strong>Lemlist successful:</strong> {{ summary.totalLemlistSuccessful ?? 0 }}</p>
        <p><strong>Lemlist failed:</strong> {{ summary.totalLemlistFailed ?? 0 }}</p>
      </div>

      <div
        v-if="rejectedCompanies.length > 0"
        class="rounded-md border border-zinc-700 bg-zinc-900/50 p-3 text-sm space-y-2"
      >
        <p class="font-medium">Rejected Companies</p>
        <p class="text-xs text-zinc-400">
          {{
            rejectedReason ??
            "Rejected because they were using other observability tools."
          }}
        </p>
        <ul class="list-disc pl-5 text-xs text-zinc-300 space-y-1">
          <li v-for="company in rejectedCompanies" :key="company">
            {{ company }}
          </li>
        </ul>
      </div>

      <a
        v-if="downloadUrl"
        :href="downloadUrl"
        download="results.csv"
        class="inline-block rounded-md bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-500"
      >
        Download Results CSV
      </a>
    </div>
  </div>
</template>
