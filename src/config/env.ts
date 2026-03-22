export function getRequiredEnv(name: string): string {
  const value = process.env[name];
  if (!value || value.trim().length === 0) {
    throw new Error(`Missing ${name} environment variable.`);
  }

  return value.trim();
}

export function getEnvBoolean(name: string, defaultValue: boolean): boolean {
  const rawValue = process.env[name];
  if (!rawValue || rawValue.trim().length === 0) {
    return defaultValue;
  }

  const normalized = rawValue.trim().toLowerCase();
  if (normalized === "true") {
    return true;
  }
  if (normalized === "false") {
    return false;
  }

  return defaultValue;
}
