import { chmodSync, existsSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { resolve } from "node:path";

function pythonExecutable(root) {
  const windows = resolve(root, ".venv", "Scripts", "python.exe");
  const posix = resolve(root, ".venv", "bin", "python");
  return existsSync(windows) ? windows : existsSync(posix) ? posix : "python";
}

export function loadVaultAccessToken(root) {
  const result = spawnSync(pythonExecutable(root), ["-m", "scripts.secret_store_bridge", "get-access"], {
    cwd: root, encoding: "utf8", windowsHide: true, stdio: ["ignore", "pipe", "ignore"],
  });
  return result.status === 0 ? String(result.stdout || "").trim() : "";
}

export function storeSessionTokens(root, sessionFile, access, refresh) {
  const result = spawnSync(pythonExecutable(root), ["-m", "scripts.secret_store_bridge", "store"], {
    cwd: root, input: JSON.stringify({ access, refresh }), encoding: "utf8", windowsHide: true,
    stdio: ["pipe", "ignore", "ignore"],
  });
  if (result.status === 0) return { storedIn: "system_secret_store", diskFallback: false };
  writeFileSync(sessionFile, `FOMO_ACCESS_TOKEN=${access}\nFOMO_REFRESH_TOKEN=${refresh}\n`, { mode: 0o600 });
  try { chmodSync(sessionFile, 0o600); } catch {}
  return { storedIn: "permission_restricted_disk_fallback", diskFallback: true };
}
