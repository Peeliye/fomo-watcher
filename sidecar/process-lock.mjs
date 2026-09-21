import { open, readFile, unlink } from "node:fs/promises";

function processAlive(pid) {
  try { process.kill(pid, 0); return true; } catch (error) { return error?.code === "EPERM"; }
}

export async function acquireSingletonLock(path, role) {
  async function create() {
    const handle = await open(path, "wx", 0o600);
    await handle.writeFile(JSON.stringify({ pid: process.pid, role, startedAt: new Date().toISOString() }));
    return handle;
  }
  let handle;
  try {
    handle = await create();
  } catch (error) {
    if (error?.code !== "EEXIST") throw error;
    let holder = null;
    try { holder = JSON.parse(await readFile(path, "utf8")); } catch {}
    if (holder?.pid && processAlive(Number(holder.pid))) {
      throw new Error(`sidecar_writer_lock_held:${holder.role || "unknown"}:${holder.pid}`);
    }
    await unlink(path);
    handle = await create();
  }
  let released = false;
  return async () => {
    if (released) return;
    released = true;
    await handle.close().catch(() => {});
    await unlink(path).catch(() => {});
  };
}
