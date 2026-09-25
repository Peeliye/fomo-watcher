const LOCAL_WEBHOOK = "http://127.0.0.1:8765/fomo/4663/buy";
const ADDRESS = /^0x[0-9a-fA-F]{40}$/;

export function shouldForwardFomoBuy(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return false;
  const body = payload.body && typeof payload.body === "object" && !Array.isArray(payload.body)
    ? payload.body : {};
  const event = { ...body, ...payload };
  return String(event.networkId) === "4663"
    && ["swap_buy", "single_user_buy"].includes(String(event.type).toLowerCase())
    && ADDRESS.test(String(event.tokenAddress || ""));
}

export async function forwardLocalFomoBuy(payload, token, fetcher = fetch) {
  if (!shouldForwardFomoBuy(payload) || typeof token !== "string" || token.length < 32) return false;
  try {
    const response = await fetcher(LOCAL_WEBHOOK, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(1500),
    });
    return response.status === 200 || response.status === 202;
  } catch {
    return false;
  }
}
