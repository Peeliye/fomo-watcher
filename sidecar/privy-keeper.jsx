import React, { useEffect } from "react";
import { createRoot } from "react-dom/client";
import { PrivyProvider, usePrivy } from "@privy-io/react-auth";

function SessionKeeper() {
  const { ready, authenticated, getAccessToken } = usePrivy();

  useEffect(() => {
    window.__privyKeeper = { ready, authenticated, lastRefreshAt: null, error: null };
    if (!ready) return undefined;
    const refresh = async () => {
      try {
        const token = await getAccessToken();
        window.__privyKeeper = {
          ready: true,
          authenticated: Boolean(token) || authenticated,
          lastRefreshAt: new Date().toISOString(),
          error: token ? null : "getAccessToken returned null",
        };
      } catch (error) {
        window.__privyKeeper = {
          ready: true,
          authenticated,
          lastRefreshAt: new Date().toISOString(),
          error: String(error?.message || error),
        };
      }
    };
    refresh();
    const timer = setInterval(refresh, 30000);
    return () => clearInterval(timer);
  }, [ready, authenticated, getAccessToken]);

  return null;
}

createRoot(document.getElementById("root")).render(
  <PrivyProvider appId={window.__PRIVY_APP_ID__} config={{ loginMethods: ["email", "google"] }}>
    <SessionKeeper />
  </PrivyProvider>,
);
