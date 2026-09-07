// Service worker for the SM System TradingView Bridge.
//
// Receives {url} messages from the content script and:
//   1. Queries all tradingview.com tabs.
//   2. If one exists -> discard it (bypasses the page's beforeunload,
//      which is what triggers TradingView's "Leave site? Changes you
//      made may not be saved" prompt), then chrome.tabs.update(...) it
//      to the new URL and focus its window. If discard fails (Chrome
//      refuses to discard the active tab), fall back to injecting a
//      tiny script that clears onbeforeunload and stops any listener
//      from setting returnValue, then navigate.
//   3. If none exists -> chrome.tabs.create({url}).
//
// COOP severs plain window.open('...', 'name') reuse across origin-
// isolated pages; extensions bypass that.
//
// Only ever keeps ONE TradingView tab alive by design. If the user has
// opened extra TradingView tabs manually, the first match wins; the
// others are left alone.

async function disarmBeforeUnload(tabId) {
  // Runs in MAIN world so it can touch window.onbeforeunload directly.
  // Best-effort: TradingView usually attaches via addEventListener, which
  // we can't remove after the fact, so we also install a capture-phase
  // listener that stops propagation and clears returnValue before the
  // page's own listener can set it.
  await chrome.scripting.executeScript({
    target: { tabId },
    world: "MAIN",
    func: () => {
      window.onbeforeunload = null;
      window.addEventListener(
        "beforeunload",
        (ev) => {
          ev.stopImmediatePropagation();
          delete ev.returnValue;
        },
        { capture: true }
      );
    },
  });
}

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  const url = msg && msg.url;
  if (typeof url !== "string") return;

  (async () => {
    try {
      const tabs = await chrome.tabs.query({
        url: "https://www.tradingview.com/*",
      });
      if (tabs.length > 0) {
        let tab = tabs[0];

        // Preferred path: discard the tab. The renderer is frozen and its
        // beforeunload never runs. chrome.tabs.discard returns the tab in
        // its discarded form (same id).
        let discarded = false;
        try {
          const t = await chrome.tabs.discard(tab.id);
          if (t) {
            tab = t;
            discarded = true;
          }
        } catch (e) {
          // Common cause: tab is the active tab in its window. Fall through
          // to the injection-based disarm below.
          console.warn("[tv_bridge] tab.discard failed, using script disarm:", e);
        }

        // Fallback: if we couldn't discard, disarm the page's beforeunload
        // handlers with a small injected script BEFORE we navigate.
        if (!discarded) {
          try {
            await disarmBeforeUnload(tab.id);
          } catch (e) {
            console.warn("[tv_bridge] disarm script failed:", e);
          }
        }

        await chrome.tabs.update(tab.id, { url, active: true });
        if (typeof tab.windowId === "number") {
          await chrome.windows.update(tab.windowId, { focused: true });
        }
      } else {
        await chrome.tabs.create({ url });
      }
      sendResponse({ ok: true });
    } catch (err) {
      // Log to the service worker console (chrome://extensions ->
      // "Inspect views: service worker") so failures are visible.
      console.error("[tv_bridge] failed to route", err);
      sendResponse({ ok: false, error: String(err) });
    }
  })();

  // Return true to keep the message channel open for the async response.
  return true;
});
