// Service worker for the SM System TradingView Bridge.
//
// Receives {url} messages from the content script and:
//   1. Queries all tradingview.com tabs.
//   2. If one exists -> chrome.tabs.update(...) navigates it in place and
//      focuses it. This is what plain window.open('...', 'name') fails to
//      do across origin-isolated pages -- extensions bypass COOP.
//   3. If none exists -> chrome.tabs.create({url}) opens a fresh tab that
//      subsequent clicks will reuse.
//
// Only ever keeps ONE TradingView tab alive by design. If the user has
// opened extra TradingView tabs manually, the first match wins; the
// others are left alone.

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  const url = msg && msg.url;
  if (typeof url !== "string") return;

  (async () => {
    try {
      const tabs = await chrome.tabs.query({
        url: "https://www.tradingview.com/*",
      });
      if (tabs.length > 0) {
        const tab = tabs[0];
        await chrome.tabs.update(tab.id, { url, active: true });
        // Also bring the containing window to the front so the reused
        // tab is actually visible after the click.
        if (typeof tab.windowId === "number") {
          await chrome.windows.update(tab.windowId, { focused: true });
        }
      } else {
        await chrome.tabs.create({ url });
      }
      sendResponse({ ok: true });
    } catch (err) {
      // Log to the service worker console (edge://extensions ->
      // "Inspect views: service worker") so failures are visible.
      console.error("[tv_bridge] failed to route", err);
      sendResponse({ ok: false, error: String(err) });
    }
  })();

  // Return true to keep the message channel open for the async response.
  return true;
});
