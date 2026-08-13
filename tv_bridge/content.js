// Runs on http://127.0.0.1:8001/* pages. Intercepts clicks on `a.tv-btn`
// elements (the ↗ buttons in the /relatr table rows) and forwards the
// target URL to the extension's background service worker. The service
// worker finds any existing tradingview.com tab and navigates it in
// place; if none exists it opens a new one. Prevents the default anchor
// navigation so the browser doesn't ALSO open a new tab.
//
// Extension not loaded? Nothing captures the click, and the anchor's
// built-in target="tradingview" fires normally (the pre-extension
// behavior). So this extension is purely additive.

document.addEventListener(
  "click",
  (e) => {
    const a = e.target.closest("a.tv-btn");
    if (!a) return;
    e.preventDefault();
    e.stopPropagation();
    chrome.runtime.sendMessage({ url: a.href });
  },
  true, // capture: run before the anchor's default handling
);
