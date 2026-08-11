SELECT ms.symbol, l.symbolid, COUNT(*) AS rows
FROM livestream l
JOIN monitored_symbols ms USING (symbolid)
GROUP BY ms.symbol, l.symbolid
ORDER BY rows DESC
LIMIT 20;