// Headless check of the live order book page. Needs the Compose stack (and ideally the bots) running.
// Run: npm install --prefix scripts/.ui-smoke playwright-core@1.55.0 && NODE_PATH=scripts/.ui-smoke/node_modules node scripts/ui-smoke.mjs
// Uses the locally installed Google Chrome (channel: 'chrome').
import { chromium } from 'playwright-core';
const browser = await chromium.launch({ channel: 'chrome', headless: true });
const page = await browser.newPage({ viewport: { width: 1200, height: 900 } });
const errors = [];
page.on('pageerror', e => errors.push('pageerror: ' + e.message));
page.on('console', m => { if (m.type() === 'error') errors.push('console: ' + m.text()); });
await page.goto('http://localhost:8081/', { waitUntil: 'load' });
await page.waitForFunction(() => /seq \d+/.test(document.getElementById('status').textContent), null, { timeout: 15000 });
const seq1 = Number((await page.textContent('#status')).match(/seq (\d+)/)[1]);
await page.waitForTimeout(3000);
const seq2 = Number((await page.textContent('#status')).match(/seq (\d+)/)[1]);
const bids = await page.$$eval('#bids tr.level', r => r.length);
const asks = await page.$$eval('#asks tr.level', r => r.length);
const trades = await page.$$eval('#trades tr', r => r.length);
const spread = await page.textContent('#spread');
await page.screenshot({ path: process.env.SCREENSHOT ?? 'ui-aapl.png' });
await page.selectOption('#symbol', 'MSFT');
await page.waitForFunction(() => /seq \d+/.test(document.getElementById('status').textContent), null, { timeout: 15000 });
await page.waitForTimeout(1500);
const msftBids = await page.$$eval('#bids tr.level', r => r.length);
const msftTop = await page.$$eval('#bids tr.level td.price', c => c.length ? c[0].textContent.trim() : null);
console.log(JSON.stringify({ seq1, seq2, advancing: seq2 > seq1, bids, asks, trades, spread, msftBids, msftTop,
  status: await page.textContent('#status'), errors }));
await browser.close();
