import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import { transform } from "esbuild";
const source = await fs.readFile(
  new URL("../src/liquidationMath.ts", import.meta.url),
  "utf8",
);
const { code: compiled } = await transform(source, {
  loader: "ts",
  target: "es2022",
  format: "esm",
});
const { modelScale, liquidationUSD, selectZoneRows } = await import(
  "data:text/javascript;base64," + Buffer.from(compiled).toString("base64")
);
test("model decimal strings and USD cents keep independent units and nulls", () => {
  assert.equal(modelScale("312480691.630001"), "3.12亿");
  assert.equal(modelScale("42800000"), "4280万");
  assert.equal(modelScale("125000"), "12.5万");
  assert.equal(modelScale("0"), "0");
  assert.equal(modelScale("0.000001"), "<0.01");
  assert.equal(modelScale(null), "—");
  assert.equal(modelScale("garbage"), "—");
  assert.equal(modelScale("999999999999999.99"), "10000000亿");
  assert.equal(liquidationUSD(4280000000), "4280万 USD");
  assert.equal(liquidationUSD(125050), "1250.5 USD");
  assert.equal(liquidationUSD(0), "0 USD");
  assert.equal(liquidationUSD(null), "—");
  assert.equal(liquidationUSD(NaN), "—");
});
test("top3 plus nearest3 are deduplicated, sorting and collapse preserve full-model weights", () => {
  const zones = Array.from({ length: 10 }, (_, i) => ({
    id: String(i),
    relative: 100 - i * 5,
    distancePercent: i < 5 ? 10 - i : 10 - i * 0.8,
    low: i * 250,
    eligible: true,
  }));
  const result = selectZoneRows(zones, false, "price");
  assert.ok(result.length <= 6);
  assert.equal(new Set(result.map((z) => z.id)).size, result.length);
  assert.ok(result.some((z) => z.id === "0"));
  assert.ok(result.some((z) => z.id === "9"));
  assert.deepEqual(
    selectZoneRows(zones, true, "strength").map((z) => z.relative),
    zones.map((z) => z.relative),
  );
  const missing = zones.map((z) => ({ ...z, distancePercent: null }));
  assert.equal(selectZoneRows(missing, false, "distance").length, 3);
});
