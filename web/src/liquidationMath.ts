// Model quantities never pass through the integer-cents formatter.
function compactDecimal(raw: string, decimalPlaces = 0): string {
  const match = /^(-?)(\d+)(?:\.(\d+))?$/.exec(raw);
  if (!match) return "—";
  const fraction = match[3] ?? "";
  const integer = BigInt(match[2] + fraction);
  const denominator = 10n ** BigInt(fraction.length + decimalPlaces);
  const scale =
    integer >= denominator * 100000000n
      ? 100000000n
      : integer >= denominator * 10000n
        ? 10000n
        : 1n;
  const divisor = denominator * scale;
  const rounded = (integer * 100n + divisor / 2n) / divisor;
  if (rounded === 0n && integer > 0n) return match[1] ? ">−0.01" : "<0.01";
  const decimals = (rounded % 100n)
    .toString()
    .padStart(2, "0")
    .replace(/0+$/, "");
  return `${match[1] && rounded ? "−" : ""}${rounded / 100n}${decimals ? "." + decimals : ""}${scale === 100000000n ? "亿" : scale === 10000n ? "万" : ""}`;
}
export const modelScale = (raw: string | null | undefined) =>
  raw == null ? "—" : compactDecimal(raw);
export const liquidationUSD = (cents: number | null | undefined) =>
  cents == null || !Number.isSafeInteger(cents)
    ? "—"
    : compactDecimal(String(cents), 2) + " USD";
export type RankedZone = {
  id: string;
  relative: number;
  distancePercent: number | null;
  low: number;
  eligible: boolean;
};
export function selectZoneRows<T extends RankedZone>(
  zones: T[],
  expanded: boolean,
  order: string,
): T[] {
  const strong = [...zones].sort(
    (a, b) => b.relative - a.relative || a.low - b.low,
  );
  const near = zones
    .filter((z) => z.relative >= 50 && z.distancePercent != null)
    .sort(
      (a, b) =>
        a.distancePercent! - b.distancePercent! || b.relative - a.relative,
    );
  const chosen = expanded
    ? [...zones]
    : [
        ...new Map(
          [...strong.slice(0, 3), ...near.slice(0, 3)].map((z) => [z.id, z]),
        ).values(),
      ];
  return chosen.sort((a, b) =>
    order === "strength"
      ? b.relative - a.relative || b.low - a.low
      : order === "distance"
        ? (a.distancePercent ?? Infinity) - (b.distancePercent ?? Infinity) ||
          b.low - a.low
        : b.low - a.low,
  );
}
