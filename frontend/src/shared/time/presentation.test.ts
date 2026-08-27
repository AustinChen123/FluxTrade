import { describe, expect, it } from "vitest";

import {
  formatPresentationTimestamp,
  parsePresentationTimestamp,
  PRESENTATION_TIME_ZONE
} from "./presentation";

const nextOutsidePositiveTimeClip = 8_640_000_000_000_001;
const nextOutsideNegativeTimeClip = -8_640_000_000_000_001;
type BerlinLocale = "en" | "zh-TW";
type FormatterKind =
  | "date"
  | "short-axis"
  | "short-tooltip"
  | "full-tooltip"
  | "numeric-axis"
  | "numeric-tooltip"
  | "medium";
type BerlinCase = Readonly<{
  name: string;
  input: string;
  month: string;
  shortMonth: Readonly<Record<BerlinLocale, string>>;
  day: string;
  hour24: string;
  hour12: string;
  dayPeriod: Readonly<Record<BerlinLocale, string>>;
  zone: string;
}>;

const berlinCases: readonly BerlinCase[] = [
  {
    name: "winter",
    input: "2026-01-15T12:34:00Z",
    month: "01",
    shortMonth: { en: "Jan", "zh-TW": "1" },
    day: "15",
    hour24: "13",
    hour12: "1",
    dayPeriod: { en: "PM", "zh-TW": "下午" },
    zone: "GMT+1"
  },
  {
    name: "summer",
    input: "2026-07-15T12:34:00Z",
    month: "07",
    shortMonth: { en: "Jul", "zh-TW": "7" },
    day: "15",
    hour24: "14",
    hour12: "2",
    dayPeriod: { en: "PM", "zh-TW": "下午" },
    zone: "GMT+2"
  },
  {
    name: "midnight rollover",
    input: "2026-01-15T23:30:00Z",
    month: "01",
    shortMonth: { en: "Jan", "zh-TW": "1" },
    day: "16",
    hour24: "00",
    hour12: "12",
    dayPeriod: { en: "AM", "zh-TW": "凌晨" },
    zone: "GMT+1"
  }
];
const dateOptions = { year: "numeric", month: "2-digit", day: "2-digit" } as const;
const shortAxisOptions = {
  month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hour12: false
} as const;
const shortTooltipOptions = { ...shortAxisOptions, timeZoneName: "short" } as const;
const numericAxisOptions = {
  month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false
} as const;
const numericTooltipOptions = { ...numericAxisOptions, timeZoneName: "short" } as const;
const fullTooltipOptions = { ...dateOptions, ...numericTooltipOptions } as const;
const mediumOptions = { dateStyle: "medium", timeStyle: "medium" } as const;
const formatterRows = [
  { name: "research epoch", kind: "date", options: dateOptions },
  { name: "results period", kind: "date", options: dateOptions },
  { name: "results equity axis", kind: "short-axis", options: shortAxisOptions },
  { name: "results equity tooltip", kind: "short-tooltip", options: shortTooltipOptions },
  { name: "results trade entry and exit", kind: "full-tooltip", options: fullTooltipOptions },
  { name: "trade candle axis", kind: "numeric-axis", options: numericAxisOptions },
  { name: "trade candle tooltip and marker", kind: "numeric-tooltip", options: numericTooltipOptions },
  { name: "trade detail", kind: "full-tooltip", options: fullTooltipOptions },
  { name: "strategy heartbeat and uptime", kind: "medium", options: mediumOptions }
] as const satisfies readonly Readonly<{
  name: string;
  kind: FormatterKind;
  options: Intl.DateTimeFormatOptions;
}>[];

function expectedParts(
  kind: FormatterKind,
  locale: BerlinLocale,
  value: BerlinCase
): string[][] {
  const date =
    locale === "en"
      ? [
          ["month", value.month],
          ["day", value.day],
          ["year", "2026"]
        ]
      : [
          ["year", "2026"],
          ["month", value.month],
          ["day", value.day]
        ];
  const shortDate = [
    ["month", value.shortMonth[locale]],
    ["day", value.day]
  ];
  const numericDate = [
    ["month", value.month],
    ["day", value.day]
  ];
  const time = [
    ["hour", value.hour24],
    ["minute", value.name === "midnight rollover" ? "30" : "34"]
  ];
  const zone = [["timeZoneName", value.zone]];

  switch (kind) {
    case "date":
      return date;
    case "short-axis":
      return [...shortDate, ...time];
    case "short-tooltip":
      return [...shortDate, ...time, ...zone];
    case "full-tooltip":
      return [...date, ...time, ...zone];
    case "numeric-axis":
      return [...numericDate, ...time];
    case "numeric-tooltip":
      return [...numericDate, ...time, ...zone];
    case "medium": {
      const mediumDate =
        locale === "en"
          ? [
              ["month", value.shortMonth.en],
              ["day", value.day],
              ["year", "2026"]
            ]
          : [
              ["year", "2026"],
              ["month", value.shortMonth["zh-TW"]],
              ["day", value.day]
            ];
      const mediumTime = [
        ["hour", value.hour12],
        ["minute", value.name === "midnight rollover" ? "30" : "34"],
        ["second", "00"]
      ];
      return locale === "en"
        ? [...mediumDate, ...mediumTime, ["dayPeriod", value.dayPeriod.en]]
        : [
            ...mediumDate,
            ["dayPeriod", value.dayPeriod["zh-TW"]],
            ...mediumTime
          ];
    }
  }
}

describe("presentation timestamps", () => {
  it.each([
    ["2026-01-15T12:34:00Z", Date.UTC(2026, 0, 15, 12, 34)],
    ["2026-01-15T12:34:00+00:00", Date.UTC(2026, 0, 15, 12, 34)],
    ["2026-01-15T12:34:00.123Z", Date.UTC(2026, 0, 15, 12, 34, 0, 123)],
    [0, 0],
    [-1, -1],
    [0.5, 0],
    [-0.5, 0],
    [8.64e15, 8.64e15],
    [-8.64e15, -8.64e15]
  ] as const)("accepts canonical presentation timestamp %j", (value, expected) => {
    const parsed = parsePresentationTimestamp(value);

    expect(parsed).toBe(expected);
    if (expected === 0) {
      expect(Object.is(parsed, 0)).toBe(true);
    }
  });

  it.each([
    "2026-02-30T12:34:00Z",
    "2026-01-15",
    "2026-01-15T12:34:00",
    "2026-01-15T13:34:00+01:00",
    " 2026-01-15T12:34:00Z",
    "0",
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.NEGATIVE_INFINITY,
    nextOutsidePositiveTimeClip,
    nextOutsideNegativeTimeClip,
    null,
    undefined,
    new Date("2026-01-15T12:34:00Z")
  ])("rejects non-canonical presentation timestamp %j", (value) => {
    expect(parsePresentationTimestamp(value)).toBeNull();
  });

  it.each(formatterRows)(
    "freezes every $name formatter part in both locales",
    ({ kind, options }) => {
      for (const locale of ["en", "zh-TW"] as const) {
        const formatter = new Intl.DateTimeFormat(locale, {
          ...options,
          timeZone: PRESENTATION_TIME_ZONE
        });
        for (const value of berlinCases) {
          const parts = formatter
            .formatToParts(parsePresentationTimestamp(value.input)!)
            .filter((part) => part.type !== "literal")
            .map(({ type, value: partValue }) => [type, partValue]);

          expect(parts, `${locale} ${value.name}`).toEqual(
            expectedParts(kind, locale, value)
          );
        }
      }
    }
  );

  it("renders invalid direct fields as the exact em dash", () => {
    const formatter = new Intl.DateTimeFormat("en", {
      timeZone: PRESENTATION_TIME_ZONE,
      dateStyle: "medium",
      timeStyle: "medium"
    });

    expect(formatPresentationTimestamp("not-a-timestamp", formatter)).toBe("—");
  });

});
