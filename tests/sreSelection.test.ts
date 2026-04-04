import { describe, expect, it } from "vitest";
import { fillToMinimumWithBackfill, selectTopSreForLemlist, selectKeywordMatchedByTenure } from "../src/services/sreSelection";
import { EnrichedEmployee } from "../src/types/prospect";

function employee(overrides: Partial<EnrichedEmployee>): EnrichedEmployee {
  return {
    startDate: "2023-01-01",
    endDate: null,
    name: "Default Name",
    linkedinUrl: null,
    currentTitle: "SRE",
    tenure: 12,
    ...overrides,
  };
}

describe("selectTopSreForLemlist", () => {
  it("caps at 7 and keeps longest-tenure in same tier", () => {
    const tier1Employees = Array.from({ length: 8 }, (_, index) =>
      employee({
        name: `Director ${index + 1}`,
        currentTitle: "Director of SRE",
        tenure: index + 1,
      })
    );

    const selected = selectTopSreForLemlist(tier1Employees, 7);
    expect(selected).toHaveLength(7);
    expect(selected.map((item) => item.name)).toEqual([
      "Director 8",
      "Director 7",
      "Director 6",
      "Director 5",
      "Director 4",
      "Director 3",
      "Director 2",
    ]);
  });

  it("fills by tiers in order and trims lower tier by tenure", () => {
    const selected = selectTopSreForLemlist(
      [
        employee({ name: "Head One", currentTitle: "Head of Site Reliability", tenure: 1 }),
        employee({ name: "Head Two", currentTitle: "SRE Director", tenure: 2 }),
        employee({ name: "Manager A", currentTitle: "SRE Manager", tenure: 2 }),
        employee({ name: "Manager B", currentTitle: "Staff Site Reliability Engineer", tenure: 10 }),
        employee({ name: "Manager C", currentTitle: "SRE Manager", tenure: 5 }),
        employee({ name: "Manager D", currentTitle: "Staff SRE", tenure: 8 }),
        employee({ name: "Manager E", currentTitle: "SRE Manager", tenure: 7 }),
        employee({ name: "Manager F", currentTitle: "Staff SRE", tenure: 6 }),
      ],
      7
    );

    expect(selected).toHaveLength(7);
    expect(selected.map((item) => item.name)).toEqual([
      "Head Two",
      "Head One",
      "Manager B",
      "Manager D",
      "Manager E",
      "Manager F",
      "Manager C",
    ]);
  });

  it("keeps tier 1-3 with null startDate when under limit", () => {
    const selected = selectTopSreForLemlist(
      [
        employee({
          name: "Head Null",
          currentTitle: "Head of SRE",
          startDate: null,
          tenure: null,
        }),
        employee({
          name: "Senior Null",
          currentTitle: "Senior Site Reliability Engineer",
          startDate: null,
          tenure: null,
        }),
      ],
      7
    );

    expect(selected).toHaveLength(2);
    expect(selected.map((item) => item.name)).toEqual(["Head Null", "Senior Null"]);
  });

  it("ranks null startDate lower only when trimming", () => {
    const selected = selectTopSreForLemlist(
      [
        employee({ name: "Staff Null", currentTitle: "Staff SRE", startDate: null, tenure: null }),
        employee({ name: "Staff A", currentTitle: "Staff SRE", tenure: 5 }),
        employee({ name: "Staff B", currentTitle: "Staff SRE", tenure: 4 }),
      ],
      2
    );

    expect(selected.map((item) => item.name)).toEqual(["Staff A", "Staff B"]);
  });

  it("applies tenure >= 2 months only to tier 4", () => {
    const selected = selectTopSreForLemlist(
      [
        employee({ name: "Plain SRE 1m", currentTitle: "SRE", tenure: 1 }),
        employee({ name: "Plain SRE 2m", currentTitle: "SRE", tenure: 2 }),
        employee({ name: "Senior 1m", currentTitle: "Senior SRE", tenure: 1 }),
      ],
      7
    );

    expect(selected.map((item) => item.name)).toEqual(["Senior 1m", "Plain SRE 2m"]);
  });

  it("treats Head of Reliability as tier 1", () => {
    const selected = selectTopSreForLemlist(
      [
        employee({ name: "Head Reliability", currentTitle: "Head of Reliability", tenure: 1 }),
        employee({ name: "Senior SRE", currentTitle: "Senior SRE", tenure: 100 }),
      ],
      1
    );

    expect(selected).toHaveLength(1);
    expect(selected[0].name).toBe("Head Reliability");
  });

  it("treats all tier-1 head titles equally and trims by tenure", () => {
    const selected = selectTopSreForLemlist(
      [
        employee({ name: "Head Reliability", currentTitle: "Head of Reliability", tenure: 2 }),
        employee({ name: "Head Site Reliability", currentTitle: "Head of Site Reliability", tenure: 8 }),
        employee({ name: "Head SRE", currentTitle: "Head of SRE", tenure: 5 }),
        employee({ name: "SRE Director", currentTitle: "Director of SRE", tenure: 7 }),
      ],
      3
    );

    expect(selected.map((item) => item.name)).toEqual([
      "Head Site Reliability",
      "SRE Director",
      "Head SRE",
    ]);
  });
});

describe("fillToMinimumWithBackfill", () => {
  it("returns empty when there is no current SRE baseline", () => {
    const result = fillToMinimumWithBackfill(
      [],
      [employee({ id: "past-1", name: "Past 1", currentTitle: "Engineer", tenure: 40 })],
      [employee({ id: "platform-1", name: "Platform 1", currentTitle: "Platform Engineer", tenure: 40 })],
      { minimum: 5, max: 7 }
    );

    expect(result).toEqual([]);
  });

  it("does not backfill when current SRE already >= 5", () => {
    const currentSelected = Array.from({ length: 5 }, (_, index) =>
      employee({ id: `current-${index}`, name: `Current ${index}`, currentTitle: "SRE", tenure: 12 })
    );

    const result = fillToMinimumWithBackfill(
      currentSelected,
      [employee({ id: "past-1", name: "Past 1", currentTitle: "Engineer", tenure: 24 })],
      [employee({ id: "platform-1", name: "Platform 1", currentTitle: "Platform Engineer", tenure: 24 })],
      { minimum: 5, max: 7 }
    );

    expect(result.map((item) => item.id)).toEqual(currentSelected.map((item) => item.id));
  });

  it("backfills from past SRE first to reach floor 5", () => {
    const currentSelected = [
      employee({ id: "current-1", name: "Current 1", currentTitle: "Senior SRE", tenure: 20 }),
      employee({ id: "current-2", name: "Current 2", currentTitle: "SRE Manager", tenure: 18 }),
    ];

    const pastCandidates = [
      employee({ id: "past-low", name: "Past Low", currentTitle: "Engineer", tenure: 6 }),
      employee({ id: "past-high", name: "Past High", currentTitle: "Engineer", tenure: 36 }),
      employee({ id: "past-mid", name: "Past Mid", currentTitle: "Engineer", tenure: 18 }),
    ];

    const result = fillToMinimumWithBackfill(currentSelected, pastCandidates, [], { minimum: 5, max: 7 });

    expect(result).toHaveLength(5);
    expect(result.map((item) => item.id)).toEqual(["current-1", "current-2", "past-high", "past-mid", "past-low"]);
  });

  it("skips duplicate IDs during backfill", () => {
    const currentSelected = [employee({ id: "same-id", name: "Current", currentTitle: "SRE", tenure: 10 })];
    const pastCandidates = [
      employee({ id: "same-id", name: "Duplicate", currentTitle: "Engineer", tenure: 40 }),
      employee({ id: "past-2", name: "Past 2", currentTitle: "Engineer", tenure: 15 }),
      employee({ id: "past-3", name: "Past 3", currentTitle: "Engineer", tenure: 14 }),
      employee({ id: "past-4", name: "Past 4", currentTitle: "Engineer", tenure: 13 }),
    ];

    const result = fillToMinimumWithBackfill(currentSelected, pastCandidates, [], { minimum: 4, max: 7 });
    expect(result.map((item) => item.id)).toEqual(["same-id", "past-2", "past-3", "past-4"]);
  });

  it("uses platform backfill when past SRE is not enough", () => {
    const currentSelected = [employee({ id: "current-1", name: "Current 1", currentTitle: "SRE", tenure: 12 })];
    const pastCandidates = [employee({ id: "past-1", name: "Past 1", currentTitle: "Engineer", tenure: 20 })];
    const platformCandidates = [
      employee({ id: "platform-senior", name: "Senior Platform", currentTitle: "Senior Platform Engineer", tenure: 2 }),
      employee({ id: "platform-junior-11", name: "Platform 11m", currentTitle: "Platform Engineer", tenure: 11 }),
      employee({ id: "platform-junior-8", name: "Platform 8m", currentTitle: "Platform Engineer", tenure: 8 }),
    ];

    const result = fillToMinimumWithBackfill(currentSelected, pastCandidates, platformCandidates, {
      minimum: 5,
      max: 7,
    });

    expect(result.map((item) => item.id)).toEqual(["current-1", "past-1", "platform-senior", "platform-junior-11"]);
  });

  it("allows past SRE with unknown tenure and falls back to platform only if still short", () => {
    const currentSelected = [employee({ id: "current-1", name: "Current 1", currentTitle: "SRE", tenure: 12 })];
    const pastCandidates = [
      employee({ id: "past-0m", name: "Past 0m", currentTitle: "Engineer", tenure: 0 }),
      employee({ id: "past-1m", name: "Past 1m", currentTitle: "Engineer", tenure: 1 }),
      employee({
        id: "past-null",
        name: "Past Null",
        currentTitle: "Engineer",
        startDate: null,
        tenure: null,
      }),
    ];
    const platformCandidates = [
      employee({ id: "platform-senior", name: "Senior Platform", currentTitle: "Senior Platform Engineer", tenure: 1 }),
      employee({ id: "platform-junior-11", name: "Platform 11m", currentTitle: "Platform Engineer", tenure: 11 }),
    ];

    const result = fillToMinimumWithBackfill(currentSelected, pastCandidates, platformCandidates, {
      minimum: 3,
      max: 5,
    });

    expect(result.map((item) => item.id)).toEqual(["current-1", "past-null", "platform-senior"]);
  });

  it("ranks past SRE with null startDate lower when only some can be taken", () => {
    const currentSelected = [employee({ id: "current-1", name: "Current 1", currentTitle: "SRE", tenure: 12 })];
    const pastCandidates = [
      employee({ id: "past-known-4m", name: "Past Known 4m", currentTitle: "Engineer", tenure: 4 }),
      employee({ id: "past-known-3m", name: "Past Known 3m", currentTitle: "Engineer", tenure: 3 }),
      employee({
        id: "past-null",
        name: "Past Null",
        currentTitle: "Engineer",
        startDate: null,
        tenure: null,
      }),
    ];

    const result = fillToMinimumWithBackfill(currentSelected, pastCandidates, [], {
      minimum: 3,
      max: 5,
    });

    expect(result.map((item) => item.id)).toEqual(["current-1", "past-known-4m", "past-known-3m"]);
  });

  it("hard-caps at 7 when current plus backfill exceed limit", () => {
    const currentSelected = Array.from({ length: 4 }, (_, index) =>
      employee({ id: `current-${index}`, name: `Current ${index}`, currentTitle: "SRE", tenure: 12 + index })
    );
    const pastCandidates = Array.from({ length: 6 }, (_, index) =>
      employee({ id: `past-${index}`, name: `Past ${index}`, currentTitle: "Engineer", tenure: 30 - index })
    );

    const result = fillToMinimumWithBackfill(currentSelected, pastCandidates, [], { minimum: 7, max: 7 });
    expect(result).toHaveLength(7);
    expect(new Set(result.map((item) => item.id)).size).toBe(7);
  });

  it("phase-2 platform backfill respects strict max 5", () => {
    const currentSelected = [
      employee({ id: "current-1", name: "Current 1", currentTitle: "SRE Manager", tenure: 12 }),
      employee({ id: "current-2", name: "Current 2", currentTitle: "SRE", tenure: 10 }),
      employee({ id: "current-3", name: "Current 3", currentTitle: "Senior SRE", tenure: 9 }),
    ];
    const platformCandidates = [
      employee({ id: "platform-1", name: "Senior Platform 1", currentTitle: "Senior Platform Engineer", tenure: 20 }),
      employee({ id: "platform-2", name: "Senior Platform 2", currentTitle: "Staff Platform Engineer", tenure: 19 }),
      employee({ id: "platform-3", name: "Platform 3", currentTitle: "Platform Engineer", tenure: 16 }),
      employee({ id: "platform-4", name: "Platform 4", currentTitle: "Platform Engineer", tenure: 15 }),
    ];

    const result = fillToMinimumWithBackfill(currentSelected, [], platformCandidates, { minimum: 5, max: 5 });
    expect(result).toHaveLength(5);
    expect(result.map((item) => item.id)).toEqual(["current-1", "current-2", "current-3", "platform-1", "platform-2"]);
  });

  it("ignores non-platform titles during platform backfill", () => {
    const currentSelected = [employee({ id: "current-1", name: "Current 1", currentTitle: "SRE", tenure: 12 })];
    const platformCandidates = [
      employee({ id: "candidate-a", name: "Non Platform", currentTitle: "Backend Engineer", tenure: 40 }),
      employee({ id: "candidate-b", name: "Platform One", currentTitle: "Platform Engineer", tenure: 12 }),
      employee({ id: "candidate-c", name: "Platform Two", currentTitle: "Senior Platform Engineer", tenure: 4 }),
    ];

    const result = fillToMinimumWithBackfill(currentSelected, [], platformCandidates, { minimum: 3, max: 5 });
    expect(result.map((item) => item.id)).toEqual(["current-1", "candidate-c", "candidate-b"]);
  });
});

describe("selectKeywordMatchedByTenure", () => {
  it("fills LinkedIn slots up to maxTotal after already selected", () => {
    const alreadySelected = [
      employee({ id: "sre-1", name: "SRE 1", tenure: 12 }),
      employee({ id: "sre-2", name: "SRE 2", tenure: 10 }),
      employee({ id: "sre-3", name: "SRE 3", tenure: 8 }),
    ];
    const keywordMatched = [
      employee({ id: "kw-1", name: "KW 1", tenure: 20 }),
      employee({ id: "kw-2", name: "KW 2", tenure: 15 }),
      employee({ id: "kw-3", name: "KW 3", tenure: 10 }),
      employee({ id: "kw-4", name: "KW 4", tenure: 5 }),
      employee({ id: "kw-5", name: "KW 5", tenure: 3 }),
      employee({ id: "kw-6", name: "KW 6", tenure: 2 }),
    ];

    const result = selectKeywordMatchedByTenure(keywordMatched, alreadySelected, 7);

    expect(result.forLinkedin).toHaveLength(4);
    expect(result.forLinkedin.map((e) => e.id)).toEqual(["kw-1", "kw-2", "kw-3", "kw-4"]);
    expect(result.forEmailRecycling).toHaveLength(2);
    expect(result.forEmailRecycling.map((e) => e.id)).toEqual(["kw-5", "kw-6"]);
  });

  it("returns all as forLinkedin when enough slots available", () => {
    const alreadySelected = [
      employee({ id: "sre-1", name: "SRE 1", tenure: 12 }),
    ];
    const keywordMatched = [
      employee({ id: "kw-1", name: "KW 1", tenure: 20 }),
      employee({ id: "kw-2", name: "KW 2", tenure: 15 }),
    ];

    const result = selectKeywordMatchedByTenure(keywordMatched, alreadySelected, 7);

    expect(result.forLinkedin).toHaveLength(2);
    expect(result.forEmailRecycling).toHaveLength(0);
  });

  it("returns all as forEmailRecycling when no slots available", () => {
    const alreadySelected = Array.from({ length: 7 }, (_, i) =>
      employee({ id: `sre-${i}`, name: `SRE ${i}`, tenure: 12 })
    );
    const keywordMatched = [
      employee({ id: "kw-1", name: "KW 1", tenure: 20 }),
    ];

    const result = selectKeywordMatchedByTenure(keywordMatched, alreadySelected, 7);

    expect(result.forLinkedin).toHaveLength(0);
    expect(result.forEmailRecycling).toHaveLength(1);
  });

  it("dedupes keyword-matched against already selected", () => {
    const alreadySelected = [
      employee({ id: "shared-1", name: "Shared", tenure: 12 }),
    ];
    const keywordMatched = [
      employee({ id: "shared-1", name: "Shared", tenure: 12 }),
      employee({ id: "kw-2", name: "KW 2", tenure: 15 }),
    ];

    const result = selectKeywordMatchedByTenure(keywordMatched, alreadySelected, 7);

    expect(result.forLinkedin).toHaveLength(1);
    expect(result.forLinkedin[0].id).toBe("kw-2");
    expect(result.forEmailRecycling).toHaveLength(0);
  });

  it("sorts by tenure descending with null tenure at end", () => {
    const keywordMatched = [
      employee({ id: "kw-null", name: "KW Null", tenure: null }),
      employee({ id: "kw-3", name: "KW 3", tenure: 3 }),
      employee({ id: "kw-10", name: "KW 10", tenure: 10 }),
    ];

    const result = selectKeywordMatchedByTenure(keywordMatched, [], 2);

    expect(result.forLinkedin.map((e) => e.id)).toEqual(["kw-10", "kw-3"]);
    expect(result.forEmailRecycling.map((e) => e.id)).toEqual(["kw-null"]);
  });

  it("handles empty keyword-matched input", () => {
    const result = selectKeywordMatchedByTenure([], [employee({ id: "sre-1", name: "SRE 1", tenure: 12 })], 7);

    expect(result.forLinkedin).toHaveLength(0);
    expect(result.forEmailRecycling).toHaveLength(0);
  });
});
