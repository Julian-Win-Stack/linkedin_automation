import { describe, expect, it, vi } from "vitest";
import { EnrichedEmployee, ApifyOpenToWorkCache, ApifyExperienceEntry } from "../src/types/prospect";

const getRequiredEnvMock = vi.fn();

vi.mock("../src/config/env", () => ({
  getRequiredEnv: (...args: unknown[]) => getRequiredEnvMock(...args),
}));

import {
  filterFrontendEngineers,
  filterByKeywordsInApifyData,
  filterOpenToWorkFromCache,
  filterOutHardwareHeavyPeople,
} from "../src/services/apifyClient";

function makeEmployee(
  overrides: Partial<EnrichedEmployee> & { name: string }
): EnrichedEmployee {
  return {
    id: overrides.id ?? overrides.name,
    startDate: overrides.startDate ?? "2022-01-01",
    endDate: overrides.endDate ?? null,
    name: overrides.name,
    email: overrides.email ?? null,
    linkedinUrl: overrides.linkedinUrl ?? null,
    currentTitle: overrides.currentTitle ?? "SRE",
    headline: overrides.headline ?? "",
    tenure: overrides.tenure ?? null,
  };
}


describe("filterFrontendEngineers", () => {
  it("keeps employees with no cached data", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    const employees = [
      makeEmployee({ name: "Alice", linkedinUrl: "https://linkedin.com/in/alice" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
    expect(result.rejectedFrontend).toHaveLength(0);
  });

  it("keeps employees with no linkedin URL", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    const employees = [makeEmployee({ name: "Alice", linkedinUrl: null })];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
  });

  it("rejects employees with frontend keyword in matched company experience", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/alice", {
      openToWork: false,
      profileSkills: [],
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "Building front-end components for the dashboard",
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "Alice", linkedinUrl: "https://linkedin.com/in/alice" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.rejectedFrontend).toHaveLength(1);
    expect(result.kept).toHaveLength(0);
  });

  it("rejects employees with Android or iOS keywords case-insensitively", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/mobile-android", {
      openToWork: false,
      profileSkills: [],
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "Senior Android engineer for core app platform",
        },
      ],
    });
    cache.set("linkedin.com/in/mobile-ios", {
      openToWork: false,
      profileSkills: [],
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "Built IOS architecture and release process",
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "Android Dev", linkedinUrl: "https://linkedin.com/in/mobile-android" }),
      makeEmployee({ name: "iOS Dev", linkedinUrl: "https://linkedin.com/in/mobile-ios" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.rejectedFrontend).toHaveLength(2);
    expect(result.kept).toHaveLength(0);
  });

  it("rejects employees with AI, ML, and machine learning keywords case-insensitively", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/ai-keyword", {
      openToWork: false,
      profileSkills: [],
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "Built AI assistants for internal workflows",
        },
      ],
    });
    cache.set("linkedin.com/in/ml-keyword", {
      openToWork: false,
      profileSkills: [],
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "Led ML model deployment and monitoring",
        },
      ],
    });
    cache.set("linkedin.com/in/machine-learning-keyword", {
      openToWork: false,
      profileSkills: [],
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "Worked on Machine Learning pipelines and feature stores",
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "AI Dev", linkedinUrl: "https://linkedin.com/in/ai-keyword" }),
      makeEmployee({ name: "ML Dev", linkedinUrl: "https://linkedin.com/in/ml-keyword" }),
      makeEmployee({ name: "Machine Learning Dev", linkedinUrl: "https://linkedin.com/in/machine-learning-keyword" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.rejectedFrontend).toHaveLength(3);
    expect(result.kept).toHaveLength(0);
  });

  it("keeps employees with frontend keyword but also backend override", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/bob", {
      openToWork: false,
      profileSkills: [],
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "Full-stack engineer working on front-end and back-end systems",
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "Bob", linkedinUrl: "https://linkedin.com/in/bob" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
    expect(result.rejectedFrontend).toHaveLength(0);
  });

  it("uses most recent role only and rejects frontend descriptions", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/charlie", {
      openToWork: false,
      profileSkills: [],
      experience: [
        {
          companyName: "Other Corp",
          companyUniversalName: "other-corp",
          description: "frontend developer",
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "Charlie", linkedinUrl: "https://linkedin.com/in/charlie" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(0);
    expect(result.rejectedFrontend).toHaveLength(1);
    expect(result.warningCandidates).toHaveLength(0);
  });

  it("keeps employee when description has no frontend keywords", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/dave", {
      openToWork: false,
      profileSkills: [],
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "Building infrastructure monitoring and alerting systems",
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "Dave", linkedinUrl: "https://linkedin.com/in/dave" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
    expect(result.rejectedFrontend).toHaveLength(0);
  });

  it("keeps employee with full-stack override keyword", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/eve", {
      openToWork: false,
      profileSkills: [],
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "end-to-end frontend and backend development",
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "Eve", linkedinUrl: "https://linkedin.com/in/eve" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
  });

  it("matches company by domain base when companyUniversalName matches", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/frank", {
      openToWork: false,
      profileSkills: [],
      experience: [
        {
          companyName: "Totally Different Name",
          companyUniversalName: "acme",
          description: "frontend developer",
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "Frank", linkedinUrl: "https://linkedin.com/in/frank" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Totally Different Name",
      companyDomain: "acme.com",
    });

    expect(result.rejectedFrontend).toHaveLength(1);
  });

  it("handles empty experience array", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/grace", {
      openToWork: false,
      profileSkills: [],
      experience: [],
    });

    const employees = [
      makeEmployee({ name: "Grace", linkedinUrl: "https://linkedin.com/in/grace" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
  });

  it("uses most recent role and ignores older frontend roles", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    const pastRole: ApifyExperienceEntry = {
      companyName: "Acme",
      companyUniversalName: "acme",
      description: "frontend developer",
      endDate: { text: "2021" },
    };
    const currentOtherRole: ApifyExperienceEntry = {
      companyName: "Other Corp",
      companyUniversalName: "other-corp",
      description: "backend engineer",
      endDate: { text: "Present" },
    };
    cache.set("linkedin.com/in/helen", {
      openToWork: false,
      profileSkills: [],
      experience: [currentOtherRole, pastRole],
    });

    const employees = [
      makeEmployee({ name: "Helen", linkedinUrl: "https://linkedin.com/in/helen" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
    expect(result.rejectedFrontend).toHaveLength(0);
    expect(result.warningCandidates).toHaveLength(0);
  });

  it("uses most recent role and does not add company-match warnings", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    const pastRole: ApifyExperienceEntry = {
      companyName: "Acme",
      companyUniversalName: "acme",
      description: "backend engineer",
      endDate: { text: "2021" },
    };
    const currentOtherRole: ApifyExperienceEntry = {
      companyName: "Other Corp",
      companyUniversalName: "other-corp",
      description: "backend engineer",
      endDate: { text: "Present" },
    };
    cache.set("linkedin.com/in/harry", {
      openToWork: false,
      profileSkills: [],
      experience: [currentOtherRole, pastRole],
    });

    const employees = [
      makeEmployee({ name: "Harry", linkedinUrl: "https://linkedin.com/in/harry" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
    expect(result.rejectedFrontend).toHaveLength(0);
    expect(result.warningCandidates).toHaveLength(0);
  });
});

describe("filterOpenToWorkFromCache", () => {
  it("keeps non-open-to-work employees from cache", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/kept", {
      openToWork: false,
      experience: [],
      profileSkills: [],
      canonicalLinkedinUrl: "https://linkedin.com/in/kept",
    });

    const result = filterOpenToWorkFromCache(
      [makeEmployee({ name: "Kept", linkedinUrl: "https://linkedin.com/in/kept" })],
      cache,
      { companyName: "Acme", companyDomain: "acme.com" }
    );

    expect(result.kept).toHaveLength(1);
    expect(result.filteredOut).toHaveLength(0);
  });

  it("filters open-to-work employees from cache", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/otw", {
      openToWork: true,
      experience: [],
      profileSkills: [],
      canonicalLinkedinUrl: "https://linkedin.com/in/otw",
    });

    const result = filterOpenToWorkFromCache(
      [makeEmployee({ name: "OTW", linkedinUrl: "https://linkedin.com/in/otw" })],
      cache,
      { companyName: "Acme", companyDomain: "acme.com" }
    );

    expect(result.kept).toHaveLength(0);
    expect(result.filteredOut).toHaveLength(1);
    expect(result.filteredOut[0].reason).toBe("open_to_work");
  });

  it("keeps cache misses as fail-open", () => {
    const result = filterOpenToWorkFromCache(
      [makeEmployee({ name: "Missing", linkedinUrl: "https://linkedin.com/in/missing" })],
      new Map(),
      { companyName: "Acme", companyDomain: "acme.com" }
    );

    expect(result.kept).toHaveLength(1);
    expect(result.filteredOut).toHaveLength(0);
  });
});

describe("filterByKeywordsInApifyData", () => {
  it("matches when keyword is in experience description", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/alice", {
      openToWork: false,
      profileSkills: [],
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "Managed incident response and on-call rotations",
          endDate: { text: "Present" },
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "Alice", linkedinUrl: "https://linkedin.com/in/alice" }),
    ];

    const result = filterByKeywordsInApifyData(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    }, ["incident response", "SRE"]);

    expect(result.matched).toHaveLength(1);
    expect(result.unmatched).toHaveLength(0);
  });

  it("matches when keyword is in experience skills array", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/bob", {
      openToWork: false,
      profileSkills: [],
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "Built microservices",
          skills: ["Kubernetes", "PagerDuty", "Terraform"],
          endDate: { text: "Present" },
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "Bob", linkedinUrl: "https://linkedin.com/in/bob" }),
    ];

    const result = filterByKeywordsInApifyData(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    }, ["PagerDuty"]);

    expect(result.matched).toHaveLength(1);
  });

  it("matches when keyword is in profile-level skills", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/charlie", {
      openToWork: false,
      profileSkills: [{ name: "SRE" }, { name: "Kubernetes" }],
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "General engineering work",
          endDate: { text: "Present" },
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "Charlie", linkedinUrl: "https://linkedin.com/in/charlie" }),
    ];

    const result = filterByKeywordsInApifyData(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    }, ["SRE"]);

    expect(result.matched).toHaveLength(1);
  });

  it("does not match when no keywords found", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/dave", {
      openToWork: false,
      profileSkills: [{ name: "React" }],
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "Built frontend components",
          skills: ["React", "TypeScript"],
          endDate: { text: "Present" },
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "Dave", linkedinUrl: "https://linkedin.com/in/dave" }),
    ];

    const result = filterByKeywordsInApifyData(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    }, ["incident response", "SRE", "on-call"]);

    expect(result.matched).toHaveLength(0);
    expect(result.unmatched).toHaveLength(1);
  });

  it("is case-insensitive", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/eve", {
      openToWork: false,
      profileSkills: [],
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "Responsible for HIGH AVAILABILITY systems",
          endDate: { text: "Present" },
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "Eve", linkedinUrl: "https://linkedin.com/in/eve" }),
    ];

    const result = filterByKeywordsInApifyData(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    }, ["high availability"]);

    expect(result.matched).toHaveLength(1);
  });

  it("puts employees without Apify data in unmatched", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    const employees = [
      makeEmployee({ name: "NoCache", linkedinUrl: "https://linkedin.com/in/nocache" }),
      makeEmployee({ name: "NoUrl", linkedinUrl: null }),
    ];

    const result = filterByKeywordsInApifyData(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    }, ["SRE"]);

    expect(result.matched).toHaveLength(0);
    expect(result.unmatched).toHaveLength(2);
  });

  it("splits multiple employees into matched and unmatched", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/match", {
      openToWork: false,
      profileSkills: [],
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "SRE team lead managing SLO dashboards",
          endDate: { text: "Present" },
        },
      ],
    });
    cache.set("linkedin.com/in/nomatch", {
      openToWork: false,
      profileSkills: [],
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "Product management",
          endDate: { text: "Present" },
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "Match", linkedinUrl: "https://linkedin.com/in/match" }),
      makeEmployee({ name: "NoMatch", linkedinUrl: "https://linkedin.com/in/nomatch" }),
    ];

    const result = filterByKeywordsInApifyData(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    }, ["SLO", "SRE"]);

    expect(result.matched).toHaveLength(1);
    expect(result.matched[0].name).toBe("Match");
    expect(result.unmatched).toHaveLength(1);
    expect(result.unmatched[0].name).toBe("NoMatch");
  });
});

describe("filterOutHardwareHeavyPeople", () => {
  const cache: ApifyOpenToWorkCache = new Map();

  it("rejects titles containing the standalone word 'hardware'", () => {
    const emp = makeEmployee({ name: "Alice", linkedinUrl: null, currentTitle: "Hardware Engineer" });
    const result = filterOutHardwareHeavyPeople([emp], cache);
    expect(result.rejected).toHaveLength(1);
    expect(result.kept).toHaveLength(0);
  });

  it("rejects titles containing the standalone word 'HW'", () => {
    const emp = makeEmployee({ name: "Bob", linkedinUrl: null, currentTitle: "HW Engineer" });
    const result = filterOutHardwareHeavyPeople([emp], cache);
    expect(result.rejected).toHaveLength(1);
    expect(result.kept).toHaveLength(0);
  });

  it("does not reject titles where 'hw' is a substring of a longer word", () => {
    const emp = makeEmployee({ name: "Carol", linkedinUrl: null, currentTitle: "webhook Infrastructure Engineer" });
    const result = filterOutHardwareHeavyPeople([emp], cache);
    expect(result.kept).toHaveLength(1);
    expect(result.rejected).toHaveLength(0);
  });

});
