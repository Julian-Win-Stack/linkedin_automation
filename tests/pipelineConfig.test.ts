import { describe, expect, it } from "vitest";
import { loadPipelineConfig } from "../src/config/pipelineConfig";

describe("loadPipelineConfig", () => {
  it("returns the hardcoded column-name candidates", () => {
    const config = loadPipelineConfig();
    expect(config.nameColumn).toEqual(["Company Name"]);
    expect(config.domainColumn).toEqual(["Website"]);
    expect(config.apolloAccountIdColumn).toEqual(["Apollo Account Id"]);
    expect(config.linkedinUrlColumn).toEqual(["Company Linkedin Url"]);
  });
});
