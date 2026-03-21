"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const searchPeople_1 = require("../src/services/searchPeople");
const apolloPostMock = vitest_1.vi.fn();
vitest_1.vi.mock("../src/services/apolloClient", () => ({
    apolloPost: (...args) => apolloPostMock(...args),
}));
(0, vitest_1.describe)("searchPeople", () => {
    const company = {
        companyName: "Acme",
        domain: "acme.com",
        linkedinUrl: null,
    };
    (0, vitest_1.beforeEach)(() => {
        apolloPostMock.mockReset();
    });
    (0, vitest_1.it)("filters people by title keywords", async () => {
        apolloPostMock.mockResolvedValue({
            people: [
                {
                    name: "A Person",
                    title: "Platform Engineer",
                    organization_name: "Acme",
                },
                {
                    name: "B Person",
                    title: "Frontend Engineer",
                    organization_name: "Acme",
                },
            ],
            pagination: { page: 1, total_pages: 1 },
        });
        const result = await (0, searchPeople_1.searchPeople)(company, 25);
        (0, vitest_1.expect)(result).toHaveLength(1);
        (0, vitest_1.expect)(result[0].name).toBe("A Person");
    });
});
