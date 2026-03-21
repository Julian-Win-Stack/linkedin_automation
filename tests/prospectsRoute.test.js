"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const supertest_1 = __importDefault(require("supertest"));
const vitest_1 = require("vitest");
const prospects_1 = __importDefault(require("../src/routes/prospects"));
const getCompanyMock = vitest_1.vi.fn();
const searchPeopleMock = vitest_1.vi.fn();
vitest_1.vi.mock("../src/services/getCompany", () => ({
    getCompany: (...args) => getCompanyMock(...args),
}));
vitest_1.vi.mock("../src/services/searchPeople", () => ({
    searchPeople: (...args) => searchPeopleMock(...args),
}));
function createTestApp() {
    const app = (0, express_1.default)();
    app.use(express_1.default.json());
    app.use("/api/v1/prospects", prospects_1.default);
    return app;
}
(0, vitest_1.describe)("POST /api/v1/prospects/by-company", () => {
    (0, vitest_1.beforeEach)(() => {
        getCompanyMock.mockReset();
        searchPeopleMock.mockReset();
    });
    (0, vitest_1.it)("returns 400 when companyQuery is missing", async () => {
        const app = createTestApp();
        const response = await (0, supertest_1.default)(app).post("/api/v1/prospects/by-company").send({});
        (0, vitest_1.expect)(response.status).toBe(400);
    });
    (0, vitest_1.it)("returns matching people when request is valid", async () => {
        getCompanyMock.mockResolvedValue({
            companyName: "Acme",
            domain: "acme.com",
            linkedinUrl: null,
        });
        searchPeopleMock.mockResolvedValue([
            {
                name: "A Person",
                title: "Platform Engineer",
                company: "Acme",
                linkedinUrl: null,
                tenureMonths: 12,
            },
        ]);
        const app = createTestApp();
        const response = await (0, supertest_1.default)(app)
            .post("/api/v1/prospects/by-company")
            .send({ companyQuery: "acme.com", maxResults: 25 });
        (0, vitest_1.expect)(response.status).toBe(200);
        (0, vitest_1.expect)(response.body.data).toHaveLength(1);
        (0, vitest_1.expect)(searchPeopleMock).toHaveBeenCalledWith({
            companyName: "Acme",
            domain: "acme.com",
            linkedinUrl: null,
        }, 25);
    });
});
