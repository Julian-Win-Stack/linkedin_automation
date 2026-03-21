"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vitest_1 = require("vitest");
const computeTenure_1 = require("../src/services/computeTenure");
(0, vitest_1.describe)("computeTenure", () => {
    (0, vitest_1.it)("returns tenure months for current role at target company", () => {
        const now = new Date("2026-03-20T00:00:00.000Z");
        const history = [
            {
                organization_name: "Acme",
                start_date: "2025-01-15",
                end_date: null,
                current: true,
            },
        ];
        const result = (0, computeTenure_1.computeTenure)(history, "Acme", now);
        (0, vitest_1.expect)(result).toBe(14);
    });
    (0, vitest_1.it)("returns null when employment history is missing", () => {
        const result = (0, computeTenure_1.computeTenure)(undefined, "Acme", new Date("2026-03-20T00:00:00.000Z"));
        (0, vitest_1.expect)(result).toBeNull();
    });
    (0, vitest_1.it)("returns null when start date is missing", () => {
        const history = [
            {
                organization_name: "Acme",
                end_date: null,
                current: true,
            },
        ];
        const result = (0, computeTenure_1.computeTenure)(history, "Acme", new Date("2026-03-20T00:00:00.000Z"));
        (0, vitest_1.expect)(result).toBeNull();
    });
});
