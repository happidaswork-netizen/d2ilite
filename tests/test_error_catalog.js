// minimal DOM-free unit check for explainError / countByReason via node
const fs = require("fs");
const path = require("path");
const src = fs.readFileSync(path.join("cloud/web/common.js"), "utf8");
// Evaluate the IIFE in a fake browser env
const fake = {
  window: { location: { href: "http://localhost/" }, history: { replaceState() {} }, prompt: () => "" },
  localStorage: { getItem: () => "", setToken() {}, setItem() {}, removeItem() {} },
  document: { getElementById: () => null },
  URL,
  fetch: async () => ({ ok: true, headers: { get: () => "" }, text: async () => "{}", blob: async () => new Blob() }),
  AbortController,
  setTimeout,
  clearTimeout,
};
const sandbox = { ...fake, window: fake.window };
sandbox.window = sandbox;
sandbox.localStorage = fake.localStorage;
sandbox.document = fake.document;
const fn = new Function("window", "localStorage", "document", "URL", "fetch", "AbortController", "setTimeout", "clearTimeout", src + "\nreturn window.D2I;");
const D2I = fn(sandbox, fake.localStorage, fake.document, URL, fake.fetch, AbortController, setTimeout, clearTimeout);

function assert(cond, msg) {
  if (!cond) throw new Error(msg);
}

const a = D2I.explainError("image_not_found");
assert(a.label === "缺图", "image_not_found label");
assert(a.severity === "must_recrawl", "image_not_found sev");

const b = D2I.explainError("TypeError: boom");
assert(b.severity === "review", "free text severity");
assert(b.label.includes("TypeError") || b.raw.includes("TypeError"), "free text kept");

const c = D2I.explainError("ambiguous_name_match");
assert(c.label === "同名歧义", "ambiguous");

const groups = D2I.countByReason(
  [
    { error: "image_not_found" },
    { error: "image_not_found" },
    { error: "multi_person" },
    { error: "" },
  ],
  "error"
);
assert(groups[0].code === "image_not_found" && groups[0].count === 2, "count order");
assert(groups.some((g) => g.code === "multi_person" && g.count === 1), "multi");

console.log("PASS explainError + countByReason");
