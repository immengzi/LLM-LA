/**
 * sync-docs.mjs — ingest the repo's docs/ tree into Starlight's content
 * collection (website/src/content/docs/). Runs automatically before `dev` and
 * `build` (see package.json). docs/ stays the single source of truth.
 *
 * For every docs/**.md file it:
 *   1. lowercases the output path (stable, case-insensitive routes),
 *   2. injects Starlight frontmatter (title from the first H1, a short
 *      description, and sidebar order/label),
 *   3. rewrites links:
 *        - in-tree .md links   -> base-absolute Starlight routes,
 *        - in-tree directories -> GitHub tree URLs (no index pages on-site),
 *        - out-of-tree targets -> GitHub blob/tree URLs (src/, infra/, ...).
 *
 * The top-level docs/README.md is skipped: the custom landing page and the
 * sidebar replace it.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { BASE, GH_BLOB, GH_TREE } from "../site.config.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WEBSITE_DIR = path.resolve(__dirname, "..");
const REPO_ROOT = path.resolve(WEBSITE_DIR, "..");
const DOCS_SRC = path.join(REPO_ROOT, "docs");
const OUT_DIR = path.join(WEBSITE_DIR, "src", "content", "docs");

// "" when BASE is root ("/"), else the base without a trailing slash (e.g.
// "/llm-la"). Used to build base-absolute in-tree links without double slashes.
const BASE_HREF = BASE === "/" ? "" : BASE.replace(/\/+$/, "");

// Section directories (mirrors docs/) that this script fully owns/regenerates.
// Hand-authored files at the docs root (e.g. index.mdx) are never touched.
const SECTION_DIRS = [
  "getting-started",
  "architecture",
  "configuration",
  "deployment",
  "operations",
  "gateways",
  "benchmarking",
  "comparisons",
  "contributing",
  "internal",
];

// Curated sidebar order + optional label overrides, keyed by lowercased path
// relative to docs/. Anything not listed sorts last (order 999).
const ORDER = {
  "getting-started/quickstart.md": { order: 1 },
  "getting-started/prerequisites.md": { order: 2 },
  "getting-started/first-experiment.md": { order: 3 },

  "architecture/overview.md": { order: 1 },
  "architecture/router-strategies.md": { order: 2 },
  "architecture/router.md": { order: 3 },
  "architecture/sidecar.md": { order: 4 },
  "architecture/kv-cache-flow.md": { order: 5 },
  "architecture/key-affinity.md": { order: 6 },
  "architecture/prefix-hash.md": { order: 7 },
  "architecture/slo-aware-routing.md": { order: 8 },
  "architecture/trace.md": { order: 9 },
  "architecture/go-services.md": { order: 10 },

  "configuration/client-config.md": { order: 1 },
  "configuration/helm-values.md": { order: 2 },
  "configuration/experiment-configs.md": { order: 3 },

  "deployment/multi-model.md": { order: 1 },
  "deployment/data-parallel-lws.md": { order: 2 },
  "deployment/lmcache-p2p-build.md": { order: 3 },
  "deployment/docker-reference/glm5-dp-docker.md": { order: 4 },
  "deployment/docker-reference/mooncake-pd-test.md": { order: 5 },
  "deployment/mooncake/helm-integration.md": { order: 6 },
  "deployment/mooncake/glm5-production.md": { order: 7 },

  "operations/cluster-setup.md": { order: 1 },
  "operations/cluster-prep-automation.md": { order: 2 },
  "operations/multi-node-setup-guide.md": { order: 3 },
  "operations/registry.md": { order: 4 },
  "operations/image-patches.md": { order: 5 },
  "operations/autoscaling.md": { order: 6 },
  "operations/bz-cluster-nodes.md": { order: 7 },
  "operations/bz-dashboard-access.md": { order: 8 },
  "operations/disaster-recovery.md": { order: 9 },
  "operations/k8s-dns-troubleshooting.md": { order: 10 },
  "operations/docker-proxy-fix.md": { order: 11 },
  "operations/aibrix-long-running-requests.md": { order: 12 },
  "operations/claude-code-setup.md": { order: 13 },
  "operations/switch_cluster.md": { order: 14 },

  "gateways/boom/overview.md": { order: 1 },
  "gateways/boom/models.md": { order: 2 },
  "gateways/boom/build.md": { order: 3 },
  "gateways/boom/claude-code.md": { order: 4 },
  "gateways/boom/boom-gateway-openeuler-walkthrough.zh.md": {
    order: 5,
    label: "openEuler walkthrough (ZH)",
  },
  "gateways/litellm/claude-code.md": { order: 1 },

  "benchmarking/harness.md": { order: 1 },
  "benchmarking/load-patterns.md": { order: 2 },
  "benchmarking/multi-turn.md": { order: 3 },
  "benchmarking/codeflowbench.md": { order: 4 },
  "benchmarking/artifacts-and-analysis.md": { order: 5 },

  "comparisons/pull-vs-aibrix-lr.md": { order: 1 },
  "comparisons/pull-vs-aibrix-kv.md": { order: 2 },

  "contributing/development.md": { order: 1, label: "Contributing" },
  "contributing/testing.md": { order: 2, label: "Testing" },

  "internal/vision.md": { order: 1 },
  "internal/open-sourcing-plan.md": { order: 2 },
  "internal/open-sourcing-v01.md": { order: 3 },
  "internal/boom-integration-notes.md": { order: 4 },
  "internal/stability-test-findings.md": { order: 5 },
  "internal/kv-cache-hit-rate-collapse.md": { order: 6 },
  "internal/lmcache-p2p-host-staging.md": { order: 7 },
  "internal/persistent-affinity-map.md": { order: 8 },
};

function yamlString(value) {
  return `"${String(value).replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
}

/** Collect *.md files under a directory (recursively), returned posix-relative. */
function walk(dir, rootForRel) {
  const out = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const abs = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      out.push(...walk(abs, rootForRel));
    } else if (entry.isFile() && entry.name.toLowerCase().endsWith(".md")) {
      out.push(path.relative(rootForRel, abs).split(path.sep).join("/"));
    }
  }
  return out;
}

/** Extract the first `# H1` as the title and strip that line from the body. */
function extractTitle(body, fallback) {
  const lines = body.split("\n");
  for (let i = 0; i < lines.length; i++) {
    const m = /^#\s+(.+?)\s*$/.exec(lines[i]);
    if (m) {
      const title = m[1].replace(/`/g, "").replace(/\*\*/g, "").trim();
      lines.splice(i, 1);
      if (lines[i] !== undefined && lines[i].trim() === "") lines.splice(i, 1);
      return { title, body: lines.join("\n") };
    }
  }
  return { title: fallback, body };
}

/** Derive a short plain-text description from the first prose paragraph. */
function extractDescription(body) {
  const lines = body.split("\n");
  for (const raw of lines) {
    const line = raw.trim();
    if (!line) continue;
    if (/^[#>|`\-*!\[]/.test(line)) continue; // skip headings, quotes, tables, fences, lists, images
    if (line.startsWith("<")) continue; // skip raw HTML
    const text = line
      .replace(/!?\[([^\]]*)\]\([^)]*\)/g, "$1") // links/images -> text
      .replace(/[`*_]/g, "")
      .replace(/\s+/g, " ")
      .trim();
    if (text.length < 20) continue;
    return text.length > 180 ? text.slice(0, 177).trimEnd() + "..." : text;
  }
  return "";
}

/** Compute the Starlight route slug for an in-tree docs path (lowercased). */
function slugFor(docsRelLower) {
  let slug = docsRelLower.replace(/\.md$/, "");
  slug = slug.replace(/\/index$/, "").replace(/^index$/, "");
  return slug;
}

function rewriteLink(url, currentDocsRel) {
  const trimmed = url.trim();
  if (
    trimmed === "" ||
    trimmed.startsWith("#") ||
    trimmed.startsWith("http://") ||
    trimmed.startsWith("https://") ||
    trimmed.startsWith("//") ||
    trimmed.startsWith("mailto:")
  ) {
    return url;
  }

  const hashIdx = trimmed.indexOf("#");
  const linkPath = hashIdx >= 0 ? trimmed.slice(0, hashIdx) : trimmed;
  const frag = hashIdx >= 0 ? trimmed.slice(hashIdx) : "";
  if (linkPath === "") return url; // pure anchor

  const currentDir = "docs/" + path.posix.dirname(currentDocsRel);
  const targetRepo = path.posix.normalize(path.posix.join(currentDir, linkPath));
  const endsWithSlash = /\/$/.test(linkPath);
  const looksLikeFile = /\.[a-z0-9]+$/i.test(path.posix.basename(targetRepo));

  if (targetRepo.startsWith("docs/")) {
    const docsRel = targetRepo.slice("docs/".length);
    if (docsRel.toLowerCase() === "readme.md") return `${BASE_HREF}/${frag}`;
    if (targetRepo.toLowerCase().endsWith(".md")) {
      const slug = slugFor(docsRel.toLowerCase());
      return `${BASE_HREF}/${slug}/${frag}`;
    }
    // In-tree directory or non-page file -> GitHub source.
    return endsWithSlash || !looksLikeFile
      ? `${GH_TREE}/${targetRepo}${frag}`
      : `${GH_BLOB}/${targetRepo}${frag}`;
  }

  if (targetRepo.startsWith("..")) return url; // escapes repo root; leave as authored
  return endsWithSlash || !looksLikeFile
    ? `${GH_TREE}/${targetRepo}${frag}`
    : `${GH_BLOB}/${targetRepo}${frag}`;
}

function rewriteBody(body, currentDocsRel) {
  // Rewrite the URL portion of Markdown links: ](target)
  return body.replace(/\]\(([^)\s]+)\)/g, (_m, url) => {
    return `](${rewriteLink(url, currentDocsRel)})`;
  });
}

function main() {
  if (!fs.existsSync(DOCS_SRC)) {
    console.error(`[sync-docs] docs/ not found at ${DOCS_SRC}`);
    process.exit(1);
  }

  // Clear generated sections + generated root files; keep hand-authored files.
  for (const dir of SECTION_DIRS) {
    fs.rmSync(path.join(OUT_DIR, dir), { recursive: true, force: true });
  }
  fs.mkdirSync(OUT_DIR, { recursive: true });

  const files = walk(DOCS_SRC, DOCS_SRC).sort();
  let written = 0;

  for (const rel of files) {
    if (rel.toLowerCase() === "readme.md") continue; // replaced by landing + nav

    const abs = path.join(DOCS_SRC, rel);
    const raw = fs.readFileSync(abs, "utf8");

    const fallback = path.posix.basename(rel).replace(/\.md$/, "");
    const { title, body: noTitle } = extractTitle(raw, fallback);
    const description = extractDescription(noTitle);
    const body = rewriteBody(noTitle, rel);

    const meta = ORDER[rel.toLowerCase()] || { order: 999 };
    const fm = ["---", `title: ${yamlString(title)}`];
    if (description) fm.push(`description: ${yamlString(description)}`);
    fm.push("sidebar:");
    fm.push(`  order: ${meta.order}`);
    if (meta.label) fm.push(`  label: ${yamlString(meta.label)}`);
    fm.push("---", "");

    const outRel = rel.toLowerCase();
    const outPath = path.join(OUT_DIR, outRel);
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    fs.writeFileSync(outPath, fm.join("\n") + body.replace(/^\n+/, ""), "utf8");
    written++;
  }

  console.log(`[sync-docs] wrote ${written} pages into src/content/docs/`);
}

main();
