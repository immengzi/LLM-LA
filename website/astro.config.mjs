// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import mermaid from "astro-mermaid";
import { SITE_URL, BASE, GH_REPO_URL, GH_EDIT } from "./site.config.mjs";

// https://astro.build/config
export default defineConfig({
  site: SITE_URL,
  base: BASE,
  // Directory-style output so Starlight routes resolve consistently on Pages.
  trailingSlash: "always",
  integrations: [
    // astro-mermaid must be registered before Starlight so it can hook the
    // Markdown/MDX pipeline; it renders ```mermaid fences client-side and
    // follows the active light/dark theme.
    mermaid({ theme: "default", autoTheme: true }),
    starlight({
      title: "LA-Boom",
      description:
        "KV-aware load balancing and benchmarking for vLLM on Kubernetes — at cluster scale.",
      logo: { src: "./src/assets/logo.svg", alt: "LA-Boom" },
      favicon: "/favicon.svg",
      customCss: ["./src/styles/custom.css"],
      social: [{ icon: "github", label: "GitHub", href: GH_REPO_URL }],
      // Generated pages mirror docs/ (lowercased); edit links point back at the
      // source-of-truth Markdown under docs/.
      editLink: { baseUrl: `${GH_EDIT}/docs/` },
      lastUpdated: true,
      head: [
        {
          tag: "meta",
          attrs: { property: "og:image", content: `${SITE_URL}${BASE}/og.svg` },
        },
        {
          tag: "meta",
          attrs: { name: "twitter:card", content: "summary_large_image" },
        },
      ],
      sidebar: [
        { label: "Getting Started", autogenerate: { directory: "getting-started" } },
        { label: "Architecture", autogenerate: { directory: "architecture" } },
        { label: "Configuration", autogenerate: { directory: "configuration" } },
        { label: "Deployment", autogenerate: { directory: "deployment" } },
        { label: "Operations", autogenerate: { directory: "operations" } },
        { label: "Gateways", autogenerate: { directory: "gateways" } },
        { label: "Benchmarking", autogenerate: { directory: "benchmarking" } },
        { label: "Comparisons", autogenerate: { directory: "comparisons" } },
        {
          label: "Design & Planning (internal)",
          collapsed: true,
          autogenerate: { directory: "internal" },
        },
      ],
    }),
  ],
});
