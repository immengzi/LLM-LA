# LA-Boom website

The project website (marketing landing page + full documentation), built with
[Astro](https://astro.build) + [Starlight](https://starlight.astro.build) and
deployed to GitHub Pages.

## How it works

`docs/` at the repo root is the **single source of truth**. A prebuild script,
[`scripts/sync-docs.mjs`](scripts/sync-docs.mjs), mirrors that tree into
Starlight's content collection (`src/content/docs/`) on every `dev` / `build`,
injecting the frontmatter Starlight needs (title from the first `# H1`, a short
description, sidebar order) and rewriting cross-links:

- in-tree `.md` links → base-absolute site routes,
- links into `src/`, `infra/`, etc. → GitHub source URLs.

The generated section folders under `src/content/docs/` are git-ignored; only the
hand-authored landing page (`src/content/docs/index.mdx`) is committed. Edit the
docs in `docs/`, not in `src/content/docs/`.

## Local development

```bash
cd website
npm install
npm run dev      # runs sync-docs, then serves at http://localhost:4321/llm-la
```

Other commands:

```bash
npm run sync     # regenerate src/content/docs/ from docs/ only
npm run build    # sync + production build into dist/
npm run preview  # preview the production build locally
```

## Configuration

Site URL, base path, and GitHub coordinates live in
[`site.config.mjs`](site.config.mjs) (shared by `astro.config.mjs` and the sync
script). For a project page the base is `/llm-la`; if you attach a custom domain,
set `BASE` to `/`.

## Deployment

Pushes to `main` that touch `website/**` or `docs/**` trigger
[`.github/workflows/deploy-website.yml`](../.github/workflows/deploy-website.yml),
which builds with Astro and publishes to GitHub Pages.

**One-time setup:** in the repo, go to **Settings → Pages → Build and deployment
→ Source** and select **GitHub Actions**. The first deploy only succeeds after
this is enabled. The published URL is `https://la-boom.github.io/llm-la/`.
