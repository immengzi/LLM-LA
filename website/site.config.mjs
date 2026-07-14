// Central site constants shared by astro.config.mjs and scripts/sync-docs.mjs.
// Keep these in sync with the GitHub repo and Pages deployment target.
//
// GitHub Project Pages serve at https://<org>.github.io/<repo>/, so `BASE`
// must be `/<repo>`. If you later attach a custom domain, set BASE to '/'.
export const GH_ORG = "LA-Boom";
export const GH_REPO = "llm-la";
export const GH_BRANCH = "main";

export const SITE_URL = `https://${GH_ORG.toLowerCase()}.github.io`;
export const BASE = `/${GH_REPO}`;

export const GH_REPO_URL = `https://github.com/${GH_ORG}/${GH_REPO}`;
export const GH_BLOB = `${GH_REPO_URL}/blob/${GH_BRANCH}`;
export const GH_TREE = `${GH_REPO_URL}/tree/${GH_BRANCH}`;
export const GH_EDIT = `${GH_REPO_URL}/edit/${GH_BRANCH}`;
