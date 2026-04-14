# BooM Gateway Integration

This folder contains everything needed to integrate BooM Gateway into the
mu-load-test / vllm-kv-stack framework as a `backend: "boom"` option.

BooM Gateway is an external Rust-based LLM API gateway (not owned by us).
This folder documents the additions we made on top of it and the exact
steps to build, deploy, and run it.

---

## What's in this folder

```
boom-integration/
├── README.md                    # This file
├── Dockerfile                   # Container image build (copy to BooMGateway-main/)
├── BUILD.md                     # Exact build steps that worked on our cluster
├── INTEGRATION.md               # What was changed in our framework code
├── cargo-config.toml            # Cargo config for host build (proxy + rsproxy mirror)
└── boom_config_example.yaml     # Example BooM Gateway runtime config (for reference)
```

## Quick start

1. Copy `Dockerfile` into the BooM Gateway repo root
2. Follow `BUILD.md` to compile and push the image
3. Apply the framework changes (already in our repo) or use `apply_boom_patch.sh`
4. Run: `python sweep_methods.py --config boom_master --skip-vllm`
