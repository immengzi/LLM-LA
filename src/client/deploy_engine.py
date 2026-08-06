#!/usr/bin/env python3
"""Engine-neutral deployment entry point.

The implementation remains in ``deploy_vllm`` to preserve the established CLI
and release lifecycle. Both entry points read ``helm.engine_type`` and deploy
the selected vLLM or SGLang runtime.
"""

from deploy_vllm import cli


if __name__ == "__main__":
    cli(prog_name="deploy_engine.py")
