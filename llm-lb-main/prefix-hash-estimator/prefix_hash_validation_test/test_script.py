#!/usr/bin/env python3
"""
Integrated test script for vLLM prefix caching validation.

This script:
1. Starts vLLM server with prefix caching enabled
2. Starts KV event listener in background
3. Sends a test prompt to the server
4. Captures KV events from the server
5. Computes expected hashes using the prefix hash estimator
6. Compares and validates the results
"""

import os
import sys
import time
import subprocess
import threading
import signal
import requests
from queue import Queue, Empty
# from typing import Optional, List, Dict, Any
from typing import Optional, List, Dict, Any, Union
import msgspec
import zmq
from msgspec.msgpack import Decoder

# Set hash seed for reproducibility
os.environ["PYTHONHASHSEED"] = "0"

# ============================================================================
# KV Event Structures (from kv_event_listener.py)
# ============================================================================

BlockHash = int

class EventBatch(msgspec.Struct, array_like=True, omit_defaults=True, gc=False):
    ts: float
    events: list[Any]

class KVCacheEvent(
    msgspec.Struct, array_like=True, omit_defaults=True, gc=False, tag=True
):
    """Base class for all KV cache-related events"""

class BlockStored(KVCacheEvent):
    block_hashes: list[BlockHash]
    parent_block_hash: Optional[BlockHash]
    token_ids: list[int]
    block_size: int
    lora_id: Optional[int]

class BlockRemoved(KVCacheEvent):
    block_hashes: list[BlockHash]

class AllBlocksCleared(KVCacheEvent):
    pass

# class KVEventBatch(EventBatch):
#     events: list[Any]

class KVEventBatch(EventBatch):
    events: list[Union[BlockStored, BlockRemoved, AllBlocksCleared]]


# ============================================================================
# Prefix Hash Computation (from compute_prefix_hash.py)
# ============================================================================

def compute_prefix_hashes(prompt_text: str, model_path: str, block_size: int = 128) -> tuple:
    """
    Compute block hashes using vLLM's actual Request class.
    Returns: (block_hashes_as_int, prompt_token_ids)
    """
    from transformers import AutoTokenizer
    from vllm.sampling_params import SamplingParams
    from vllm.v1.request import Request
    from vllm.v1.core.kv_cache_utils import (
        get_request_block_hasher,
        init_none_hash,
        maybe_convert_block_hash,
    )
    from vllm.utils.hashing import sha256_cbor
    
    # Initialize NONE_HASH once
    init_none_hash(sha256_cbor)
    
    # Load tokenizer
    tokenizer = AutoTokenizer.from_pretrained(
        model_path, 
        trust_remote_code=True, 
        local_files_only=True
    )
    
    # Tokenize prompt
    prompt_token_ids = tokenizer.encode(prompt_text, add_special_tokens=False)
    
    print(f"📝 Tokenized prompt: {len(prompt_token_ids)} tokens")
    
    # Create sampling params
    sampling_params = SamplingParams(
        ignore_eos=False,
        max_tokens=32,
        stop_token_ids=None,
        prompt_logprobs=None,
    )
    
    # Create block hasher
    block_hasher = get_request_block_hasher(block_size, sha256_cbor)
    
    # Create Request object
    req = Request(
        request_id="compute-hash-demo",
        prompt_token_ids=prompt_token_ids,
        sampling_params=sampling_params,
        pooling_params=None,
        eos_token_id=151643,  # Qwen's EOS token
        client_index=0,
        arrival_time=time.time(),
        prompt_embeds=None,
        mm_features=None,
        lora_request=None,
        cache_salt=None,
        priority=0,
        trace_headers=None,
        block_hasher=block_hasher,
    )
    
    # Convert block hashes to int format
    block_hashes_as_int = []
    for block_hash in req.block_hashes:
        hash_int = maybe_convert_block_hash(block_hash)
        block_hashes_as_int.append(hash_int)
    # print(f"estimated hash block:{block_hashes_as_int}")
    
    return block_hashes_as_int, prompt_token_ids


# ============================================================================
# KV Event Listener Thread
# ============================================================================

class KVEventListener:
    """Background thread to listen for KV cache events."""
    
    def __init__(self, pub_port: int = 5589, replay_port: int = 5578):
        self.pub_port = pub_port
        self.replay_port = replay_port
        self.event_queue = Queue()
        self.running = False
        self.thread = None
        self.decoder = Decoder(type=KVEventBatch)
        
    def start(self):
        """Start the listener thread."""
        self.running = True
        self.thread = threading.Thread(target=self._listen_loop, daemon=True)
        self.thread.start()
        print(f"🎧 KV Event Listener started (pub={self.pub_port}, replay={self.replay_port})")
        
    def stop(self):
        """Stop the listener thread."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=2)
        print("🛑 KV Event Listener stopped")
        
    def _listen_loop(self):
        """Main listening loop (runs in background thread)."""
        context = zmq.Context()
        
        sub = context.socket(zmq.SUB)
        sub.connect(f"tcp://localhost:{self.pub_port}")
        topic = "kv-events"
        sub.setsockopt_string(zmq.SUBSCRIBE, topic)
        
        replay = context.socket(zmq.REQ)
        replay.connect(f"tcp://localhost:{self.replay_port}")
        
        poller = zmq.Poller()
        poller.register(replay, zmq.POLLIN)
        
        last_seq = -1
        
        while self.running:
            try:
                if sub.poll(50):
                    _, seq_bytes, payload = sub.recv_multipart()
                    seq = int.from_bytes(seq_bytes, "big")
                    
                    # Handle missed messages
                    if last_seq >= 0 and seq > last_seq + 1:
                        missed = seq - last_seq - 1
                        print(f"⚠️  Missed {missed} messages, requesting replay...")
                        replay.send((last_seq + 1).to_bytes(8, "big"))
                        
                        while poller.poll(timeout=200):
                            seq_bytes, replay_payload = replay.recv_multipart()
                            if not replay_payload:
                                break
                            replay_seq = int.from_bytes(seq_bytes, "big")
                            if replay_seq > last_seq:
                                try:
                                    event_batch = self.decoder.decode(replay_payload)
                                    self.event_queue.put(event_batch)
                                except msgspec.ValidationError as e:
                                    print(f"❌ Replay decode error: {e}")
                                last_seq = replay_seq
                                if replay_seq >= seq - 1:
                                    break
                    
                    # Process current message
                    try:
                        event_batch = self.decoder.decode(payload)
                        self.event_queue.put(event_batch)
                    except msgspec.ValidationError as e:
                        print(f"❌ Live decode error: {e}")
                    
                    last_seq = seq
                    
            except Exception as e:
                if self.running:
                    print(f"❌ Listener error: {e}")
        
        sub.close()
        replay.close()
        context.term()
    
    def get_events(self, timeout: float = 5.0) -> List[EventBatch]:
        """Get all events from the queue within timeout period."""
        events = []
        deadline = time.time() + timeout
        
        while time.time() < deadline:
            try:
                event = self.event_queue.get(timeout=0.1)
                events.append(event)
            except Empty:
                continue
        
        return events


# ============================================================================
# Docker Process Manager
# ============================================================================

class VLLMDockerManager:
    """Manages the vLLM Docker container lifecycle."""
    
    def __init__(self):
        self.process = None
        self.container_name = "vllm-ascend-hw"
        
    def start(self, model_path: str):
        """Start the vLLM Docker container."""
        print("🐳 Starting vLLM Docker container...")
        
        # First, ensure no existing container is running
        self._cleanup_existing()
        
        docker_cmd = [
            "docker", "run", "--rm", "--name", self.container_name, "-it",
            "--device", "/dev/davinci2",
            "--device", "/dev/davinci_manager",
            "--device", "/dev/devmm_svm",
            "--device", "/dev/hisi_hdc",
            "-v", "/usr/local/dcmi:/usr/local/dcmi",
            "-v", "/usr/local/bin/npu-smi:/usr/local/bin/npu-smi",
            "-v", "/usr/local/Ascend/driver/lib64/:/usr/local/Ascend/driver/lib64/",
            "-v", "/usr/local/Ascend/driver/version.info:/usr/local/Ascend/driver/version.info",
            "-v", "/etc/ascend_install.info:/etc/ascend_install.info",
            "-v", "/root/.cache:/root/.cache",
            "-p", "8100:8000",
            "-p", "5589:5557",
            "-p", "5578:5558",
            "-v", f"{model_path}:/model:ro",
            "--shm-size", "8g",
            "-e", "HF_HUB_DISABLE_TELEMETRY=1",
            "-e", "VLLM_ALLOW_RUNTIME_DOWNLOADS=0",
            "-e", "VLLM_NO_HF_ACCESS=1",
            "-e", "PYTHONHASHSEED=0",
            "--entrypoint", "python",
            "quay.io/ascend/vllm-ascend:v0.11.0rc0",
            "-m", "vllm.entrypoints.openai.api_server",
            "--model", "/model",
            "--trust-remote-code",
            "--served-model-name", "qwen-local",
            "--port", "8000",
            "--enable-prefix-caching",
            "--prefix-caching-hash-algo", "sha256_cbor",
            "--gpu-memory-utilization", "0.80",
            "--max-model-len", "4096",
            "--kv-events-config", '{"enable_kv_cache_events": true, "publisher": "zmq", "endpoint": "tcp://*:5557", "replay_endpoint": "tcp://*:5558", "topic": "kv-events"}'
        ]
        
        self.process = subprocess.Popen(
            docker_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        print("⏳ Waiting for vLLM server to be ready...")
        if not self._wait_for_server(timeout=180):
            raise RuntimeError("vLLM server failed to start within timeout")
        
        print("✅ vLLM server is ready!")
        
    def _cleanup_existing(self):
        """Remove any existing container with the same name."""
        try:
            subprocess.run(
                ["docker", "rm", "-f", self.container_name],
                capture_output=True,
                timeout=10
            )
        except:
            pass
    
    def _wait_for_server(self, timeout: int = 180) -> bool:
        """Wait for the server to be ready."""
        url = "http://localhost:8100/v1/models"
        deadline = time.time() + timeout
        
        while time.time() < deadline:
            try:
                response = requests.get(url, timeout=2)
                if response.status_code == 200:
                    return True
            except requests.exceptions.RequestException:
                pass
            
            # Check if process is still running
            if self.process.poll() is not None:
                print("❌ vLLM server process terminated unexpectedly")
                return False
            
            time.sleep(2)
        
        return False
    
    def stop(self):
        """Stop the vLLM Docker container."""
        print("🛑 Stopping vLLM Docker container...")
        try:
            subprocess.run(
                ["docker", "stop", self.container_name],
                capture_output=True,
                timeout=30
            )
        except:
            pass
        
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()


# ============================================================================
# Main Test Orchestrator
# ============================================================================

def send_prompt_to_server(prompt: str, url: str = "http://localhost:8100/v1/completions") -> dict:
    """Send a prompt to the vLLM server."""
    payload = {
        "model": "qwen-local",
        "prompt": prompt,
        "max_tokens": 32,
        "temperature": 0.0,
    }
    
    print(f"📤 Sending prompt to server...")
    response = requests.post(url, json=payload, timeout=60)
    response.raise_for_status()
    return response.json()


def compare_hashes(computed_hashes: List[int], kv_events: List[EventBatch], token_ids: List[int], block_size: int = 128):
    """Compare computed hashes with KV event hashes."""
    print("\n" + "="*40)
    print("COMPARISON RESULTS")
    print("="*40)
    
    # Extract BlockStored events
    block_stored_events = []
    for event_batch in kv_events:
        for event in event_batch.events:
            if isinstance(event, BlockStored):
                block_stored_events.append(event)
    
    if not block_stored_events:
        print("❌ No BlockStored events found in KV events!")
        return False
    
    # Use the first BlockStored event (should be our prompt)
    kv_event = block_stored_events[0]
    vllm_hashes = kv_event.block_hashes
    
    print(f"\n📊 Comparison Summary:")
    print(f"   Computed blocks: {len(computed_hashes)}")
    print(f"   vLLM KV blocks:  {len(vllm_hashes)}")
    print(f"   Total tokens:    {len(token_ids)}")
    print(f"   Block size:      {block_size}")
    print()
    
    if len(computed_hashes) != len(vllm_hashes):
        print(f"⚠️  Block count mismatch!")
        print(f"   This might indicate a partial prompt or different block boundaries.")
        print()
    
    # Compare each block
    all_match = True
    min_blocks = min(len(computed_hashes), len(vllm_hashes))
    
    for i in range(min_blocks):
        computed = computed_hashes[i]
        vllm = vllm_hashes[i]
        match = computed == vllm
        
        # Calculate token range for this block
        start_idx = i * block_size
        end_idx = min(start_idx + block_size, len(token_ids))
        block_tokens = token_ids[start_idx:end_idx]
        
        if match:
            print(f"✅ Block {i}: MATCH")
            print(f"   Hash: {computed} (0x{computed & 0xFFFFFFFFFFFFFFFF:016x})")
            print(f"   Tokens[{start_idx}:{end_idx}]: {len(block_tokens)} tokens")
        else:
            print(f"❌ Block {i}: MISMATCH")
            print(f"   Computed:   {computed} (0x{computed & 0xFFFFFFFFFFFFFFFF:016x})")
            print(f"   vLLM event: {vllm} (0x{vllm & 0xFFFFFFFFFFFFFFFF:016x})")
            print(f"   Tokens[{start_idx}:{end_idx}]: {block_tokens}")
            all_match = False
        print()
    
    print("="*40)
    if all_match and len(computed_hashes) == len(vllm_hashes):
        print("🎉 SUCCESS: ALL BLOCK HASHES MATCH PERFECTLY!")
    elif all_match:
        print("⚠️  PARTIAL SUCCESS: All compared blocks match, but counts differ")
    else:
        print("❌ FAILURE: Some block hashes don't match")
    print("="*40)
    
    return all_match


def main():
    """Main test orchestration."""
    # Configuration
    MODEL_PATH = "/home/haiting/llm-lb/prefix-hash-estimator/qwen-test"
    BLOCK_SIZE = 128
    TEST_PROMPT = "I want you to act as an English pronunciation assistant for Turkish speaking people. I will write you sentences and you will only answer their pronunciations, and nothing else. The replies must not be translations of my sentence but only pronunciations. Pronunciations should use Turkish Latin letters for phonetics. Do not write explanations on replies. My first sentence is how the weather is in Istanbul? I will speak to you in English and you will reply to me in English to practice my spoken English. I want you to keep your reply neat, limiting the reply to 100 words. I want you to strictly correct my grammar mistakes, typos, and factual errors. I want you to ask me a question in your reply. Now let's start practicing, you could ask me a question first. Remember, I want you to strictly correct my grammar mistakes, typos, and factual errors. You will come up with entertaining stories that are engaging, imaginative and captivating for the audience. It can be fairy tales, educational stories or any other type of stories which has the potential to capture people's attention and imagination. Depending on the target audience, you may choose specific themes or topics for your storytelling session e.g., if it's children then you can talk about animals; If it's adults then history-based tales might engage them better etc. My first request is I need an interesting story on perseverance. I will provide you with some information about someone's goals and challenges, and it will be your job to come up with strategies that can help this person achieve their goals. This could involve providing positive affirmations, giving helpful advice or suggesting activities they can do to reach their end goal"
    
    docker_manager = None
    kv_listener = None
    
    try:
        print("="*40)
        print("VLLM PREFIX CACHE INTEGRATION TEST")
        print("="*40)
        print()
        
        # Step 1: Start vLLM Docker container
        docker_manager = VLLMDockerManager()
        docker_manager.start(MODEL_PATH)
        
        # Step 2: Start KV event listener
        kv_listener = KVEventListener(pub_port=5589, replay_port=5578)
        kv_listener.start()
        
        # Wait a bit for listener to be ready
        time.sleep(2)
        
        # Step 3: Send prompt to server
        print(f"\n📝 Test prompt: {TEST_PROMPT[:100]}...")
        print()
        server_response = send_prompt_to_server(TEST_PROMPT)
        print(f"✅ Server responded successfully")
        print(f"   Generated text: {server_response.get('choices', [{}])[0].get('text', '')[:100]}...")
        print()
        
        # Step 4: Wait for and collect KV events
        print("⏳ Waiting for KV events (5 seconds)...")
        time.sleep(5)
        kv_events = kv_listener.get_events(timeout=2)
        print(f"📦 Received {len(kv_events)} event batches")
        # print(f"📦 Received {kv_events}")
        
        # Display KV events
        for idx, event_batch in enumerate(kv_events):
            print(f"\n   Event Batch {idx} (ts={event_batch.ts}):")
            for event in event_batch.events:
                if isinstance(event, BlockStored):
                    print(f"      BlockStored: {len(event.block_hashes)} blocks, {len(event.token_ids)} tokens")
                elif isinstance(event, BlockRemoved):
                    print(f"      BlockRemoved: {len(event.block_hashes)} blocks")
                elif isinstance(event, AllBlocksCleared):
                    print(f"      AllBlocksCleared")
        print()
        
        # Step 5: Compute expected hashes
        print("🔢 Computing expected block hashes...")
        computed_hashes, token_ids = compute_prefix_hashes(TEST_PROMPT, MODEL_PATH, BLOCK_SIZE)
        print(f"✅ Computed {len(computed_hashes)} block hashes")
        print()
        
        # Step 6: Compare results
        success = compare_hashes(computed_hashes, kv_events, token_ids, BLOCK_SIZE)
        
        # Final result
        print()
        if success:
            print("🎊 TEST PASSED: Prefix cache validation successful!")
            return 0
        else:
            print("💥 TEST FAILED: Hash mismatch detected")
            return 1
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        return 130
    
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        # Cleanup
        print("\n" + "="*40)
        print("CLEANUP")
        print("="*40)
        
        if kv_listener:
            kv_listener.stop()
        
        if docker_manager:
            docker_manager.stop()
        
        print("✨ Cleanup complete")


if __name__ == "__main__":
    sys.exit(main())