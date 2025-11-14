from typing import Any, Optional, Union
import msgspec
import zmq
from msgspec.msgpack import Decoder

# vLLM is sending int, not bytes
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
    # REMOVED: medium: Optional[str] - This field doesn't exist in v0.10.1rc1

class BlockRemoved(KVCacheEvent):
    block_hashes: list[BlockHash]
    # REMOVED: medium: Optional[str] - This field doesn't exist in v0.10.1rc1

class AllBlocksCleared(KVCacheEvent):
    pass

class KVEventBatch(EventBatch):
    events: list[Union[BlockStored, BlockRemoved, AllBlocksCleared]]

def process_event(event_batch):
    print(f"\n{'='*80}")
    print(f"Event batch received at {event_batch.ts}")
    print(f"{'='*80}")
    
    for event in event_batch.events:
        # UNCOMMENTED: Now we can process the events in detail
        if isinstance(event, BlockStored):
            print(f"\n📦 BlockStored Event:")
            print(f"   Block size: {event.block_size}")
            print(f"   Number of blocks: {len(event.block_hashes)}")
            print(f"   Total tokens: {len(event.token_ids)}")
            print(f"   Parent block hash: {event.parent_block_hash}")
            print(f"   LoRA ID: {event.lora_id}")
            # print(f"   Medium: {event.medium}") # This field doesn't exist
            print(f"\n   Block Hashes:")
            for idx, block_hash in enumerate(event.block_hashes):
                print(f"       Block {idx}:")
                print(f"           Hash (decimal): {block_hash}")
                print(f"           Hash (hex): 0x{block_hash & 0xFFFFFFFFFFFFFFFF:016x}")
                # Calculate which tokens belong to this block
                start_token = idx * event.block_size
                end_token = min(start_token + event.block_size, len(event.token_ids))
                block_tokens = event.token_ids[start_token:end_token]
                print(f"           Tokens[{start_token}:{end_token}]: {block_tokens}")
        
        elif isinstance(event, BlockRemoved):
            print(f"\n🗑️  BlockRemoved Event:")
            print(f"   Number of blocks removed: {len(event.block_hashes)}")
            # print(f"   Medium: {event.medium}") # This field doesn't exist
            for idx, block_hash in enumerate(event.block_hashes):
                print(f"       Block {idx}: {block_hash} (0x{block_hash & 0xFFFFFFFFFFFFFFFF:016x})")
        
        elif isinstance(event, AllBlocksCleared):
            print(f"\n🧹 AllBlocksCleared Event")
        
        else:
            print(f"\n❓ Unknown event: {event}")

def main():
    decoder = Decoder(type=KVEventBatch)
    last_seq = -1
    context = zmq.Context()
    
    sub = context.socket(zmq.SUB)
    # MODIFIED: Connect to host port 5588
    sub.connect("tcp://localhost:5588") 
    topic = "kv-events"
    sub.setsockopt_string(zmq.SUBSCRIBE, topic)
    
    replay = context.socket(zmq.REQ)
    # MODIFIED: Connect to host port 5577
    replay.connect("tcp://localhost:5577") 
    
    poller = zmq.Poller()
    poller.register(replay, zmq.POLLIN)
    
    print("🎧 Listening for KV cache events...")
    # MODIFIED: Updated print statement
    print(f"   Connecting to publisher at tcp://localhost:5588 (Topic: '{topic}')")
    # MODIFIED: Updated print statement
    print(f"   Connecting to replay at tcp://localhost:5577")
    print("📡 Waiting for events...\n")
    
    while True:
        try:
            if sub.poll(50):
                _, seq_bytes, payload = sub.recv_multipart()
                seq = int.from_bytes(seq_bytes, "big")
                
                if last_seq >= 0 and seq > last_seq + 1:
                    missed = seq - last_seq - 1
                    print(f"⚠️  Missed {missed} messages (last: {last_seq}, current: {seq})")
                    replay.send((last_seq + 1).to_bytes(8, "big"))
                    
                    while poller.poll(timeout=200):
                        seq_bytes, replay_payload = replay.recv_multipart()
                        if not replay_payload:
                            break
                        replay_seq = int.from_bytes(seq_bytes, "big")
                        if replay_seq > last_seq:
                            try:
                                event_batch = decoder.decode(replay_payload)
                                process_event(event_batch)
                            except msgspec.ValidationError as e:
                                print(f"❌ REPLAY DECODE ERROR: {e}")
                                print(f"   RAW REPLAY PAYLOAD: {replay_payload!r}\n")
                            last_seq = replay_seq
                            if replay_seq >= seq - 1:
                                break
                
                try:
                    event_batch = decoder.decode(payload)
                    process_event(event_batch)
                except msgspec.ValidationError as e:
                    print(f"❌ LIVE DECODE ERROR: {e}")
                    print(f"   RAW LIVE PAYLOAD (seq={seq}): {payload!r}\n")
                
                last_seq = seq
                
        except KeyboardInterrupt:
            print("\n\n👋 Interrupted - shutting down")
            break
        except Exception as e:
            # We will still keep the try/except here just in case.
            print(f"❌ Unhandled Error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()