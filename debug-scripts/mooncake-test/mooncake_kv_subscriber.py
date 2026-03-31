#!/usr/bin/env python3
"""
Mooncake KV Event Subscriber - Data Parallel架构版本

架构说明:
  - Data Parallel = 2 (两个vLLM实例)
  - 每个实例独立发布KV events到各自的 tcp://*:5557
  - 本脚本可以同时订阅多个节点的KV events

Usage:
    # 订阅本机 (默认)
    python mooncake_kv_subscriber.py

    # 订阅多个节点 (主节点 + 从节点)
    python mooncake_kv_subscriber.py --nodes "10.1.2.1,10.1.2.3"

    # 指定单个节点
    python mooncake_kv_subscriber.py --nodes "10.1.2.1"

    # 自定义端口
    VLLM_SUB_PORT=5558 python mooncake_kv_subscriber.py --nodes "10.1.2.1"
"""
import os
import time
import threading
import argparse
from typing import Any, Union
from collections import defaultdict, defaultdict
from dataclasses import dataclass, field

import zmq
import msgspec

# ============================================================================
# 1. Configuration
# ============================================================================
VLLM_PORT = int(os.getenv("VLLM_SUB_PORT", "5557"))
MODEL_NAME = os.getenv("MODEL_NAME", "glm5")

# ============================================================================
# 2. Type definitions (与vLLM发布的KV events一致)
# ============================================================================
class EventBatch(msgspec.Struct, array_like=True, omit_defaults=True, gc=False):
    ts: float
    events: list[Any]

class KVCacheEvent(msgspec.Struct, array_like=True, omit_defaults=True, gc=False, tag=True):
    pass

class BlockStored(KVCacheEvent):
    block_hashes: list[Any]
    parent_block_hash: Any = None
    token_ids: list[int] = field(default_factory=list)
    block_size: int = 0
    lora_id: Any = None
    medium: str = ""

class BlockRemoved(KVCacheEvent):
    block_hashes: list[Any]
    medium: str = ""

class AllBlocksCleared(KVCacheEvent):
    pass

class KVEventBatch(EventBatch):
    events: list[Union[BlockStored, BlockRemoved, AllBlocksCleared]]

# ============================================================================
# 3. Prefix Cache Stats (单节点统计)
# ============================================================================
class NodePrefixCacheStats:
    """单个节点的prefix cache统计"""

    def __init__(self, node_ip: str):
        self.node_ip = node_ip
        self.stored_blocks: set = set()      # 当前存储的block hashes
        self.stats = {
            "total_stored": 0,
            "total_removed": 0,
            "total_cleared": 0,
        }
        self.lock = threading.Lock()

    def _normalize_bh(self, bh: Any) -> str:
        """将bytes/int统一转换为字符串"""
        if isinstance(bh, int):
            return str(bh)
        if isinstance(bh, bytes):
            val = int.from_bytes(bh[-8:], byteorder="big")
            return str(val)
        return str(bh)

    def handle_stored(self, block_hashes: list[Any], medium: str):
        with self.lock:
            for bh in block_hashes:
                bh_str = self._normalize_bh(bh)
                if bh_str not in self.stored_blocks:
                    self.stored_blocks.add(bh_str)
                    self.stats["total_stored"] += 1

    def handle_removed(self, block_hashes: list[Any], medium: str):
        with self.lock:
            for bh in block_hashes:
                bh_str = self._normalize_bh(bh)
                if bh_str in self.stored_blocks:
                    self.stored_blocks.discard(bh_str)
                    self.stats["total_removed"] += 1

    def handle_cleared(self):
        with self.lock:
            self.stored_blocks.clear()
            self.stats["total_cleared"] += 1

    def get_stats(self) -> dict:
        with self.lock:
            return {
                "node_ip": self.node_ip,
                "current_blocks": len(self.stored_blocks),
                **self.stats,
            }

# ============================================================================
# 4. 全局统计管理器
# ============================================================================
class GlobalPrefixCacheStats:
    """
    跨节点的全局prefix cache统计
    用于数据并行场景下追踪全局的KV cache状态
    """

    def __init__(self):
        # 每个节点的统计
        self.node_stats: dict[str, NodePrefixCacheStats] = {}
        # 全局block索引: block_hash -> set(node_ips)
        self.global_block_index: dict[str, set] = defaultdict(set)
        self.lock = threading.Lock()

    def register_node(self, node_ip: str):
        """注册一个新节点"""
        with self.lock:
            if node_ip not in self.node_stats:
                self.node_stats[node_ip] = NodePrefixCacheStats(node_ip)
                print(f" [STATS] Registered new node: {node_ip}")

    def handle_stored(self, node_ip: str, block_hashes: list[Any], medium: str):
        """处理BlockStored事件"""
        self.register_node(node_ip)
        node_stats = self.node_stats[node_ip]

        with self.lock:
            for bh in block_hashes:
                bh_str = node_stats._normalize_bh(bh)
                # 更新全局索引
                self.global_block_index[bh_str].add(node_ip)

        node_stats.handle_stored(block_hashes, medium)

    def handle_removed(self, node_ip: str, block_hashes: list[Any], medium: str):
        """处理BlockRemoved事件"""
        self.register_node(node_ip)
        node_stats = self.node_stats[node_ip]

        with self.lock:
            for bh in block_hashes:
                bh_str = node_stats._normalize_bh(bh)
                # 更新全局索引
                self.global_block_index[bh_str].discard(node_ip)

        node_stats.handle_removed(block_hashes, medium)

    def handle_cleared(self, node_ip: str):
        """处理AllBlocksCleared事件"""
        self.register_node(node_ip)
        node_stats = self.node_stats[node_ip]

        with self.lock:
            # 清除该节点的所有block
            blocks_to_remove = [
                bh for bh, nodes in self.global_block_index.items()
                if node_ip in nodes
            ]
            for bh in blocks_to_remove:
                self.global_block_index[bh].discard(node_ip)

        node_stats.handle_cleared()

    def get_global_stats(self) -> dict:
        """获取全局统计信息"""
        with self.lock:
            total_stored = sum(s.stats["total_stored"] for s in self.node_stats.values())
            total_removed = sum(s.stats["total_removed"] for s in self.node_stats.values())
            total_cleared = sum(s.stats["total_cleared"] for s in self.node_stats.values())
            unique_blocks = len(self.global_block_index)

            node_details = []
            for ip, stats in self.node_stats.items():
                node_details.append(stats.get_stats())

            return {
                "total_stored": total_stored,
                "total_removed": total_removed,
                "total_cleared": total_cleared,
                "unique_blocks": unique_blocks,
                "node_count": len(self.node_stats),
                "nodes": node_details,
            }

# ============================================================================
# 5. 单节点订阅器
# ============================================================================
class KVEventSubscriber:
    """单个节点的KV event订阅器"""

    def __init__(self, node_ip: str, port: int, container_name: str, stats: GlobalPrefixCacheStats):
        self.node_ip = node_ip
        self.port = port
        self.container_name = container_name
        self.stats = stats

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._decoder = msgspec.msgpack.Decoder(type=KVEventBatch)

        # 事件计数
        self._event_counts = defaultdict(int)

    def start(self):
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name=f"sub-{self.node_ip}")
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def _loop(self):
        ctx = zmq.Context()
        sub = ctx.socket(zmq.SUB)

        connect_addr = f"tcp://{self.node_ip}:{self.port}"
        try:
            sub.connect(connect_addr)
        except Exception as e:
            print(f" [ERROR] Failed to connect to {connect_addr}: {e}")
            return

        topic = f"kv@{self.container_name}@{self.node_ip}"
        sub.setsockopt_string(zmq.SUBSCRIBE, topic)

        print(f" [SUB] Connected to {connect_addr}, topic: {topic}")

        try:
            while not self._stop.is_set():
                try:
                    frames = sub.recv_multipart(flags=zmq.NOBLOCK)
                except zmq.Again:
                    time.sleep(0.01)
                    continue

                if len(frames) != 3:
                    continue

                _, _, payload = frames
                try:
                    batch = self._decoder.decode(payload)
                    self._handle_batch(batch)
                except Exception as e:
                    pass

        finally:
            sub.close(0)
            ctx.term()

    def _handle_batch(self, event_batch: KVEventBatch):
        for ev in event_batch.events:
            self._event_counts[type(ev).__name__] += 1

            if isinstance(ev, BlockStored):
                self.stats.handle_stored(self.node_ip, ev.block_hashes, ev.medium)

            elif isinstance(ev, BlockRemoved):
                self.stats.handle_removed(self.node_ip, ev.block_hashes, ev.medium)

            elif isinstance(ev, AllBlocksCleared):
                self.stats.handle_cleared(self.node_ip)

# ============================================================================
# 6. 主程序
# ============================================================================
def main():
    parser = argparse.ArgumentParser(description="Mooncake KV Event Subscriber (DP版)")
    parser.add_argument(
        "--nodes",
        type=str,
        default=None,
        help="节点IP列表，逗号分隔。例如: 10.1.2.1,10.1.2.3"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=VLLM_PORT,
        help=f"订阅端口 (默认: {VLLM_PORT})"
    )
    parser.add_argument(
        "--container",
        type=str,
        default=f"glm5@{os.getenv('HOSTNAME', 'unknown')}",
        help="容器名称 (用于topic)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="统计打印间隔(秒)"
    )
    args = parser.parse_args()

    # 确定要订阅的节点
    if args.nodes:
        node_ips = [ip.strip() for ip in args.nodes.split(",")]
    else:
        # 默认订阅本机
        import socket
        hostname = socket.gethostname()
        try:
            local_ip = socket.gethostbyname(hostname)
        except:
            local_ip = "127.0.0.1"
        node_ips = [local_ip]

    print("=" * 60)
    print("  Mooncake KV Event Subscriber (Data Parallel)")
    print("=" * 60)
    print(f"  Nodes:   {node_ips}")
    print(f"  Port:    {args.port}")
    print(f"  Container: {args.container}")
    print(f"  Interval: {args.interval}s")
    print("=" * 60)

    # 创建全局统计管理器
    global_stats = GlobalPrefixCacheStats()

    # 为每个节点创建订阅器
    subscribers = []
    for ip in node_ips:
        sub = KVEventSubscriber(
            node_ip=ip,
            port=args.port,
            container_name=args.container,
            stats=global_stats
        )
        sub.start()
        subscribers.append(sub)

    print(f"\n [READY] Subscribing to {len(subscribers)} node(s)")

    # 定期打印统计
    try:
        while True:
            time.sleep(args.interval)
            stats = global_stats.get_global_stats()

            print("\n" + "=" * 60)
            print(f" Global Prefix Cache Statistics")
            print("=" * 60)
            print(f"  Total Stored:   {stats['total_stored']}")
            print(f"  Total Removed:  {stats['total_removed']}")
            print(f"  Total Cleared:  {stats['total_cleared']}")
            print(f"  Unique Blocks:  {stats['unique_blocks']}")
            print(f"  Active Nodes:   {stats['node_count']}")
            print("-" * 60)

            for node in stats.get("nodes", []):
                print(f"  [{node['node_ip']}] blocks={node['current_blocks']}, "
                      f"stored={node['total_stored']}, removed={node['total_removed']}")

            print("=" * 60)

    except KeyboardInterrupt:
        print("\n [SHUTDOWN] Stopping subscribers...")
        for sub in subscribers:
            sub.stop()
        print(" [SHUTDOWN] Done")


if __name__ == "__main__":
    main()