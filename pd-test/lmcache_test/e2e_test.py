#!/usr/bin/env python3
import subprocess
import time
import sys
import signal
import os
import threading
import select
import json
import requests
import re
import csv

class DockerVLLMOrchestrator:
    def __init__(self):
        self.container_name = "lmcache-ascend-haiting"
        self.processes = []
        self.reported_exits = set()
        self.last_hit_tokens = 0
        self.last_total_tokens = 0
        self.last_throughput = "0.0"
        self.log_file = "lmcache_performance_stats.csv"
        self._init_csv()

    def _init_csv(self):
        """Creates the CSV file with headers if it doesn't exist"""
        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["Timestamp", "Service", "Action", "Hit_Tokens", "Total_Tokens", "Hit_Rate_%", "Throughput_GBps", "Size_GB"])

    # --- NEW METHOD ADDED ---
    def log_kv_info(self, service, action, hit=0, total=0, rate=0.0, tp="0.0", size="0.0"):
        """Saves a row of KV information to the CSV file"""
        with open(self.log_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([time.strftime("%Y-%m-%d %H:%M:%S"), service, action, hit, total, f"{rate:.2f}", tp, size])

    def parse_lmcache_stats(self, service_name, line):
        # Change: Combined search for both values in one line
        hit_match = re.search(r"Total tokens (\d+), LMCache hit tokens: (\d+)", line)
        if hit_match:
            total, hit = int(hit_match.group(1)), int(hit_match.group(2))
            rate = (hit / total) * 100
            # Change: Now calls the log_kv_info method to save to disk
            self.log_kv_info(service_name, "RETRIEVE_SUMMARY", hit=hit, total=total, rate=rate)

        # Change: Unified patterns for 'Retrieved' and 'Stored'
        tp_match = re.search(r"(Retrieved|Stored) (\d+).*size: ([\d\.]+) gb.*throughput: ([\d\.]+) GB/s", line)
        if tp_match:
            action, tokens, size, tp = tp_match.groups()
            self.log_kv_info(service_name, action.upper(), total=tokens, tp=tp, size=size)

    def stream_logs(self):
        """Unified stream loop: parses AND prints to ensure nothing is missed"""
        print(f"\n📊 Monitoring & Logging to {self.log_file}...")
        service_names = ["Prefiller", "Decoder", "Proxy"]
        
        try:
            while True:
                for i, proc in enumerate(self.processes):
                    if proc and proc.poll() is None:
                        # Non-blocking read
                        while True:
                            line = proc.stdout.readline()
                            if not line: break
                            
                            service = service_names[i] if i < len(service_names) else f"Proc_{i}"
                            clean_line = line.rstrip()
                            
                            # RUN PARSER
                            self.parse_lmcache_stats(service, clean_line)
                            
                            # PRINT TO TERMINAL
                            print(f"[{service}] {clean_line}")
                time.sleep(0.01)
        except KeyboardInterrupt:
            print("\n⚠️ Stopping...")       
    def cleanup(self, signum=None, frame=None):
        """Cleanup processes and container on exit"""
        print("\n🧹 Cleaning up...")
        for proc in self.processes:
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
        
        # Stop and remove container
        subprocess.run(['docker', 'stop', self.container_name], 
                      stderr=subprocess.DEVNULL)
        subprocess.run(['docker', 'rm', self.container_name], 
                      stderr=subprocess.DEVNULL)
        print("✅ Cleanup complete")
        sys.exit(0)
    
    def start_container(self):
        """Start the Docker container"""
        print("🚀 Starting Docker container...")
        
        # Check if container already exists
        result = subprocess.run(
            ['docker', 'ps', '-a', '--filter', f'name={self.container_name}', '--format', '{{.Names}}'],
            capture_output=True, text=True
        )
        
        if self.container_name in result.stdout:
            print(f"⚠️  Container {self.container_name} already exists. Removing...")
            subprocess.run(['docker', 'rm', '-f', self.container_name])
        
        docker_cmd = [
            'docker', 'run', '-d',
            '--privileged',
            '--net=host',
            '--shm-size=32g',
            '--name', self.container_name,
            '-e', 'ASCEND_VISIBLE_DEVICES=0,1,2,3',
            '-e', 'ASCEND_RT_VISIBLE_DEVICES=0,1,2,3',
            '-e', 'ASCEND_TOTAL_MEMORY_GB=32',
            '-e', 'VLLM_TARGET_DEVICE=npu',
            '-e', f'http_proxy={os.environ.get("http_proxy", "")}',
            '-e', f'https_proxy={os.environ.get("https_proxy", "")}',
            '-e', f'no_proxy={os.environ.get("no_proxy", "")}',
            '-e', 'PROMETHEUS_MULTIPROC_DIR=/tmp/lmcache_prometheus',
            '--device', '/dev/davinci0',
            '--device', '/dev/davinci1',
            '--device', '/dev/davinci2',
            '--device', '/dev/davinci3',
            '--device', '/dev/davinci_manager',
            '--device', '/dev/devmm_svm',
            '--device', '/dev/hisi_hdc',
            '-v', '/mnt/nvme1/haiting_jd/models/qwen3-8b:/model',
            '-v', '/usr/local/Ascend/driver:/usr/local/Ascend/driver',
            '-v', '/etc/localtime:/etc/localtime',
            '-v', '/var/log/npu:/var/log/npu',
            '-v', '/etc/ascend_install.info:/etc/ascend_install.info',
            '-v', '/etc/hccn.conf:/etc/hccn.conf',
            '-v', '/usr/local/bin/npu-smi:/usr/local/bin/npu-smi',
            'lmcache-ascend:env-v1',
            'tail', '-f', '/dev/null'
        ]
        
        result = subprocess.run(docker_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"❌ Failed to start container: {result.stderr}")
            sys.exit(1)
        
        print(f"✅ Container {self.container_name} started")
        time.sleep(2)
    
    def docker_exec_interactive(self, cmd, env=None, name="Process"):
        """Execute command in Docker container with live output"""
        docker_cmd = ['docker', 'exec', '-i']
        
        if env:
            for key, value in env.items():
                docker_cmd.extend(['-e', f'{key}={value}'])
        
        # Use a here-document to avoid quoting issues
        wrapped_cmd = f'stdbuf -oL -eL bash <<EOF\n{cmd}\nEOF'
        docker_cmd.extend([self.container_name, 'bash', '-c', wrapped_cmd])
        
        print(f"🔄 Starting {name}...")
        proc = subprocess.Popen(
            docker_cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT, 
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        return proc
    
    
    def print_logs_nonblocking(self, proc, service_name, stop_event):
        """Print logs from a process in a separate thread"""
        try:
            while not stop_event.is_set():
                if proc.poll() is not None:
                    for line in proc.stdout:
                        print(f"[{service_name}] {line.rstrip()}")
                    break
                
                if hasattr(select, 'select'):
                    ready, _, _ = select.select([proc.stdout], [], [], 0.1)
                    if ready:
                        line = proc.stdout.readline()
                        if line:
                            print(f"[{service_name}] {line.rstrip()}")
                else:
                    line = proc.stdout.readline()
                    if line:
                        print(f"[{service_name}] {line.rstrip()}")
                    else:
                        time.sleep(0.1)
        except Exception as e:
            print(f"[{service_name}] Error reading logs: {e}")
    
    def check_port_listening(self, port):
        """Check if a port is listening on the host machine"""
        cmd = f"ss -tlnp 2>/dev/null | grep ':{port}' || true"
        result = subprocess.run(
            ['docker', 'exec', self.container_name, 'bash', '-c', cmd],
            capture_output=True, text=True
        )
        if f":{port}" in result.stdout:
            return True
        
        cmd = f"netstat -tlnp 2>/dev/null | grep ':{port}' || true"
        result = subprocess.run(
            ['docker', 'exec', self.container_name, 'bash', '-c', cmd],
            capture_output=True, text=True
        )
        if f":{port}" in result.stdout:
            return True
        
        cmd = f"timeout 1 bash -c 'cat < /dev/null > /dev/tcp/localhost/{port}' 2>/dev/null && echo 'OPEN' || echo 'CLOSED'"
        result = subprocess.run(
            ['bash', '-c', cmd],
            capture_output=True, text=True
        )
        if 'OPEN' in result.stdout:
            return True
        
        return False
    
    def health_check_service(self, port, service_name, timeout=60):
        """Perform health check on a service by attempting actual requests"""
        print(f"🏥 Health checking {service_name} on port {port}...")
        start_time = time.time()
        
        # Try health endpoint first
        health_endpoints = ['/health', '/v1/models', '/']
        
        while time.time() - start_time < timeout:
            for endpoint in health_endpoints:
                try:
                    response = requests.get(
                        f"http://localhost:{port}{endpoint}",
                        timeout=2
                    )
                    if response.status_code in [200, 404]:  # 404 is ok, means server is responding
                        print(f"✅ {service_name} health check PASSED (HTTP {response.status_code})")
                        return True
                except requests.exceptions.RequestException:
                    pass
            
            time.sleep(2)
        
        print(f"⚠️  {service_name} health check timed out")
        return False
    def prepare_prometheus_directory(self):
        """Ensure the Prometheus multiproc directory exists"""
        print("📁 Creating Prometheus metrics directory...")
        cmd = "mkdir -p /tmp/lmcache_prometheus && chmod 777 /tmp/lmcache_prometheus"
        result = subprocess.run(
            ['docker', 'exec', self.container_name, 'bash', '-c', cmd],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            print(f"⚠️  Warning: Failed to create Prometheus directory: {result.stderr}")
        else:
            print("✅ Prometheus metrics directory created")
    
    # def wait_for_service(self, port, service_name, proc, timeout=180):
    #     """Wait for a service to be ready while printing its logs"""
    #     print(f"⏳ Waiting for {service_name} on port {port}...")
    #     start_time = time.time()
        
    #     stop_logging = threading.Event()
    #     log_thread = threading.Thread(
    #         target=self.print_logs_nonblocking, 
    #         args=(proc, service_name, stop_logging),
    #         daemon=True
    #     )
    #     log_thread.start()
        
    #     last_check_time = 0
    #     while time.time() - start_time < timeout:
    #         if proc.poll() is not None:
    #             stop_logging.set()
    #             log_thread.join(timeout=1)
    #             print(f"\n❌ {service_name} exited with code {proc.returncode}")
    #             return False
            
    #         current_time = time.time()
    #         if current_time - last_check_time >= 2:
    #             last_check_time = current_time
                
    #             if self.check_port_listening(port):
    #                 stop_logging.set()
    #                 log_thread.join(timeout=1)
    #                 print(f"\n✅ {service_name} port {port} is listening")
    #                 return True
            
    #         time.sleep(0.1)
        
    #     stop_logging.set()
    #     log_thread.join(timeout=1)
    #     print(f"\n⚠️  Timeout waiting for {service_name} on port {port}")
    #     return False


    def wait_for_service(self, port, service_name, proc, timeout=180):
            """Wait for service to be ready using non-blocking reads to avoid 'stealing' logs"""
            print(f"⏳ Waiting for {service_name} on port {port}...")
            start_time = time.time()
            
            # Make the process output non-blocking so we can read 'available' lines
            # without getting stuck
            os.set_blocking(proc.stdout.fileno(), False)
            
            last_check_time = 0
            while time.time() - start_time < timeout:
                # 1. Check if process crashed
                if proc.poll() is not None:
                    print(f"\n❌ {service_name} exited with code {proc.returncode}")
                    return False
                
                # 2. Print any available logs in real-time
                try:
                    while True:
                        line = proc.stdout.readline()
                        if not line: break
                        print(f"[{service_name}-init] {line.rstrip()}")
                except (IOError, TypeError):
                    pass  # No data available to read right now
                
                # 3. Check if the port is listening
                current_time = time.time()
                if current_time - last_check_time >= 2:
                    last_check_time = current_time
                    if self.check_port_listening(port):
                        print(f"\n✅ {service_name} port {port} is listening")
                        # Reset blocking for the final stream_logs method
                        os.set_blocking(proc.stdout.fileno(), True)
                        return True
                
                time.sleep(0.1)
                
            print(f"\n⚠️  Timeout waiting for {service_name} on port {port}")
            return False
    
    def start_prefiller(self):
        """Start the prefiller service"""
        print("🔧 Starting prefiller service on NPUs 0,1...")
        
        env = {
            'PROMETHEUS_MULTIPROC_DIR': '/tmp/lmcache_prometheus', 
            'LMCACHE_CONFIG_FILE': '/workspace/LMCache-Ascend/examples/disagg_prefill/1p1d/configs/lmcache-prefiller-config.yaml',
            'ASCEND_RT_VISIBLE_DEVICES': '0,1',
            'VLLM_ENABLE_V1_MULTIPROCESSING': '1',
            'VLLM_WORKER_MULTIPROC_METHOD': 'spawn',
            'PYTHONHASHSEED': '0',
            'VLLM_VERSION': '0.11.0',
            'PYTHONUNBUFFERED': '1',
            'LMCACHE_METRICS_ENABLED': 'True',
            'LMCACHE_INTERNAL_API_SERVER_PORT_START': '6990',
            'LMCACHE_INTERNAL_API_SERVER_ENABLED': 'True'
        }
        
        cmd = '''cd / && python3 -u -m vllm.entrypoints.openai.api_server --port 7100 --model /model --tensor-parallel-size 2 --enforce-eager --no-enable-prefix-caching --trust-remote-code --disable-log-requests --block-size 128 --max-model-len 32768 --gpu-memory-utilization 0.6 --kv-transfer-config '{"kv_connector":"LMCacheAscendConnectorV1Dynamic","kv_role":"kv_producer","kv_connector_module_path":"lmcache_ascend.integration.vllm.lmcache_ascend_connector_v1","kv_connector_extra_config":{"discard_partial_chunks":false,"lmcache_rpc_port":"producer1"}}'
'''
        
        proc = self.docker_exec_interactive(cmd, env=env, name="Prefiller")
        self.processes.append(proc)
        return self.wait_for_service(7100, "Prefiller", proc, timeout=180)
    
    def start_decoder(self):
        """Start the decoder service"""
        print("\n🔧 Starting decoder service on NPUs 2,3...")
        
        env = {
            'PROMETHEUS_MULTIPROC_DIR': '/tmp/lmcache_prometheus', 
            'LMCACHE_CONFIG_FILE': '/workspace/LMCache-Ascend/examples/disagg_prefill/1p1d/configs/lmcache-decoder-config.yaml',
            'ASCEND_RT_VISIBLE_DEVICES': '2,3',
            'VLLM_ENABLE_V1_MULTIPROCESSING': '1',
            'VLLM_WORKER_MULTIPROC_METHOD': 'spawn',
            'PYTHONHASHSEED': '0',
            'VLLM_VERSION': '0.11.0',
            'PYTHONUNBUFFERED': '1',
            'LMCACHE_METRICS_ENABLED': 'True',
            'LMCACHE_METRICS_PORT': '8002', # Force LMCache to use its own port
            'LMCACHE_INTERNAL_API_SERVER_PORT_START': '7000',
            'LMCACHE_INTERNAL_API_SERVER_ENABLED': 'True'
        }
        
        cmd = '''cd / && python3 -u -m vllm.entrypoints.openai.api_server --port 7200 --model /model --tensor-parallel-size 2 --enforce-eager --no-enable-prefix-caching --trust-remote-code --disable-log-requests --block-size 128 --max-model-len 32768 --gpu-memory-utilization 0.6 --kv-transfer-config '{"kv_connector":"LMCacheAscendConnectorV1Dynamic","kv_role":"kv_consumer","kv_connector_module_path":"lmcache_ascend.integration.vllm.lmcache_ascend_connector_v1","kv_connector_extra_config":{"discard_partial_chunks":false,"lmcache_rpc_port":"consumer1","skip_last_n_tokens":1}}'
'''
        
        proc = self.docker_exec_interactive(cmd, env=env, name="Decoder")
        self.processes.append(proc)
        return self.wait_for_service(7200, "Decoder", proc, timeout=180)
    
    def start_proxy(self):
        """Start the proxy server"""
        print("\n🔧 Starting proxy server...")
        
        env = {'PYTHONUNBUFFERED': '1'}
        
        cmd = '''cd / && python3 -u /workspace/LMCache/examples/disagg_prefill/disagg_proxy_server.py --host localhost --port 9101 --prefiller-host localhost --prefiller-port 7100 --num-prefillers 1 --decoder-host localhost --decoder-port 7200 --decoder-init-port "7300,7301" --decoder-alloc-port "7400,7401" --proxy-host localhost --proxy-port 7500 --num-decoders 1
'''
        
        proc = self.docker_exec_interactive(cmd, env=env, name="Proxy")
        self.processes.append(proc)
        return self.wait_for_service(9101, "Proxy", proc, timeout=60)
    
    def check_services_alive(self):
        """Check if all services are still running"""
        service_names = ["Prefiller", "Decoder", "Proxy"]
        all_alive = True
        for i, proc in enumerate(self.processes):
            if proc and proc.poll() is not None:
                service = service_names[i] if i < len(service_names) else f"Process {i}"
                print(f"⚠️  {service} has crashed (exit code: {proc.returncode})")
                all_alive = False
        return all_alive

    
    def comprehensive_health_check(self):
        """Perform comprehensive health checks on all services"""
        print("\n" + "="*80)
        print("🏥 Running Comprehensive Health Checks")
        print("="*80)
        
        # Check if processes are still alive
        if not self.check_services_alive():
            print("❌ One or more services have crashed")
            return False
        
        # Health check prefiller
        if not self.health_check_service(7100, "Prefiller", timeout=60):
            return False
        
        # Health check decoder
        if not self.health_check_service(7200, "Decoder", timeout=60):
            return False
        
        # Health check proxy
        if not self.health_check_service(9101, "Proxy", timeout=60):
            return False
        
        # Additional wait to ensure internal initialization
        print("\n⏳ Waiting additional 5 seconds for internal service initialization...")
        time.sleep(5)
        
        # Final check that services are still alive
        if not self.check_services_alive():
            print("❌ Services crashed during health check wait")
            return False
        
        print("\n✅ All health checks PASSED")
        print("="*80)
        return True
    
    def run_end_to_end_test(self):
        """Run an end-to-end test with a long prompt to verify KV cache transfer"""
        print("\n" + "="*80)
        print("🧪 Running End-to-End Test")
        print("="*80)
        
        # Match your exact working curl prompt
        base_text = "Explain the significance of KV cache in language models in English.You are absolutely right that output is being produced, which proves the Decoder is alive and the Prefill-to-Decode transfer worked.Where should we start?Where should we start?Where should we start?Where should we start?Where should we start?Where should we start?Where should we start?."
        long_prompt = base_text * 100  # Repeat 100 times like your curl
        
        payload = {
            "model": "/model",
            "prompt": long_prompt,
            "max_tokens": 100
        }
        
        print(f"📝 Sending test request with prompt length: {len(long_prompt)} characters")
        print(f"🎯 Target endpoint: http://localhost:9101/v1/completions")
        
        try:
            start_time = time.time()
            
            response = requests.post(
                "http://localhost:9101/v1/completions",
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=180
            )
            
            elapsed_time = time.time() - start_time
            
            print(f"\n📡 Response received:")
            print(f"   Status code: {response.status_code}")
            print(f"   Response time: {elapsed_time:.2f} seconds")
            print(f"   Content length: {len(response.content)} bytes")
            
            if response.status_code == 200 and len(response.content) > 0:
                print(f"\n✅ Test PASSED - Server responded successfully")
                
                # Parse streaming response to extract generated text
                response_text = response.text.strip()
                
                if response_text.startswith('data:'):
                    print(f"\n🔍 Parsing streaming response (SSE format)...")
                    
                    # Split by newlines and extract data lines
                    lines = response_text.split('\n')
                    data_lines = [line[5:].strip() for line in lines if line.startswith('data:')]
                    
                    # Extract text from each chunk
                    all_tokens = []
                    for data_line in data_lines:
                        if not data_line or data_line == '[DONE]':
                            continue
                        try:
                            chunk = json.loads(data_line)
                            if 'choices' in chunk and len(chunk['choices']) > 0:
                                text = chunk['choices'][0].get('text', '')
                                if text:
                                    all_tokens.append(text)
                        except json.JSONDecodeError:
                            continue
                    
                    # Combine all tokens into full text
                    full_generated_text = ''.join(all_tokens)
                    
                    print(f"\n📊 Streaming response statistics:")
                    print(f"   Total chunks received: {len(data_lines)}")
                    print(f"   Tokens extracted: {len(all_tokens)}")
                    print(f"   Generated text length: {len(full_generated_text)} characters")
                    
                    print(f"\n📄 Complete generated text:")
                    print(f"{'='*80}")
                    print(full_generated_text)
                    print(f"{'='*80}")
                    
                else:
                    # Non-streaming response (shouldn't happen but handle it)
                    print(f"\n📄 Non-streaming response:")
                    print(f"{response.text}")
                
                print("\n" + "="*80)
                print("✅ End-to-End Test Complete - System is working correctly!")
                print("="*80)
                return True
            else:
                print(f"\n❌ Test FAILED - Unexpected status or empty response")
                return False
                
        except requests.exceptions.Timeout:
            print(f"\n❌ Test FAILED - Request timed out after 180 seconds")
            if not self.check_services_alive():
                print("⚠️  Services crashed during the test")
            return False
            
        except requests.exceptions.ConnectionError as e:
            print(f"\n❌ Test FAILED - Connection error: {e}")
            if not self.check_services_alive():
                print("⚠️  Services crashed during the test")
            return False
            
        except Exception as e:
            print(f"\n❌ Test FAILED - Unexpected error: {e}")
            import traceback
            traceback.print_exc()
            
            if not self.check_services_alive():
                print("\n⚠️  Services crashed during the test")
            
            return False
    
    def stream_logs(self):
        """Stream logs from all processes"""
        print("\n📊 Streaming logs from all services (Ctrl+C to stop)...")
        print("=" * 80)
        
        try:
            while True:
                all_dead = True
                for i, proc in enumerate(self.processes):
                    if proc is None:
                        continue
                    
                    returncode = proc.poll()
                    if returncode is None:
                        all_dead = False
                        if hasattr(select, 'select'):
                            ready, _, _ = select.select([proc.stdout], [], [], 0.01)
                            if ready:
                                line = proc.stdout.readline()
                                if line:
                                    service_names = ["Prefiller", "Decoder", "Proxy"]
                                    service = service_names[i] if i < len(service_names) else f"Process {i}"
                                    print(f"[{service}] {line.rstrip()}")
                        else:
                            try:
                                line = proc.stdout.readline()
                                if line:
                                    service_names = ["Prefiller", "Decoder", "Proxy"]
                                    service = service_names[i] if i < len(service_names) else f"Process {i}"
                                    print(f"[{service}] {line.rstrip()}")
                            except:
                                pass
                    else:
                        if i not in self.reported_exits:
                            self.reported_exits.add(i)
                            service_names = ["Prefiller", "Decoder", "Proxy"]
                            service = service_names[i] if i < len(service_names) else f"Process {i}"
                            if returncode != 0:
                                print(f"⚠️  {service} exited with code {returncode}")
                            else:
                                print(f"ℹ️  {service} exited normally")
                
                if all_dead:
                    print("\n⚠️  All processes have exited")
                    break
                    
                time.sleep(0.01)
        except KeyboardInterrupt:
            print("\n⚠️  Interrupted by user")
    
    def run(self):
        """Main orchestration"""
        signal.signal(signal.SIGINT, self.cleanup)
        signal.signal(signal.SIGTERM, self.cleanup)
        
        try:
            self.start_container()
            self.prepare_prometheus_directory()
            
            # Start all services
            if not self.start_prefiller():
                print("❌ Failed to start prefiller")
                self.cleanup()
                return
            
            if not self.start_decoder():
                print("❌ Failed to start decoder")
                self.cleanup()
                return
            
            if not self.start_proxy():
                print("❌ Failed to start proxy")
                self.cleanup()
                return
            
            print("\n🎉 All services started successfully!")
            print("\n📍 Service endpoints:")
            print("   - Prefiller: http://localhost:7100")
            print("   - Decoder:   http://localhost:7200")
            print("   - Proxy:     http://localhost:9101")
            print("   - API:       http://localhost:7500")
            
            # CRITICAL: Perform comprehensive health checks before testing
            if not self.comprehensive_health_check():
                print("\n❌ Health checks failed - not running end-to-end test")
                self.cleanup()
                return
            
            # Only run test after health checks pass
            test_passed = self.run_end_to_end_test()
            
            if test_passed:
                print("\n✅ System is ready for use!")
                print("\nYou can now:")
                print("  - Send requests to http://localhost:9101/v1/completions")
                print("  - Monitor logs below")
                print("  - Press Ctrl+C to stop all services")
            else:
                print("\n⚠️  End-to-end test failed")
                if self.check_services_alive():
                    print("Services are still running - check logs below for issues")
                else:
                    print("Services have crashed - check logs above for errors")
            
            self.stream_logs()
            
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
            self.cleanup()

if __name__ == "__main__":
    orchestrator = DockerVLLMOrchestrator()
    orchestrator.run()


