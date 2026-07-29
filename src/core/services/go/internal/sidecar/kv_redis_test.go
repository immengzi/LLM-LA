package sidecar

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

func TestRedisProjectionFailureUsesNoPartialClientWrites(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	var mu sync.Mutex
	var commands []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer connection.Close()
		reader := bufio.NewReader(connection)
		command, readErr := readRESPCommand(reader)
		if readErr != nil {
			return
		}
		mu.Lock()
		commands = append(commands, strings.ToLower(command))
		mu.Unlock()
		fmt.Fprint(connection, "-ERR injected transaction failure\r\n")
	}()

	client := redis.NewClient(&redis.Options{
		Addr: listener.Addr().String(), MaxRetries: -1, DialTimeout: time.Second,
	})
	defer client.Close()
	projection := &redisKVProjection{
		client: client, model: "model", pod: "pod", engine: "sglang", expectedPageSize: 16,
	}
	err = projection.Apply(context.Background(), kvEventBatch{events: []kvEvent{{
		kind: eventStored, hashes: []string{"1"}, blockSize: 16,
	}}})
	if err == nil {
		t.Fatal("injected Redis transaction failure should be returned")
	}
	<-done
	mu.Lock()
	defer mu.Unlock()
	if len(commands) != 1 || commands[0] != "evalsha" {
		t.Fatalf("projection issued partial non-atomic writes: %v", commands)
	}
}

func readRESPCommand(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	count, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
	if err != nil || count < 1 {
		return "", fmt.Errorf("invalid RESP array header %q", line)
	}
	var command string
	for index := 0; index < count; index++ {
		lengthLine, readErr := reader.ReadString('\n')
		if readErr != nil {
			return "", readErr
		}
		length, parseErr := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(lengthLine, "$")))
		if parseErr != nil {
			return "", parseErr
		}
		value := make([]byte, length+2)
		if _, readErr = io.ReadFull(reader, value); readErr != nil {
			return "", readErr
		}
		if index == 0 {
			command = string(value[:length])
		}
	}
	return command, nil
}

func TestRedisProjectionAtomicSchemaAndClearBarrier(t *testing.T) {
	client := startTestRedis(t)
	projection := &redisKVProjection{
		client: client, model: "model", pod: "pod", engine: "sglang", expectedPageSize: 16,
	}
	ctx := context.Background()
	err := projection.Apply(ctx, kvEventBatch{events: []kvEvent{
		{kind: eventStored, hashes: []string{"1"}, blockSize: 16},
		{kind: eventCleared},
		{kind: eventStored, hashes: []string{"2"}, blockSize: 16},
	}})
	if err != nil {
		t.Fatal(err)
	}
	members, err := client.SMembers(ctx, "model:podblocks:pod").Result()
	if err != nil || len(members) != 1 || members[0] != "2" {
		t.Fatalf("clear barrier left wrong inverse index: members=%v err=%v", members, err)
	}
	if client.HExists(ctx, "model:kvblock:1", "pod").Val() ||
		!client.HExists(ctx, "model:kvblock:2", "pod").Val() {
		t.Fatal("clear barrier did not preserve event ordering")
	}
	if client.HGet(ctx, "model:kvblocks", "2").Val() != "model:kvblock:2" {
		t.Fatal("global block index schema changed")
	}

	if err := client.Set(ctx, "model:podblocks:broken", "wrong-type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	broken := &redisKVProjection{
		client: client, model: "model", pod: "broken", engine: "sglang", expectedPageSize: 16,
	}
	err = broken.Apply(ctx, kvEventBatch{events: []kvEvent{{
		kind: eventStored, hashes: []string{"9"}, blockSize: 16,
	}}})
	if err == nil {
		t.Fatal("wrong-type transaction should fail")
	}
	if client.Exists(ctx, "model:kvblock:9").Val() != 0 ||
		client.HExists(ctx, "model:kvblocks", "9").Val() {
		t.Fatal("failed transaction partially published an owner or index")
	}
}

func startTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	binary, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server is not installed")
	}
	socket := filepath.Join(t.TempDir(), "redis.sock")
	command := exec.Command(binary,
		"--save", "", "--appendonly", "no", "--port", "0",
		"--unixsocket", socket, "--unixsocketperm", "700")
	command.Stdout = nil
	command.Stderr = nil
	if err := command.Start(); err != nil {
		t.Fatalf("start redis-server: %v", err)
	}
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_ = command.Wait()
	})

	client := redis.NewClient(&redis.Options{
		Network: "unix", Addr: socket, DialTimeout: 100 * time.Millisecond,
		ReadTimeout: 100 * time.Millisecond, WriteTimeout: 100 * time.Millisecond,
	})
	t.Cleanup(func() { _ = client.Close() })
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, statErr := os.Stat(socket); statErr == nil && client.Ping(context.Background()).Err() == nil {
			return client
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("redis-server did not become ready")
	return nil
}
