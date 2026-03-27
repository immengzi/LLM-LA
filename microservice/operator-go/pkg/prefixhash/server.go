package prefixhash

import (
	"encoding/json"
	"hash/fnv"
	"net/http"
	"os"
	"strconv"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Config struct {
	Port      int
	BlockSize int
}

func LoadConfig() *Config {
	return &Config{
		Port:      envInt("PORT", 9095),
		BlockSize: envInt("BLOCK_SIZE", 16),
	}
}

type Server struct {
	cfg *Config
}

func NewServer(cfg *Config) *Server {
	return &Server{cfg: cfg}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /metrics", promhttp.Handler().ServeHTTP)
	mux.HandleFunc("POST /compute_hashes", s.handleComputeHashes)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

type hashRequest struct {
	Prompt string `json:"prompt"`
}

type hashResponse struct {
	BlockHashes []int64 `json:"block_hashes"`
}

func (s *Server) handleComputeHashes(w http.ResponseWriter, r *http.Request) {
	var req hashRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", 400)
		return
	}

	hashes := computeBlockHashes(req.Prompt, s.cfg.BlockSize)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(hashResponse{BlockHashes: hashes})
}

// computeBlockHashes splits the prompt into blocks and hashes each prefix.
func computeBlockHashes(prompt string, blockSize int) []int64 {
	tokens := tokenize(prompt)
	if len(tokens) == 0 {
		return nil
	}

	var hashes []int64
	for i := blockSize; i <= len(tokens); i += blockSize {
		block := tokens[:i]
		h := hashTokens(block)
		hashes = append(hashes, h)
	}
	return hashes
}

// tokenize splits prompt into simple word tokens (matching the Python service behavior).
func tokenize(prompt string) []string {
	var tokens []string
	word := ""
	for _, ch := range prompt {
		if ch == ' ' || ch == '\n' || ch == '\t' || ch == '\r' {
			if word != "" {
				tokens = append(tokens, word)
				word = ""
			}
		} else {
			word += string(ch)
		}
	}
	if word != "" {
		tokens = append(tokens, word)
	}
	return tokens
}

func hashTokens(tokens []string) int64 {
	h := fnv.New64a()
	for i, t := range tokens {
		if i > 0 {
			h.Write([]byte(" "))
		}
		h.Write([]byte(t))
	}
	return int64(h.Sum64())
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
