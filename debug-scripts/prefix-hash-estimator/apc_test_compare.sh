#!/bin/bash

# ============================================
# vLLM Prefix Cache Experiment Setup Script
# ============================================

set -e

echo "=========================================="
echo "vLLM Prefix Cache K8s Experiment Setup"
echo "=========================================="

# Configuration
NAMESPACE="vllm-experiment"
EXPERIMENT_DIR="vllm-prefix-cache-experiment"
HF_TOKEN="${HUGGING_FACE_TOKEN:-}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl not found. Please install kubectl."
        exit 1
    fi
    
    if ! kubectl cluster-info &> /dev/null; then
        log_error "Cannot connect to Kubernetes cluster."
        exit 1
    fi
    
    # Check for GPU nodes
    GPU_NODES=$(kubectl get nodes -l accelerator=nvidia-gpu --no-headers 2>/dev/null | wc -l)
    if [ "$GPU_NODES" -eq 0 ]; then
        log_warn "No GPU nodes found with label 'accelerator=nvidia-gpu'"
        log_warn "You may need to adjust nodeSelector or add GPU nodes"
    else
        log_info "Found $GPU_NODES GPU node(s)"
    fi
    
    log_info "Prerequisites check complete"
}

# Create experiment directory
setup_directory() {
    log_info "Setting up experiment directory..."
    mkdir -p "$EXPERIMENT_DIR"
    cd "$EXPERIMENT_DIR"
}

# Create Hugging Face token secret
create_hf_secret() {
    log_info "Creating Hugging Face token secret..."
    
    if [ -z "$HF_TOKEN" ]; then
        log_warn "HUGGING_FACE_TOKEN not set. You may need gated model access."
        log_warn "Set it with: export HUGGING_FACE_TOKEN='your_token'"
        log_warn "Creating dummy secret..."
        kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
        kubectl create secret generic hf-token \
            --from-literal=token="dummy" \
            --namespace="$NAMESPACE" \
            --dry-run=client -o yaml | kubectl apply -f -
    else
        kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
        kubectl create secret generic hf-token \
            --from-literal=token="$HF_TOKEN" \
            --namespace="$NAMESPACE" \
            --dry-run=client -o yaml | kubectl apply -f -
        log_info "Hugging Face token secret created"
    fi
}

# Deploy vLLM servers
deploy_vllm() {
    log_info "Deploying vLLM servers..."
    
    # Apply all manifests (assuming they're in vllm-manifests.yaml)
    kubectl apply -f ../vllm-manifests.yaml
    
    log_info "Waiting for deployments to be ready..."
    log_info "This may take several minutes as models are downloaded..."
    
    kubectl wait --for=condition=available \
        --timeout=600s \
        deployment/vllm-server-prefix-cache \
        -n "$NAMESPACE" || log_warn "Prefix cache deployment timeout"
    
    kubectl wait --for=condition=available \
        --timeout=600s \
        deployment/vllm-server-no-cache \
        -n "$NAMESPACE" || log_warn "No-cache deployment timeout"
    
    log_info "vLLM servers deployed"
}

# Check deployment status
check_status() {
    log_info "Checking deployment status..."
    
    echo ""
    echo "Pods:"
    kubectl get pods -n "$NAMESPACE" -o wide
    
    echo ""
    echo "Services:"
    kubectl get svc -n "$NAMESPACE"
    
    echo ""
    echo "PVCs:"
    kubectl get pvc -n "$NAMESPACE"
}

# Run experiment
run_experiment() {
    log_info "Starting experiment..."
    
    # Delete previous job if exists
    kubectl delete job vllm-prefix-cache-test -n "$NAMESPACE" 2>/dev/null || true
    
    # Wait a bit for cleanup
    sleep 5
    
    # Create and run test job
    kubectl apply -f ../vllm-manifests.yaml
    
    log_info "Test job created. Waiting for completion..."
    
    # Wait for job to complete
    kubectl wait --for=condition=complete \
        --timeout=600s \
        job/vllm-prefix-cache-test \
        -n "$NAMESPACE" || {
            log_error "Job did not complete in time"
            log_info "Checking job logs..."
            kubectl logs -n "$NAMESPACE" job/vllm-prefix-cache-test --tail=50
            return 1
        }
    
    log_info "Experiment complete!"
}

# Get results
get_results() {
    log_info "Fetching experiment results..."
    
    POD_NAME=$(kubectl get pods -n "$NAMESPACE" \
        -l job-name=vllm-prefix-cache-test \
        -o jsonpath='{.items[0].metadata.name}')
    
    if [ -z "$POD_NAME" ]; then
        log_error "Could not find test pod"
        return 1
    fi
    
    echo ""
    echo "=========================================="
    echo "EXPERIMENT LOGS"
    echo "=========================================="
    kubectl logs -n "$NAMESPACE" "$POD_NAME"
    
    # Try to copy results file
    kubectl cp "$NAMESPACE/$POD_NAME:/tmp/results.json" ./results.json 2>/dev/null || \
        log_warn "Could not copy results.json"
    
    if [ -f results.json ]; then
        log_info "Results saved to: $(pwd)/results.json"
    fi
}

# View logs
view_logs() {
    local deployment=$1
    log_info "Viewing logs for $deployment..."
    kubectl logs -n "$NAMESPACE" -l app=vllm-server,cache="$2" --tail=100
}

# Cleanup
cleanup() {
    log_info "Cleaning up experiment..."
    
    read -p "Are you sure you want to delete the entire namespace? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        kubectl delete namespace "$NAMESPACE"
        log_info "Cleanup complete"
    else
        log_info "Cleanup cancelled"
    fi
}

# Main menu
show_menu() {
    echo ""
    echo "=========================================="
    echo "vLLM Prefix Cache Experiment Menu"
    echo "=========================================="
    echo "1. Setup and Deploy Everything"
    echo "2. Check Status"
    echo "3. Run Experiment"
    echo "4. Get Results"
    echo "5. View Logs (Prefix Cache)"
    echo "6. View Logs (No Cache)"
    echo "7. Cleanup"
    echo "8. Exit"
    echo ""
    read -p "Select option: " choice
    
    case $choice in
        1)
            check_prerequisites
            setup_directory
            create_hf_secret
            deploy_vllm
            check_status
            ;;
        2)
            check_status
            ;;
        3)
            run_experiment
            ;;
        4)
            get_results
            ;;
        5)
            view_logs "vllm-server-prefix-cache" "enabled"
            ;;
        6)
            view_logs "vllm-server-no-cache" "disabled"
            ;;
        7)
            cleanup
            ;;
        8)
            exit 0
            ;;
        *)
            log_error "Invalid option"
            ;;
    esac
    
    show_menu
}

# Run full setup if --auto flag is provided
if [ "$1" = "--auto" ]; then
    check_prerequisites
    setup_directory
    create_hf_secret
    deploy_vllm
    sleep 60  # Wait for pods to be ready
    check_status
    run_experiment
    get_results
else
    show_menu
fi