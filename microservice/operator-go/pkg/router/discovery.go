package router

import (
	"context"
	"fmt"
	"log"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// PodDiscovery watches Kubernetes pods for sidecar endpoints.
type PodDiscovery struct {
	clientset *kubernetes.Clientset
	cfg       *Config
}

func NewPodDiscovery(cs *kubernetes.Clientset, cfg *Config) *PodDiscovery {
	return &PodDiscovery{clientset: cs, cfg: cfg}
}

// Discover returns sidecar HTTP URLs for all running pods matching the label selector.
func (d *PodDiscovery) Discover(ctx context.Context) ([]string, error) {
	pods, err := d.clientset.CoreV1().Pods(d.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: d.cfg.LabelSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	var eps []string
	for _, pod := range pods.Items {
		if pod.Status.Phase != "Running" || pod.DeletionTimestamp != nil {
			continue
		}
		if pod.Status.PodIP == "" {
			continue
		}
		eps = append(eps, fmt.Sprintf("http://%s:%d", pod.Status.PodIP, d.cfg.SidecarPort))
	}
	return eps, nil
}

// DiscoverPodNames returns running pod names matching the label selector.
func (d *PodDiscovery) DiscoverPodNames(ctx context.Context) ([]string, error) {
	pods, err := d.clientset.CoreV1().Pods(d.cfg.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: d.cfg.LabelSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	var names []string
	for _, pod := range pods.Items {
		if pod.Status.Phase != "Running" || pod.DeletionTimestamp != nil {
			continue
		}
		names = append(names, pod.Name)
	}
	return names, nil
}

// DiscoveryLoop refreshes push router endpoints periodically.
func DiscoveryLoop(ctx context.Context, disc *PodDiscovery, push *PushRouter) {
	ticker := time.NewTicker(time.Duration(disc.cfg.KVDiscoveryIntervalS * float64(time.Second)))
	defer ticker.Stop()

	refresh := func() {
		eps, err := disc.Discover(ctx)
		if err != nil {
			log.Printf("[discovery] error: %v", err)
			return
		}
		push.SetEndpoints(eps)
		log.Printf("[discovery] %d endpoints", len(eps))
	}

	refresh()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh()
		}
	}
}
