package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// ---- VllmRouter CRD ----

type VllmRouterSpec struct {
	Image         string            `json:"image"`
	Replicas      int32             `json:"replicas,omitempty"`
	RouterMode    string            `json:"routerMode,omitempty"`
	KVAware       *bool             `json:"kvAware,omitempty"`
	LenAware      *bool             `json:"lenAware,omitempty"`
	LenPolicy     string            `json:"lenPolicy,omitempty"`
	PoolFactor    int               `json:"poolFactor,omitempty"`
	TransportMode string            `json:"transportMode,omitempty"`
	RedisHost     string            `json:"redisHost,omitempty"`
	RedisPort     int               `json:"redisPort,omitempty"`
	Port          int               `json:"port,omitempty"`
	Env           map[string]string `json:"env,omitempty"`
	Resources     *Resources        `json:"resources,omitempty"`
}

type VllmRouterStatus struct {
	Phase   string `json:"phase,omitempty"`
	Ready   int32  `json:"ready,omitempty"`
	Message string `json:"message,omitempty"`
}

type VllmRouter struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              VllmRouterSpec   `json:"spec"`
	Status            VllmRouterStatus `json:"status,omitempty"`
}

type VllmRouterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VllmRouter `json:"items"`
}

// ---- VllmSidecar CRD ----

type VllmSidecarSpec struct {
	Image       string            `json:"image"`
	SidecarMode string            `json:"sidecarMode,omitempty"`
	BatchSize   int               `json:"batchSize,omitempty"`
	RouterURL   string            `json:"routerURL,omitempty"`
	VllmURL     string            `json:"vllmURL,omitempty"`
	ModelName   string            `json:"modelName,omitempty"`
	RedisHost   string            `json:"redisHost,omitempty"`
	RedisPort   int               `json:"redisPort,omitempty"`
	Port        int               `json:"port,omitempty"`
	Env         map[string]string `json:"env,omitempty"`
	Resources   *Resources        `json:"resources,omitempty"`
}

type VllmSidecarStatus struct {
	Phase   string `json:"phase,omitempty"`
	Ready   int32  `json:"ready,omitempty"`
	Message string `json:"message,omitempty"`
}

type VllmSidecar struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              VllmSidecarSpec   `json:"spec"`
	Status            VllmSidecarStatus `json:"status,omitempty"`
}

type VllmSidecarList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VllmSidecar `json:"items"`
}

// ---- VllmPrefixHash CRD ----

type VllmPrefixHashSpec struct {
	Image     string            `json:"image"`
	Replicas  int32             `json:"replicas,omitempty"`
	BlockSize int               `json:"blockSize,omitempty"`
	Port      int               `json:"port,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	Resources *Resources        `json:"resources,omitempty"`
}

type VllmPrefixHashStatus struct {
	Phase   string `json:"phase,omitempty"`
	Ready   int32  `json:"ready,omitempty"`
	Message string `json:"message,omitempty"`
}

type VllmPrefixHash struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              VllmPrefixHashSpec   `json:"spec"`
	Status            VllmPrefixHashStatus `json:"status,omitempty"`
}

type VllmPrefixHashList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VllmPrefixHash `json:"items"`
}

// ---- Shared ----

type Resources struct {
	Requests map[string]string `json:"requests,omitempty"`
	Limits   map[string]string `json:"limits,omitempty"`
}
