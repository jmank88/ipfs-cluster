package crdt

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kelseyhightower/envconfig"

	"github.com/ipfs/ipfs-cluster/config"
)

var configKey = "crdt"
var envConfigKey = "cluster_crdt"

var (
	DefaultClusterName        = "ipfs-cluster"
	DefaultPeersetMetric      = "ping"
	DefaultDatastoreNamespace = "/crdt"
)

type Config struct {
	config.Saver

	hostShutdown bool

	// The name of the metric we use to obtain the peerset (every peer
	// with valid metric of this type is part of it).
	PeersetMetric string

	// The topic we wish to subscribe to
	ClusterName string

	// All keys written to the datastore will be namespaced with this prefix
	DatastoreNamespace string

	// Tracing enables propagation of contexts across binary boundaries.
	Tracing bool
}

type jsonConfig struct {
	ClusterName        string `json:"cluster_name"`
	PeersetMetric      string `json:"peerset_metric,omitempty"`
	DatastoreNamespace string `json:"datastore_namespace,omitempty"`
}

func (cfg *Config) ConfigKey() string {
	return configKey
}

func (cfg *Config) Validate() error {
	if cfg.ClusterName == "" {
		return errors.New("crdt.cluster_name cannot be empty")

	}

	if cfg.PeersetMetric == "" {
		return errors.New("crdt.PeersetMetric needs a name")
	}
	return nil
}

func (cfg *Config) LoadJSON(raw []byte) error {
	jcfg := &jsonConfig{}
	err := json.Unmarshal(raw, jcfg)
	if err != nil {
		return fmt.Errorf("error unmarshaling %s config", configKey)
	}

	cfg.Default()

	return cfg.applyJSONConfig(jcfg)
}

func (cfg *Config) applyJSONConfig(jcfg *jsonConfig) error {
	cfg.ClusterName = jcfg.ClusterName
	config.SetIfNotDefault(jcfg.PeersetMetric, &cfg.PeersetMetric)
	config.SetIfNotDefault(jcfg.DatastoreNamespace, &cfg.DatastoreNamespace)
	return cfg.Validate()
}

func (cfg *Config) ToJSON() ([]byte, error) {
	jcfg := cfg.toJSONConfig()

	return config.DefaultJSONMarshal(jcfg)
}

func (cfg *Config) toJSONConfig() *jsonConfig {
	jcfg := &jsonConfig{
		ClusterName:   cfg.ClusterName,
		PeersetMetric: "",
	}

	if cfg.PeersetMetric != DefaultPeersetMetric {
		jcfg.PeersetMetric = cfg.PeersetMetric
		// otherwise leave empty/hidden
	}

	if cfg.DatastoreNamespace != DefaultDatastoreNamespace {
		jcfg.DatastoreNamespace = cfg.DatastoreNamespace
		// otherwise leave empty/hidden
	}
	return jcfg
}

func (cfg *Config) Default() error {
	cfg.ClusterName = DefaultClusterName
	cfg.PeersetMetric = DefaultPeersetMetric
	cfg.DatastoreNamespace = DefaultDatastoreNamespace
	return nil
}

// ApplyEnvVars fills in any Config fields found
// as environment variables.
func (cfg *Config) ApplyEnvVars() error {
	jcfg := cfg.toJSONConfig()

	err := envconfig.Process(envConfigKey, jcfg)
	if err != nil {
		return err
	}

	return cfg.applyJSONConfig(jcfg)
}
