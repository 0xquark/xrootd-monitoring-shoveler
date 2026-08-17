package shoveler

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"

	"github.com/stretchr/testify/assert"
)

// TestWLCGRoutingDisabledByEmptyLists covers turning WLCG routing off entirely so
// every record is published as a plain collector record. Viper returns nil for an
// explicitly empty list, which the correlator would read as "unset" and replace
// with the defaults, so the config layer normalizes it to an empty non-nil slice.
func TestWLCGRoutingDisabledByEmptyLists(t *testing.T) {
	defer viper.Reset()

	path := filepath.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(path, []byte("wlcg:\n  vos: []\n  path_prefixes: []\n"), 0600)
	assert.NoError(t, err, "Failed to write temporary config file")

	var config Config
	config.ReadConfigWithPathAndPrefix(path, "COLLECTOR")

	assert.NotNil(t, config.WLCG.VOs, "empty vos must stay non-nil or the correlator restores the cms default")
	assert.Empty(t, config.WLCG.VOs, "empty vos should match nothing")
	assert.NotNil(t, config.WLCG.PathPrefixes, "empty path_prefixes must stay non-nil")
	assert.Empty(t, config.WLCG.PathPrefixes, "empty path_prefixes should match nothing")
}

// TestWLCGRoutingDefaultsWhenAbsent guards the other half: omitting the keys
// entirely must preserve the upstream OSG defaults.
func TestWLCGRoutingDefaultsWhenAbsent(t *testing.T) {
	defer viper.Reset()

	path := filepath.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(path, []byte("listen:\n  port: 9993\n"), 0600)
	assert.NoError(t, err, "Failed to write temporary config file")

	var config Config
	config.ReadConfigWithPathAndPrefix(path, "COLLECTOR")

	assert.Equal(t, []string{"cms"}, config.WLCG.VOs, "absent vos should keep the upstream default")
	assert.Equal(t, []string{"/store", "/user/dteam"}, config.WLCG.PathPrefixes, "absent path_prefixes should keep the upstream default")
}

// TestWLCGEnabledDefaultsTrue guards the master switch default: absent from the
// config, WLCG conversion stays on exactly as upstream ships it.
func TestWLCGEnabledDefaultsTrue(t *testing.T) {
	defer viper.Reset()

	path := filepath.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(path, []byte("listen:\n  port: 9993\n"), 0600)
	assert.NoError(t, err, "Failed to write temporary config file")

	var config Config
	config.ReadConfigWithPathAndPrefix(path, "COLLECTOR")

	assert.True(t, config.WLCG.Enabled, "wlcg.enabled should default to true")
}

// TestWLCGEnabledFromConfigAndEnvironment covers turning the switch off from the
// config file and from COLLECTOR_WLCG_ENABLED.
func TestWLCGEnabledFromConfigAndEnvironment(t *testing.T) {
	defer viper.Reset()

	path := filepath.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(path, []byte("wlcg:\n  enabled: false\n"), 0600)
	assert.NoError(t, err, "Failed to write temporary config file")

	var config Config
	config.ReadConfigWithPathAndPrefix(path, "COLLECTOR")
	assert.False(t, config.WLCG.Enabled, "wlcg.enabled: false should disable WLCG conversion")

	viper.Reset()

	t.Setenv("COLLECTOR_WLCG_ENABLED", "false")

	path = filepath.Join(t.TempDir(), "config.yaml")
	err = os.WriteFile(path, []byte("wlcg:\n  enabled: true\n"), 0600)
	assert.NoError(t, err, "Failed to write temporary config file")

	config = Config{}
	config.ReadConfigWithPathAndPrefix(path, "COLLECTOR")
	assert.False(t, config.WLCG.Enabled, "COLLECTOR_WLCG_ENABLED should override the config file")
}
