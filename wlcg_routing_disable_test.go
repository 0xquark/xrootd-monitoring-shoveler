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
