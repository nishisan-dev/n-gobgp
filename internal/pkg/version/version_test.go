package version

import (
	"fmt"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestForkIdentity(t *testing.T) {
	oldCommit, oldIdentifier, oldMetadata, oldDirty := COMMIT, IDENTIFIER, METADATA, DIRTY
	t.Cleanup(func() { COMMIT, IDENTIFIER, METADATA, DIRTY = oldCommit, oldIdentifier, oldMetadata, oldDirty })
	COMMIT, IDENTIFIER, METADATA, DIRTY = "0123456789abcdef", "dev", "", "false"
	base := fmt.Sprintf("%d.%d.%d", MAJOR, MINOR, PATCH)
	i := Get()
	require.Equal(t, "n-gobgp", i.Product)
	require.Equal(t, base+"-dev+commit.0123456789abcdef", i.Version)
	require.Equal(t, COMMIT, i.Commit)
	require.Equal(t, "false", i.Dirty)
	require.Equal(t, "osrg/gobgp", i.UpstreamRepository)
	require.Equal(t, UpstreamVersion, i.UpstreamVersion)
	require.Regexp(t, "^[a-f0-9]{40}$", i.UpstreamCommit)
	require.Contains(t, Description(), "upstream=osrg/gobgp@"+UpstreamCommit)
	IDENTIFIER = ""
	require.Equal(t, base+"+commit.0123456789abcdef", Version())
	METADATA = "custom"
	require.Equal(t, base+"+commit.0123456789abcdef.custom", Version())
}

func TestVCSSettings(t *testing.T) {
	commit, dirty := vcsSettings(nil)
	require.Equal(t, "unknown", commit)
	require.Equal(t, "unknown", dirty)
	commit, dirty = vcsSettings([]debug.BuildSetting{{Key: "vcs.revision", Value: "abc"}, {Key: "vcs.modified", Value: "true"}})
	require.Equal(t, "abc", commit)
	require.Equal(t, "true", dirty)
}
