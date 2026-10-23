package table

import (
	"testing"

	"github.com/osrg/gobgp/v4/pkg/config/oc"
	"github.com/stretchr/testify/require"
)

func TestASPathPrependRejectsASZero(t *testing.T) {
	for _, value := range []string{"0", "00"} {
		_, err := NewAsPathPrependAction(oc.SetAsPathPrepend{As: value, RepeatN: 1})
		require.ErrorContains(t, err, "AS 0")
	}
	for _, value := range []string{"1", "65001", "4294967295", "last-as"} {
		action, err := NewAsPathPrependAction(oc.SetAsPathPrepend{As: value, RepeatN: 1})
		require.NoError(t, err)
		require.NotNil(t, action)
	}
}
