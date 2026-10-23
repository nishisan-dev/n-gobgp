package bgp

import (
	"fmt"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateAttributeASZero(t *testing.T) {
	agg, err := NewPathAttributeAggregator(uint16(0), netip.MustParseAddr("192.0.2.1"))
	require.NoError(t, err)
	agg4, err := NewPathAttributeAggregator(uint32(0), netip.MustParseAddr("192.0.2.1"))
	require.NoError(t, err)
	as4agg, err := NewPathAttributeAs4Aggregator(0, netip.MustParseAddr("192.0.2.1"))
	require.NoError(t, err)
	type attributeCase struct {
		name     string
		attr     PathAttributeInterface
		handling ErrorHandling
	}
	cases := []attributeCase{
		{"aggregator-two-octet", agg, ERROR_HANDLING_ATTRIBUTE_DISCARD},
		{"aggregator-four-octet", agg4, ERROR_HANDLING_ATTRIBUTE_DISCARD},
		{"as4-aggregator", as4agg, ERROR_HANDLING_ATTRIBUTE_DISCARD},
		{"as4-path", NewPathAttributeAs4Path([]*As4PathParam{NewAs4PathParam(BGP_ASPATH_ATTR_TYPE_SEQ, []uint32{65001, 0})}), ERROR_HANDLING_ATTRIBUTE_DISCARD},
	}
	for _, kind := range []uint8{BGP_ASPATH_ATTR_TYPE_SEQ, BGP_ASPATH_ATTR_TYPE_SET, BGP_ASPATH_ATTR_TYPE_CONFED_SEQ, BGP_ASPATH_ATTR_TYPE_CONFED_SET} {
		cases = append(cases,
			attributeCase{fmt.Sprintf("segment-%d/two-octet", kind), NewPathAttributeAsPath([]AsPathParamInterface{NewAsPathParam(kind, []uint16{65001, 0})}), ERROR_HANDLING_TREAT_AS_WITHDRAW},
			attributeCase{fmt.Sprintf("segment-%d/four-octet", kind), NewPathAttributeAsPath([]AsPathParamInterface{NewAs4PathParam(kind, []uint32{65001, 0})}), ERROR_HANDLING_TREAT_AS_WITHDRAW})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// AS 0 is invalid for both internal and external sessions.
			for _, external := range []bool{false, true} {
				ok, err := ValidateAttribute(tc.attr, nil, external, false, false)
				require.False(t, ok)
				require.Error(t, err)
				require.Equal(t, tc.handling, err.(*MessageError).ErrorHandling)
			}
		})
	}
}

func TestValidateUpdateDiscardsAllASZeroOptionalAttributes(t *testing.T) {
	agg, err := NewPathAttributeAggregator(uint16(0), netip.MustParseAddr("192.0.2.1"))
	require.NoError(t, err)
	as4agg, err := NewPathAttributeAs4Aggregator(0, netip.MustParseAddr("192.0.2.1"))
	require.NoError(t, err)
	msg := bgpupdate().Body.(*BGPUpdate)
	msg.PathAttributes = append(msg.PathAttributes, agg,
		NewPathAttributeAs4Path([]*As4PathParam{NewAs4PathParam(BGP_ASPATH_ATTR_TYPE_SEQ, []uint32{0})}), as4agg)
	ok, err := ValidateUpdateMsg(msg, map[Family]BGPAddPathMode{RF_IPv4_UC: BGP_ADD_PATH_NONE}, true, false, false)
	require.False(t, ok)
	require.Error(t, err)
	require.Equal(t, ERROR_HANDLING_ATTRIBUTE_DISCARD, err.(*MessageError).ErrorHandling)
	// Every offending attribute must be removed, not only the first error.
	require.Len(t, msg.PathAttributes, 3)
	ok, err = ValidateUpdateMsg(msg, map[Family]BGPAddPathMode{RF_IPv4_UC: BGP_ADD_PATH_NONE}, true, false, false)
	require.True(t, ok)
	require.NoError(t, err)
}

func TestValidateASZeroDoesNotMaskMissingMandatoryAttribute(t *testing.T) {
	agg, err := NewPathAttributeAggregator(uint32(0), netip.MustParseAddr("192.0.2.1"))
	require.NoError(t, err)
	msg := bgpupdate().Body.(*BGPUpdate)
	msg.PathAttributes = []PathAttributeInterface{agg}
	ok, err := ValidateUpdateMsg(msg, map[Family]BGPAddPathMode{RF_IPv4_UC: BGP_ADD_PATH_NONE}, false, false, false)
	require.False(t, ok)
	require.Error(t, err)
	require.Equal(t, ERROR_HANDLING_TREAT_AS_WITHDRAW, err.(*MessageError).ErrorHandling)
}

func TestValidateASNumbersNonzero(t *testing.T) {
	for _, asn := range []uint32{1, 23456, 65001, 65535, 65536, 4294967295} {
		agg, err := NewPathAttributeAggregator(asn, netip.MustParseAddr("192.0.2.1"))
		require.NoError(t, err)
		as4agg, err := NewPathAttributeAs4Aggregator(asn, netip.MustParseAddr("192.0.2.1"))
		require.NoError(t, err)
		for _, attr := range []PathAttributeInterface{
			NewPathAttributeAsPath([]AsPathParamInterface{NewAs4PathParam(BGP_ASPATH_ATTR_TYPE_SEQ, []uint32{asn})}),
			NewPathAttributeAs4Path([]*As4PathParam{NewAs4PathParam(BGP_ASPATH_ATTR_TYPE_SEQ, []uint32{asn})}), agg, as4agg,
		} {
			ok, err := ValidateAttribute(attr, nil, true, false, false)
			require.True(t, ok, "AS %d in %s", asn, attr.GetType())
			require.NoError(t, err)
		}
	}
	// A locally originated path may legitimately have no AS segments.
	ok, err := ValidateAttribute(NewPathAttributeAsPath(nil), nil, false, false, false)
	require.True(t, ok)
	require.NoError(t, err)
}
