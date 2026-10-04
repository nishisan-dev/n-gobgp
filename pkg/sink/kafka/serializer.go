package kafka

import (
	"fmt"

	"github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/pkg/event"
	"google.golang.org/protobuf/encoding/protojson"
)

type ProtoJSONSerializer struct{}

func (ProtoJSONSerializer) Serialize(v *api.WatchEventResponse) ([]byte, error) {
	// Empty responses carry snapshot control headers without inventing BGP data.
	if v == nil {
		return nil, fmt.Errorf("missing GoBGP event")
	}
	return protojson.Marshal(v)
}
func (ProtoJSONSerializer) ContentType() string { return "application/json" }

var _ event.Serializer = ProtoJSONSerializer{}

// NewSerializer selects the payload encoding without changing sink delivery.
func NewSerializer(format string) (event.Serializer, error) {
	switch format {
	case "protojson":
		return ProtoJSONSerializer{}, nil
	default:
		return nil, fmt.Errorf("unsupported event serializer %q", format)
	}
}
