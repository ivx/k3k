package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrimaryServiceCIDR(t *testing.T) {
	for in, want := range map[string]string{
		"10.197.0.0/16":                      "10.197.0.0/16",
		"10.197.0.0/16,fd42:0:1a:ffff::/112": "10.197.0.0/16",
		" 10.43.0.0/16 , fd00::/108":         "10.43.0.0/16",
		"fd42:0:1a:ffff::/112,10.197.0.0/16": "fd42:0:1a:ffff::/112",
	} {
		got, err := primaryServiceCIDR(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}

	_, err := primaryServiceCIDR("not-a-cidr")
	assert.Error(t, err)
}
