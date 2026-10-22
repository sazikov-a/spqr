package planner

import (
	"context"
	"fmt"
	"testing"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/connmgr"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/virtual"
	"github.com/stretchr/testify/require"
)

func TestPlanTargetListIntegerLiteralTypes(t *testing.T) {
	for _, tt := range []struct {
		literal string
		oid     uint32
		size    int16
	}{
		{literal: "42", oid: catalog.INT4OID, size: 4},
		{literal: "2147483647", oid: catalog.INT4OID, size: 4},
		{literal: "-2147483648", oid: catalog.INT4OID, size: 4},
		{literal: "2147483648", oid: catalog.INT8OID, size: 8},
		{literal: "-2147483649", oid: catalog.INT8OID, size: 8},
		{literal: "4294967296", oid: catalog.INT8OID, size: 8},
		{literal: "9223372036854775807", oid: catalog.INT8OID, size: 8},
	} {
		t.Run(tt.literal, func(t *testing.T) {
			parsed, _, err := lyx.Parse("SELECT " + tt.literal)
			require.NoError(t, err)
			p, err := PlanTargetList(context.Background(), &rmeta.RoutingMetadataContext{}, nil, parsed[0].(*lyx.Select))
			require.NoError(t, err)
			vp := p.(*plan.VirtualPlan)
			require.Len(t, vp.TTS.Desc, 1)
			require.Equal(t, tt.oid, vp.TTS.Desc[0].DataTypeOID)
			require.Equal(t, tt.size, vp.TTS.Desc[0].DataTypeSize)
			require.Equal(t, int16(0), vp.TTS.Desc[0].Format)
			require.Equal(t, [][][]byte{{[]byte(tt.literal)}}, vp.TTS.Raw)
		})
	}
}

type readyPlanner struct {
	QueryPlanner
	ready bool
}

func (p readyPlanner) Ready() bool { return p.ready }

func TestPlanTargetListTextResults(t *testing.T) {
	for _, ready := range []bool{false, true} {
		t.Run(fmt.Sprintf("ready=%v", ready), func(t *testing.T) {
			parsed, _, err := lyx.Parse("SELECT 42, 2147483648, 'hello', current_user, " + virtual.VirtualFuncIsReady + "()")
			require.NoError(t, err)
			sph := session.NewSimpleHandler(config.TargetSessionAttrsRW, false, "")
			sph.SetUsr("test-user")
			rm := &rmeta.RoutingMetadataContext{SPH: sph}
			p, err := PlanTargetList(context.Background(), rm, readyPlanner{ready: ready}, parsed[0].(*lyx.Select))
			require.NoError(t, err)
			vp := p.(*plan.VirtualPlan)
			want := [][]byte{[]byte("42"), []byte("2147483648"), []byte("hello"), []byte("test-user"), []byte("f")}
			if ready {
				want[4] = []byte("t")
			}
			require.Len(t, vp.TTS.Desc, 5)
			for i, desc := range vp.TTS.Desc {
				require.Equal(t, int16(0), desc.Format, "column %d", i)
			}
			require.Equal(t, [][][]byte{want}, vp.TTS.Raw)
		})
	}
}

type hostStatusConnectionMgr struct {
	connmgr.ConnectionMgr
}

func (hostStatusConnectionMgr) InstanceHealthChecks() map[string]tsa.CachedCheckResult {
	return map[string]tsa.CachedCheckResult{"test-host": {CR: tsa.CheckResult{RW: true}}}
}

func TestPlanTargetListHostTextResults(t *testing.T) {
	for _, suffix := range []string{"", ", 42"} {
		t.Run(suffix, func(t *testing.T) {
			parsed, _, err := lyx.Parse("SELECT " + virtual.VirtualFuncHosts + "('test-host')" + suffix)
			require.NoError(t, err)
			rm := &rmeta.RoutingMetadataContext{CSM: hostStatusConnectionMgr{}}
			p, err := PlanTargetList(context.Background(), rm, nil, parsed[0].(*lyx.Select))
			require.NoError(t, err)
			vp := p.(*plan.VirtualPlan)
			want := [][]byte{[]byte("test-host"), []byte("true")}
			if suffix != "" {
				want = append(want, []byte("42"))
			}
			require.Len(t, vp.TTS.Desc, len(want))
			for i, desc := range vp.TTS.Desc {
				require.Equal(t, int16(0), desc.Format, "column %d", i)
			}
			require.Equal(t, [][][]byte{want}, vp.TTS.Raw)
		})
	}
}

func TestPlanTargetListBooleanTextResults(t *testing.T) {
	for _, tsa := range []string{config.TargetSessionAttrsRW, config.TargetSessionAttrsRO} {
		t.Run(tsa, func(t *testing.T) {
			parsed, _, err := lyx.Parse("SELECT pg_is_in_recovery(), NOT pg_is_in_recovery(), current_setting('transaction_read_only')")
			require.NoError(t, err)
			rm := &rmeta.RoutingMetadataContext{SPH: session.NewSimpleHandler(tsa, false, "")}
			p, err := PlanTargetList(context.Background(), rm, nil, parsed[0].(*lyx.Select))
			require.NoError(t, err)
			vp := p.(*plan.VirtualPlan)
			want := [][]byte{[]byte("f"), []byte("t"), []byte("f")}
			if tsa == config.TargetSessionAttrsRO {
				want = [][]byte{[]byte("t"), []byte("f"), []byte("t")}
			}
			require.Len(t, vp.TTS.Desc, 3)
			for i, desc := range vp.TTS.Desc {
				require.Equal(t, int16(0), desc.Format, "column %d", i)
			}
			require.Equal(t, [][][]byte{want}, vp.TTS.Raw)
		})
	}
}

func TestConsoleFunctionCallReadyTextResults(t *testing.T) {
	for _, tt := range []struct {
		ready bool
		want  string
	}{
		{want: "f"},
		{ready: true, want: "t"},
	} {
		tts, err := ConsoleFunctionCall(context.Background(), &rmeta.RoutingMetadataContext{}, readyPlanner{ready: tt.ready}, virtual.VirtualFuncIsReady, nil)
		require.NoError(t, err)
		require.Len(t, tts.Desc, 1)
		require.Equal(t, int16(0), tts.Desc[0].Format)
		require.Equal(t, [][][]byte{{[]byte(tt.want)}}, tts.Raw)
	}
}
