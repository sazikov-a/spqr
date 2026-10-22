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
	"github.com/pg-sharding/spqr/router/xproto"
	"github.com/stretchr/testify/require"
)

func TestPlanTargetListIntegerLiteralTypes(t *testing.T) {
	for _, tt := range []struct {
		literal string
		oid     uint32
		size    int16
		binary  []byte
	}{
		{literal: "42", oid: catalog.INT4OID, size: 4, binary: []byte{0, 0, 0, 42}},
		{literal: "2147483647", oid: catalog.INT4OID, size: 4, binary: []byte{0x7f, 0xff, 0xff, 0xff}},
		{literal: "-2147483648", oid: catalog.INT4OID, size: 4, binary: []byte{0x80, 0, 0, 0}},
		{literal: "2147483648", oid: catalog.INT8OID, size: 8, binary: []byte{0, 0, 0, 0, 0x80, 0, 0, 0}},
		{literal: "-2147483649", oid: catalog.INT8OID, size: 8, binary: []byte{0xff, 0xff, 0xff, 0xff, 0x7f, 0xff, 0xff, 0xff}},
		{literal: "4294967296", oid: catalog.INT8OID, size: 8, binary: []byte{0, 0, 0, 1, 0, 0, 0, 0}},
		{literal: "9223372036854775807", oid: catalog.INT8OID, size: 8, binary: []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	} {
		t.Run(tt.literal, func(t *testing.T) {
			parsed, _, err := lyx.Parse("SELECT " + tt.literal)
			require.NoError(t, err)
			stmt, ok := parsed[0].(*lyx.Select)
			require.True(t, ok)
			for _, format := range []int16{xproto.FormatCodeText, xproto.FormatCodeBinary} {
				rm := &rmeta.RoutingMetadataContext{LastResultFormatCodes: []int16{format}}
				p, err := PlanTargetList(context.Background(), rm, nil, stmt)
				require.NoError(t, err)
				virtual, ok := p.(*plan.VirtualPlan)
				require.True(t, ok)
				require.Len(t, virtual.TTS.Desc, 1)
				require.Equal(t, tt.oid, virtual.TTS.Desc[0].DataTypeOID)
				require.Equal(t, tt.size, virtual.TTS.Desc[0].DataTypeSize)
				require.Equal(t, format, virtual.TTS.Desc[0].Format)
				want := []byte(tt.literal)
				if format == xproto.FormatCodeBinary {
					want = tt.binary
				}
				require.Equal(t, [][][]byte{{want}}, virtual.TTS.Raw)
			}
		})
	}
}

type resultFormatPlanner struct {
	QueryPlanner
	ready bool
}

func (p resultFormatPlanner) Ready() bool { return p.ready }

func TestPlanTargetListMixedResultFormats(t *testing.T) {
	for _, ready := range []bool{false, true} {
		for _, tt := range []struct {
			name    string
			formats []int16
			want    []int16
		}{
			{name: "default text", want: []int16{0, 0, 0, 0, 0}},
			{name: "all text", formats: []int16{0}, want: []int16{0, 0, 0, 0, 0}},
			{name: "all binary", formats: []int16{1}, want: []int16{1, 1, 1, 1, 1}},
			{name: "mixed", formats: []int16{0, 1, 0, 1, 0}, want: []int16{0, 1, 0, 1, 0}},
			{name: "mixed inverse", formats: []int16{1, 0, 1, 0, 1}, want: []int16{1, 0, 1, 0, 1}},
		} {
			t.Run(fmt.Sprintf("%s/ready=%v", tt.name, ready), func(t *testing.T) {
				parsed, _, err := lyx.Parse("SELECT 42, 2147483648, 'hello', current_user, " + virtual.VirtualFuncIsReady + "()")
				require.NoError(t, err)
				stmt, ok := parsed[0].(*lyx.Select)
				require.True(t, ok)
				sph := session.NewSimpleHandler(config.TargetSessionAttrsRW, false, "")
				sph.SetUsr("test-user")
				rm := &rmeta.RoutingMetadataContext{SPH: sph, LastResultFormatCodes: tt.formats}
				p, err := PlanTargetList(context.Background(), rm, resultFormatPlanner{ready: ready}, stmt)
				require.NoError(t, err)
				virtual, ok := p.(*plan.VirtualPlan)
				require.True(t, ok)
				want := [][]byte{[]byte("42"), []byte("2147483648"), []byte("hello"), []byte("test-user"), []byte("f")}
				if ready {
					want[4] = []byte("t")
				}
				if tt.want[0] == xproto.FormatCodeBinary {
					want[0] = []byte{0, 0, 0, 42}
				}
				if tt.want[1] == xproto.FormatCodeBinary {
					want[1] = []byte{0, 0, 0, 0, 0x80, 0, 0, 0}
				}
				if tt.want[4] == xproto.FormatCodeBinary {
					want[4] = []byte{0}
					if ready {
						want[4][0] = 1
					}
				}
				require.Len(t, virtual.TTS.Desc, 5)
				for i, format := range tt.want {
					require.Equal(t, format, virtual.TTS.Desc[i].Format, "column %d", i)
				}
				require.Equal(t, [][][]byte{want}, virtual.TTS.Raw)
			})
		}
	}
}

type resultFormatConnectionMgr struct {
	connmgr.ConnectionMgr
}

func (resultFormatConnectionMgr) InstanceHealthChecks() map[string]tsa.CachedCheckResult {
	return map[string]tsa.CachedCheckResult{"test-host": {CR: tsa.CheckResult{RW: true}}}
}

func TestPlanTargetListHostResultFormats(t *testing.T) {
	for _, formats := range [][]int16{nil, {0}, {1}, {0, 1}, {1, 0}} {
		parsed, _, err := lyx.Parse("SELECT " + virtual.VirtualFuncHosts + "('test-host')")
		require.NoError(t, err)
		rm := &rmeta.RoutingMetadataContext{CSM: resultFormatConnectionMgr{}, LastResultFormatCodes: formats}
		p, err := PlanTargetList(context.Background(), rm, nil, parsed[0].(*lyx.Select))
		require.NoError(t, err)
		virtual, ok := p.(*plan.VirtualPlan)
		require.True(t, ok)
		require.Len(t, virtual.TTS.Desc, 2)
		for i := range virtual.TTS.Desc {
			require.Equal(t, rm.GetResultFormatCode(i), virtual.TTS.Desc[i].Format)
		}
		require.Equal(t, [][][]byte{{[]byte("test-host"), []byte("true")}}, virtual.TTS.Raw)
	}

	// The host function produces two result columns from one target expression.
	parsed, _, err := lyx.Parse("SELECT " + virtual.VirtualFuncHosts + "('test-host'), 42")
	require.NoError(t, err)
	rm := &rmeta.RoutingMetadataContext{
		CSM:                   resultFormatConnectionMgr{},
		LastResultFormatCodes: []int16{0, 0, 1},
	}
	p, err := PlanTargetList(context.Background(), rm, nil, parsed[0].(*lyx.Select))
	require.NoError(t, err)
	virtualPlan, ok := p.(*plan.VirtualPlan)
	require.True(t, ok)
	require.Equal(t, []int16{0, 0, 1}, []int16{
		virtualPlan.TTS.Desc[0].Format,
		virtualPlan.TTS.Desc[1].Format,
		virtualPlan.TTS.Desc[2].Format,
	})
	require.Equal(t, [][][]byte{{[]byte("test-host"), []byte("true"), {0, 0, 0, 42}}}, virtualPlan.TTS.Raw)
}

func TestPlanTargetListBooleanResultFormats(t *testing.T) {
	for _, tsa := range []string{config.TargetSessionAttrsRW, config.TargetSessionAttrsRO} {
		t.Run(tsa, func(t *testing.T) {
			for _, tt := range []struct {
				name    string
				formats []int16
				want    []int16
			}{
				{name: "default text", want: []int16{0, 0, 0}},
				{name: "all text", formats: []int16{0}, want: []int16{0, 0, 0}},
				{name: "all binary", formats: []int16{1}, want: []int16{1, 1, 1}},
				{name: "mixed", formats: []int16{0, 1, 0}, want: []int16{0, 1, 0}},
				{name: "mixed inverse", formats: []int16{1, 0, 1}, want: []int16{1, 0, 1}},
			} {
				t.Run(tt.name, func(t *testing.T) {
					parsed, _, err := lyx.Parse("SELECT pg_is_in_recovery(), NOT pg_is_in_recovery(), current_setting('transaction_read_only')")
					require.NoError(t, err)
					stmt, ok := parsed[0].(*lyx.Select)
					require.True(t, ok)
					rm := &rmeta.RoutingMetadataContext{
						SPH:                   session.NewSimpleHandler(tsa, false, ""),
						LastResultFormatCodes: tt.formats,
					}

					p, err := PlanTargetList(context.Background(), rm, nil, stmt)
					require.NoError(t, err)
					virtual, ok := p.(*plan.VirtualPlan)
					require.True(t, ok)
					require.Len(t, virtual.TTS.Desc, 3)
					require.Len(t, virtual.TTS.Raw, 1)
					require.Len(t, virtual.TTS.Raw[0], 3)

					for i, wantFormat := range tt.want {
						require.Equal(t, wantFormat, virtual.TTS.Desc[i].Format, "column %d", i)
						wantValue := tsa == config.TargetSessionAttrsRO
						if i == 1 {
							wantValue = !wantValue
						}
						var want []byte
						if wantFormat == xproto.FormatCodeBinary {
							want = []byte{0}
							if wantValue {
								want[0] = 1
							}
						} else {
							want = []byte("f")
							if wantValue {
								want = []byte("t")
							}
						}
						require.Equal(t, want, virtual.TTS.Raw[0][i], "column %d", i)
					}
				})
			}
		})
	}
}
