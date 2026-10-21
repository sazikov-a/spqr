package planner

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/connmgr"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/tsa"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type targetListTestPlanner struct {
	ready bool
}

func (p targetListTestPlanner) Ready() bool { return p.ready }

func (targetListTestPlanner) PlanQueryTopLevel(context.Context, *rmeta.RoutingMetadataContext, lyx.Node) (plan.Plan, error) {
	panic("virtual target list must not plan backend work")
}

type targetListTestConnMgr struct {
	connmgr.ConnectionMgr
	checks map[string]tsa.CachedCheckResult
}

func (c targetListTestConnMgr) InstanceHealthChecks() map[string]tsa.CachedCheckResult {
	return c.checks
}

func targetListSelect(t *testing.T, query string) *lyx.Select {
	t.Helper()
	stmts, _, err := lyx.Parse(query)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	stmt, ok := stmts[0].(*lyx.Select)
	require.True(t, ok)
	return stmt
}

func targetListVirtualPlan(t *testing.T, query string, rm *rmeta.RoutingMetadataContext, ready bool) *plan.VirtualPlan {
	t.Helper()
	p, err := PlanTargetList(context.Background(), rm, targetListTestPlanner{ready: ready}, targetListSelect(t, query))
	require.NoError(t, err)
	virtualPlan, ok := p.(*plan.VirtualPlan)
	require.True(t, ok, "unexpected plan %T", p)
	require.NotNil(t, virtualPlan.TTS)
	require.Len(t, virtualPlan.TTS.Raw, 1)
	return virtualPlan
}

func TestPlanTargetListBooleanResultFormats(t *testing.T) {
	for _, state := range []struct {
		name   string
		tsa    string
		ready  bool
		text   string
		binary []byte
	}{
		{"read-write-ready", config.TargetSessionAttrsRW, true, "ftft", []byte{0, 1, 0, 1}},
		{"read-only-not-ready", config.TargetSessionAttrsRO, false, "tftf", []byte{1, 0, 1, 0}},
	} {
		for _, formats := range []struct {
			name  string
			codes []int16
			want  []int16
		}{
			{"omitted", nil, []int16{0, 0, 0, 0}},
			{"text", []int16{0}, []int16{0, 0, 0, 0}},
			{"binary", []int16{1}, []int16{1, 1, 1, 1}},
			{"mixed", []int16{1, 0, 1, 0}, []int16{1, 0, 1, 0}},
		} {
			t.Run(state.name+"/"+formats.name, func(t *testing.T) {
				sph := session.NewSimpleHandler("", false, "")
				sph.SetTsa(session.VirtualParamLevelStatement, state.tsa)
				rm := &rmeta.RoutingMetadataContext{SPH: sph, LastResultFormatCodes: formats.codes}
				p := targetListVirtualPlan(t,
					"SELECT pg_is_in_recovery()::bool, NOT pg_is_in_recovery(), current_setting('transaction_read_only')::bool, __spqr__is_ready()",
					rm, state.ready)
				require.Len(t, p.TTS.Desc, len(state.text))
				require.Len(t, p.TTS.Raw[0], len(state.text))
				for i := range state.text {
					assert.Equal(t, uint32(catalog.BOOLOID), p.TTS.Desc[i].DataTypeOID)
					assert.Equal(t, int16(1), p.TTS.Desc[i].DataTypeSize)
					assert.Equal(t, formats.want[i], p.TTS.Desc[i].Format)
					want := state.text[i]
					if formats.want[i] == 1 {
						want = state.binary[i]
					}
					assert.Equal(t, []byte{want}, p.TTS.Raw[0][i])
				}
			})
		}
	}
}

func TestPlanTargetListInt4ResultFormats(t *testing.T) {
	for _, value := range []struct {
		value  int64
		text   string
		binary []byte
	}{
		{0, "0", []byte{0, 0, 0, 0}},
		{1, "1", []byte{0, 0, 0, 1}},
		{-1, "-1", []byte{255, 255, 255, 255}},
		{42, "42", []byte{0, 0, 0, 42}},
		{-42, "-42", []byte{255, 255, 255, 214}},
		{math.MinInt32, "-2147483648", []byte{128, 0, 0, 0}},
		{math.MaxInt32, "2147483647", []byte{127, 255, 255, 255}},
	} {
		for _, formats := range []struct {
			name  string
			codes []int16
			want  int16
		}{
			{"omitted", nil, 0},
			{"text", []int16{0}, 0},
			{"binary", []int16{1}, 1},
		} {
			t.Run(fmt.Sprintf("%d/%s", value.value, formats.name), func(t *testing.T) {
				rm := &rmeta.RoutingMetadataContext{LastResultFormatCodes: formats.codes}
				p := targetListVirtualPlan(t, fmt.Sprintf("SELECT %d", value.value), rm, false)
				require.Len(t, p.TTS.Desc, 1)
				assert.Equal(t, uint32(catalog.INT4OID), p.TTS.Desc[0].DataTypeOID)
				assert.Equal(t, int16(4), p.TTS.Desc[0].DataTypeSize)
				assert.Equal(t, formats.want, p.TTS.Desc[0].Format)
				want := []byte(value.text)
				if formats.want == 1 {
					want = value.binary
				}
				assert.Equal(t, [][]byte{want}, p.TTS.Raw[0])
			})
		}
	}
}

func TestPlanTargetListInt4OverflowDelegates(t *testing.T) {
	for _, value := range []int64{math.MinInt32 - 1, math.MaxInt32 + 1} {
		for _, codes := range [][]int16{nil, {0}, {1}} {
			t.Run(fmt.Sprintf("%d/%v", value, codes), func(t *testing.T) {
				rm := &rmeta.RoutingMetadataContext{LastResultFormatCodes: codes}
				stmt := targetListSelect(t, fmt.Sprintf("SELECT 1, %d, 'hello'", value))
				p, err := PlanTargetList(context.Background(), rm, targetListTestPlanner{}, stmt)
				require.NoError(t, err)
				assert.IsType(t, &plan.RandomDispatchPlan{}, p)
				virtualPlan, err := PlanVirtualTargetList(context.Background(), rm, targetListTestPlanner{}, stmt)
				require.NoError(t, err)
				assert.Nil(t, virtualPlan)
			})
		}
	}
}

func TestPlanTargetListTextResultFormats(t *testing.T) {
	for _, codes := range [][]int16{nil, {0}, {1}, {1, 0}} {
		t.Run(fmt.Sprint(codes), func(t *testing.T) {
			sph := session.NewSimpleHandler("", false, "")
			sph.SetUsr("alice")
			rm := &rmeta.RoutingMetadataContext{SPH: sph, LastResultFormatCodes: codes}
			p := targetListVirtualPlan(t, "SELECT 'hello', current_user", rm, false)
			require.Len(t, p.TTS.Desc, 2)
			assert.Equal(t, [][]byte{[]byte("hello"), []byte("alice")}, p.TTS.Raw[0])
			want := []int16{0, 0}
			if len(codes) == 1 {
				want = []int16{codes[0], codes[0]}
			} else if len(codes) == 2 {
				want = codes
			}
			for i := range p.TTS.Desc {
				assert.Equal(t, uint32(catalog.TEXTOID), p.TTS.Desc[i].DataTypeOID)
				assert.Equal(t, int16(-1), p.TTS.Desc[i].DataTypeSize)
				assert.Equal(t, want[i], p.TTS.Desc[i].Format)
			}
		})
	}
}

func TestPlanTargetListHostFormatsUseOutputColumnOrder(t *testing.T) {
	for _, rw := range []bool{false, true} {
		for _, codes := range [][]int16{{0, 1, 1, 0}, {1, 0, 0, 1}} {
			t.Run(fmt.Sprintf("%t/%v", rw, codes), func(t *testing.T) {
				sph := session.NewSimpleHandler("", false, "")
				sph.SetTsa(session.VirtualParamLevelStatement, config.TargetSessionAttrsRW)
				rm := &rmeta.RoutingMetadataContext{
					SPH:                   sph,
					LastResultFormatCodes: codes,
					CSM: targetListTestConnMgr{checks: map[string]tsa.CachedCheckResult{
						"backend": {CR: tsa.CheckResult{RW: rw}},
					}},
				}
				p := targetListVirtualPlan(t, "SELECT __spqr__host_status('backend'), 1, pg_is_in_recovery()", rm, false)
				require.Len(t, p.TTS.Desc, 4)
				for i := range p.TTS.Desc {
					assert.Equal(t, codes[i], p.TTS.Desc[i].Format)
				}
				assert.Equal(t, uint32(catalog.TEXTOID), p.TTS.Desc[0].DataTypeOID)
				assert.Equal(t, uint32(catalog.TEXTOID), p.TTS.Desc[1].DataTypeOID)
				wantInt, wantBool := []byte("1"), []byte("f")
				if codes[2] == 1 {
					wantInt = []byte{0, 0, 0, 1}
				}
				if codes[3] == 1 {
					wantBool = []byte{0}
				}
				assert.Equal(t, [][]byte{[]byte("backend"), fmt.Append(nil, rw), wantInt, wantBool}, p.TTS.Raw[0])
			})
		}
	}
}

func TestPlanVirtualTargetListDoesNotPlanBackendWork(t *testing.T) {
	for _, query := range []string{
		"SELECT pg_backend_pid()",
		"SELECT f((SELECT 1))",
		"SELECT (SELECT 1)",
		"SELECT 1 FROM t",
		"SELECT 1 UNION SELECT 2",
		"WITH x AS (SELECT 1) SELECT 1",
		"SELECT 1, unknown_column",
	} {
		t.Run(query, func(t *testing.T) {
			p, err := PlanVirtualTargetList(context.Background(), &rmeta.RoutingMetadataContext{}, targetListTestPlanner{}, targetListSelect(t, query))
			require.NoError(t, err)
			assert.Nil(t, p)
		})
	}
}

func TestPlanVirtualTargetListCreatesIndependentResults(t *testing.T) {
	rm := &rmeta.RoutingMetadataContext{LastResultFormatCodes: []int16{0}}
	stmt := targetListSelect(t, "SELECT 1, 'hello'")
	textPlan, err := PlanVirtualTargetList(context.Background(), rm, targetListTestPlanner{}, stmt)
	require.NoError(t, err)
	require.NotNil(t, textPlan)
	rm.LastResultFormatCodes = []int16{1}
	binaryPlan, err := PlanVirtualTargetList(context.Background(), rm, targetListTestPlanner{}, stmt)
	require.NoError(t, err)
	require.NotNil(t, binaryPlan)
	assert.Equal(t, int16(0), textPlan.TTS.Desc[0].Format)
	assert.Equal(t, [][]byte{[]byte("1"), []byte("hello")}, textPlan.TTS.Raw[0])
	assert.Equal(t, int16(1), binaryPlan.TTS.Desc[0].Format)
	assert.Equal(t, [][]byte{{0, 0, 0, 1}, []byte("hello")}, binaryPlan.TTS.Raw[0])
}
