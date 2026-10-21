package relay

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	mockcl "github.com/pg-sharding/spqr/router/mock/client"
	mockcmgr "github.com/pg-sharding/spqr/router/mock/poolmgr"
	mockqr "github.com/pg-sharding/spqr/router/mock/qrouter"
	"github.com/pg-sharding/spqr/router/planner"
	"github.com/pg-sharding/spqr/router/qrouter"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// The production proxy router implements both interfaces. Backend planning is
// deliberately unavailable here so these tests catch accidental rerouting.
type virtualReuseQueryRouter struct {
	qrouter.QueryRouter
	t *testing.T
}

func (qr virtualReuseQueryRouter) PlanQueryTopLevel(context.Context, *rmeta.RoutingMetadataContext, lyx.Node) (plan.Plan, error) {
	qr.t.Fatal("virtual result refresh must not plan backend work")
	return nil, nil
}

func virtualReuseState(t *testing.T, sql string) (*RelayStateImpl, *rmeta.RoutingMetadataContext) {
	t.Helper()
	stmts, _, err := lyx.Parse(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	ctrl := gomock.NewController(t)
	cl := mockcl.NewMockRouterClient(ctrl)
	cl.EXPECT().ID().Return(uint(1)).AnyTimes()
	cl.EXPECT().Usr().Return("user1").AnyTimes()
	cl.EXPECT().DB().Return("db1").AnyTimes()
	cl.EXPECT().EnhancedMultiShardProcessing().Return(false).AnyTimes()
	pool := mockcmgr.NewMockPoolMgr(ctrl)
	pool.EXPECT().ValidateGangChange(gomock.Any()).Return(false).AnyTimes()
	qr := mockqr.NewMockQueryRouter(ctrl)
	rst := &RelayStateImpl{
		Cl:      cl,
		Qr:      virtualReuseQueryRouter{QueryRouter: qr, t: t},
		poolMgr: pool,
	}
	rst.qp.SetStmt(stmts[0])
	return rst, &rmeta.RoutingMetadataContext{
		SPH:  session.NewSimpleHandler(config.TargetSessionAttrsRW, false, ""),
		Stmt: stmts[0],
	}
}

func TestPrepareExecutionSliceRefreshesVirtualResultFormats(t *testing.T) {
	rst, rm := virtualReuseState(t, "SELECT 1, pg_is_in_recovery()")
	stmt := rm.Stmt.(*lyx.Select)
	previous, err := planner.PlanVirtualTargetList(context.Background(), rm, rst.Qr.(planner.QueryPlanner), stmt)
	require.NoError(t, err)
	require.NotNil(t, previous)
	previous.SetStmt(stmt)
	require.Equal(t, [][]byte{[]byte("1"), []byte("f")}, previous.TTS.Raw[0])

	rm.LastResultFormatCodes = []int16{1}
	bound, err := rst.PrepareExecutionSlice(context.Background(), rm, previous)
	require.NoError(t, err)
	require.IsType(t, &plan.VirtualPlan{}, bound)
	binaryPlan := bound.(*plan.VirtualPlan)
	require.NotSame(t, previous, binaryPlan)
	require.Same(t, stmt, binaryPlan.Stmt())
	require.Equal(t, int16(1), binaryPlan.TTS.Desc[0].Format)
	require.Equal(t, int16(1), binaryPlan.TTS.Desc[1].Format)
	require.Equal(t, [][]byte{{0, 0, 0, 1}, {0}}, binaryPlan.TTS.Raw[0])
	require.Equal(t, int16(0), previous.TTS.Desc[0].Format)
	require.Equal(t, [][]byte{[]byte("1"), []byte("f")}, previous.TTS.Raw[0])

	rm.LastResultFormatCodes = nil
	bound, err = rst.PrepareExecutionSlice(context.Background(), rm, binaryPlan)
	require.NoError(t, err)
	textPlan := bound.(*plan.VirtualPlan)
	require.NotSame(t, binaryPlan, textPlan)
	require.Equal(t, int16(0), textPlan.TTS.Desc[0].Format)
	require.Equal(t, int16(0), textPlan.TTS.Desc[1].Format)
	require.Equal(t, [][]byte{[]byte("1"), []byte("f")}, textPlan.TTS.Raw[0])
	require.Equal(t, int16(1), binaryPlan.TTS.Desc[0].Format)
	require.Equal(t, [][]byte{{0, 0, 0, 1}, {0}}, binaryPlan.TTS.Raw[0])
}

func TestPrepareExecutionSlicePreservesOtherPlanReuse(t *testing.T) {
	withSubplan := virtualReuseSentinel()
	withSubplan.SubPlan = &plan.ScatterPlan{}
	for _, tt := range []struct {
		name     string
		query    string
		previous plan.Plan
	}{
		{
			name: "backend plan", query: "SELECT 1",
			previous: &plan.ShardDispatchPlan{},
		},
		{
			name: "SHOW result", query: "SHOW transaction_read_only",
			previous: virtualReuseSentinel(),
		},
		{
			name: "metadata result without FROM", query: "SELECT __spqr__show('clients')",
			previous: virtualReuseSentinel(),
		},
		{
			name: "metadata result with FROM", query: "SELECT * FROM __spqr__show('clients')",
			previous: virtualReuseSentinel(),
		},
		{
			name: "virtual result with subplan", query: "SELECT 1",
			previous: withSubplan,
		},
		{
			name: "unknown function with subquery", query: "SELECT backend_func((SELECT 1))",
			previous: virtualReuseSentinel(),
		},
		{
			name: "integer requiring backend", query: "SELECT 2147483648",
			previous: virtualReuseSentinel(),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rst, rm := virtualReuseState(t, tt.query)
			rm.LastResultFormatCodes = []int16{1}
			bound, err := rst.PrepareExecutionSlice(context.Background(), rm, tt.previous)
			require.NoError(t, err)
			require.Same(t, tt.previous, bound)
			require.Same(t, rm.Stmt, bound.Stmt())
			if p, ok := bound.(*plan.VirtualPlan); ok {
				require.Equal(t, int16(0), p.TTS.Desc[0].Format)
				require.Equal(t, [][]byte{[]byte("sentinel")}, p.TTS.Raw[0])
			}
		})
	}
}

func virtualReuseSentinel() *plan.VirtualPlan {
	return &plan.VirtualPlan{TTS: &tupleslot.TupleTableSlot{
		Desc: []pgproto3.FieldDescription{{DataTypeOID: catalog.TEXTOID, Format: 0}},
		Raw:  [][][]byte{{[]byte("sentinel")}},
	}}
}
