package relay

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/meta"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	mockpm "github.com/pg-sharding/spqr/router/mock/poolmgr"
	"github.com/pg-sharding/spqr/router/planner"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestBoundVirtualPortalsOwnResultFormats(t *testing.T) {
	for _, order := range [][]string{{"text", "binary"}, {"binary", "text"}} {
		t.Run(order[0]+" first", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			cl, replies := resultFormatClient(t, ctrl, nil)
			pm := mockpm.NewMockPoolMgr(ctrl)
			const query = "SELECT 42"
			def := &prepstatement.PreparedStatementDefinition{Name: "s", Query: query}
			cl.EXPECT().PreparedStatementDefinitionByName("s").Times(2).Return(def)
			cl.EXPECT().PreparedStatementQueryHashByName("s").Times(2).Return(uint64(123))
			cl.EXPECT().ReplyBindComplete().Times(2).Return(nil)
			cl.EXPECT().SetBindParams(nil).Times(2)
			cl.EXPECT().SetParamFormatCodes(nil).Times(2)
			cl.EXPECT().Usr().Times(2).Return("u")
			cl.EXPECT().DB().Times(2).Return("d")
			cl.EXPECT().EnhancedMultiShardProcessing().Times(2).Return(false)
			pm.EXPECT().ValidateGangChange(gomock.Any()).Times(2).Return(false)
			stmts, _, err := lyx.Parse(query)
			require.NoError(t, err)
			rm := &rmeta.RoutingMetadataContext{Stmt: stmts[0]}
			p, err := planner.PlanTargetList(context.Background(), rm, nil, stmts[0].(*lyx.Select))
			require.NoError(t, err)
			rst := &RelayStateImpl{
				Cl: cl, poolMgr: pm, qse: NewQueryStateExecutor(nil, nil, pm, cl),
				routingDecisionPlan: p,
				bindQueryPlanMP:     map[string]plan.Plan{},
				lastBindNameMP:      map[string]string{},
				saveBindNamed:       map[string]*pgproto3.Bind{},
				executeMp:           map[string]func(uint32) error{},
				savedPortalDesc:     map[string]*PortalDesc{},
				savedRM:             map[string]*rmeta.RoutingMetadataContext{"s": rm},
				parseCache:          map[string]ParseCacheEntry{query: {ps: stmts, stmt: stmts[0]}},
			}
			// Reuse the frontend slice to ensure each Bind takes ownership.
			formats := []int16{0}
			require.NoError(t, rst.BindPrepared(context.Background(), "s", "text", nil, nil, formats))
			formats[0] = 1
			require.NoError(t, rst.BindPrepared(context.Background(), "s", "binary", nil, nil, formats))
			formats[0] = 2
			require.Equal(t, []int16{0}, rst.saveBindNamed["text"].ResultFormatCodes)
			require.Equal(t, []int16{1}, rst.saveBindNamed["binary"].ResultFormatCodes)

			for _, portal := range order {
				start := len(*replies)
				require.NoError(t, rst.DescribePrepared('P', portal, nil))
				require.NoError(t, rst.ExecutePortal(portal, 0))
				messages := (*replies)[start:]
				require.Len(t, messages, 2)
				field := messages[0].(*pgproto3.RowDescription).Fields[0]
				row := messages[1].(*pgproto3.DataRow)
				wantFormat := int16(0)
				if portal == "binary" {
					wantFormat = 1
				}
				require.Equal(t, wantFormat, field.Format)
				var value int32
				require.NoError(t, pgtype.NewMap().Scan(field.DataTypeOID, field.Format, row.Values[0], &value))
				require.Equal(t, int32(42), value)
			}
			require.Zero(t, p.(*plan.VirtualPlan).TTS.Desc[0].Format)
			require.Equal(t, [][][]byte{{[]byte("42")}}, p.(*plan.VirtualPlan).TTS.Raw)
		})
	}
}

type startupFinishedManager struct{ meta.EntityMgr }

func (startupFinishedManager) StartupFinished() bool { return true }

func TestConsoleBooleanPortalBinaryDecoding(t *testing.T) {
	ctrl := gomock.NewController(t)
	cl, replies := resultFormatClient(t, ctrl, nil)
	rm := &rmeta.RoutingMetadataContext{Mgr: startupFinishedManager{}}
	tts, err := planner.ConsoleFunctionCall(context.Background(), rm, nil, "__spqr__show", []lyx.Node{&lyx.AExprSConst{Value: "startup_finished"}})
	require.NoError(t, err)
	rst := &RelayStateImpl{
		Cl: cl, qse: NewQueryStateExecutor(nil, nil, nil, cl),
		unnamedPortalExists: true,
		saveBind:            pgproto3.Bind{ResultFormatCodes: []int16{1}},
		bindQueryPlan:       &plan.VirtualPlan{TTS: tts},
	}
	require.NoError(t, rst.DescribePrepared('P', "", nil))
	require.NoError(t, BindAndReadSliceResult(rst, &rst.saveBind, nil, "", 0))
	require.Len(t, *replies, 2)
	field := (*replies)[0].(*pgproto3.RowDescription).Fields[0]
	row := (*replies)[1].(*pgproto3.DataRow)
	require.Equal(t, int16(1), field.Format)
	var ready bool
	require.NoError(t, pgtype.NewMap().Scan(field.DataTypeOID, field.Format, row.Values[0], &ready))
	require.True(t, ready)
	require.Equal(t, &tupleslot.TupleTableSlot{
		Desc: tts.Desc,
		Raw:  [][][]byte{{[]byte("t")}},
	}, tts)
	require.Zero(t, tts.Desc[0].Format)
}
