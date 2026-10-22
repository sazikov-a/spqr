package relay

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/session/sessiontest"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	mockcl "github.com/pg-sharding/spqr/router/mock/client"
	mocksv "github.com/pg-sharding/spqr/router/mock/server"
	"github.com/pg-sharding/spqr/router/planner"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/server"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func resultFormatClient(t *testing.T, ctrl *gomock.Controller, srv server.Server) (*mockcl.MockRouterClient, *[]pgproto3.BackendMessage) {
	t.Helper()
	cl := mockcl.NewMockRouterClient(ctrl)
	cl.EXPECT().CleanupStatementSet().AnyTimes()
	cl.EXPECT().Server().AnyTimes().Return(srv)
	cl.EXPECT().ID().AnyTimes().Return(uint(1))
	cl.EXPECT().FindStrGUC(session.SPQR_EXECUTE_ON).AnyTimes().Return(sessiontest.MustFindStrGUC(session.SPQR_EXECUTE_ON), nil)
	cl.EXPECT().ResolveVirtualStringParam(session.SPQR_EXECUTE_ON, gomock.Any()).AnyTimes().Return("")
	for _, name := range []string{session.SPQR_REPLY_NOTICE, session.SPQR_LINEARIZE_DISPATCH} {
		cl.EXPECT().FindBoolGUC(name).AnyTimes().Return(sessiontest.MustFindBoolGUC(name), nil)
	}
	cl.EXPECT().ResolveVirtualBoolParam(gomock.Any(), gomock.Any()).AnyTimes().Return(false)
	var replies []pgproto3.BackendMessage
	cl.EXPECT().Send(gomock.Any()).AnyTimes().DoAndReturn(func(msg pgproto3.BackendMessage) error {
		replies = append(replies, msg)
		return nil
	})
	return cl, &replies
}

func TestExecuteVirtualResultFormats(t *testing.T) {
	for _, tt := range []struct {
		name    string
		literal string
		formats []int16
		portal  string
		want    [][]byte
	}{
		{name: "default text", literal: "42", want: [][]byte{[]byte("42"), []byte("f"), {}}},
		{name: "binary", literal: "42", formats: []int16{1}, want: [][]byte{{0, 0, 0, 42}, {0}, {}}},
		{name: "mixed", literal: "42", formats: []int16{1, 0, 1}, want: [][]byte{{0, 0, 0, 42}, []byte("f"), {}}},
		{name: "named portal", literal: "42", portal: "p", formats: []int16{1}, want: [][]byte{{0, 0, 0, 42}, {0}, {}}},
		{name: "bigint binary", literal: "4294967296", formats: []int16{1}, want: [][]byte{{0, 0, 0, 1, 0, 0, 0, 0}, {0}, {}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			cl, replies := resultFormatClient(t, ctrl, nil)
			parsed, _, err := lyx.Parse("SELECT " + tt.literal + ", pg_is_in_recovery(), ''")
			require.NoError(t, err)
			rm := &rmeta.RoutingMetadataContext{
				SPH:                   session.NewSimpleHandler(config.TargetSessionAttrsRW, false, ""),
				LastResultFormatCodes: tt.formats,
			}
			p, err := planner.PlanTargetList(context.Background(), rm, nil, parsed[0].(*lyx.Select))
			require.NoError(t, err)
			rst := &RelayStateImpl{
				Cl:              cl,
				qse:             NewQueryStateExecutor(nil, nil, nil, cl),
				bindQueryPlan:   p,
				bindQueryPlanMP: map[string]plan.Plan{},
			}
			if tt.portal != "" {
				rst.bindQueryPlan = nil
				rst.bindQueryPlanMP[tt.portal] = p
			}
			bind := &pgproto3.Bind{DestinationPortal: tt.portal, ResultFormatCodes: tt.formats}
			require.NoError(t, BindAndReadSliceResult(rst, bind, nil, tt.portal, 0))
			require.Equal(t, []pgproto3.BackendMessage{&pgproto3.DataRow{Values: tt.want}}, *replies)
			// The planner produces the requested representation; execution forwards it.
			require.Equal(t, tt.want, p.(*plan.VirtualPlan).TTS.Raw[0])
		})
	}
}

func TestExecuteRewrittenQueryResultFormats(t *testing.T) {
	ctrl := gomock.NewController(t)
	srv := mocksv.NewMockServer(ctrl)
	cl, replies := resultFormatClient(t, ctrl, srv)
	key := kr.ShardKey{Name: "sh1"}
	p := &plan.ScatterPlan{
		ExecTargets:    []kr.ShardKey{key},
		OverwriteQuery: map[string]string{"sh1": "SELECT 42, true, '', custom_value"},
	}
	srv.EXPECT().Datashards().AnyTimes()
	srv.EXPECT().Name().AnyTimes().Return("server")
	formats := []int16{1, 0, 1, 1}
	gomock.InOrder(
		srv.EXPECT().SendShard(&pgproto3.Parse{Query: "SELECT 42, true, '', custom_value"}, key).Return(nil),
		srv.EXPECT().SendShard(&pgproto3.Bind{ResultFormatCodes: formats}, key).Return(nil),
		srv.EXPECT().SendShard(&pgproto3.Execute{}, key).Return(nil),
		srv.EXPECT().SendShard(&pgproto3.Sync{}, key).Return(nil),
	)
	opaque := []byte{0xfe, 0, 0xff}
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.ParseComplete{},
		&pgproto3.BindComplete{},
		&pgproto3.DataRow{Values: [][]byte{{0, 0, 0, 42}, []byte("t"), {}, opaque}},
		&pgproto3.DataRow{Values: [][]byte{nil, nil, nil, nil}},
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 2")},
		&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
	} {
		srv.EXPECT().Receive(gomock.Any()).Return(msg, uint(0), nil)
	}
	qd := &QueryDesc{Msg: &pgproto3.Bind{PreparedStatement: "original", ResultFormatCodes: formats}, exec: pgexec}
	executor := NewQueryStateExecutor(nil, nil, nil, cl)
	require.NoError(t, executor.ExecuteSlice(qd, p, true))
	// Execute must emit rows in the requested formats, without sending another
	// RowDescription after the client's portal Describe.
	require.Equal(t, []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{{0, 0, 0, 42}, []byte("t"), {}, opaque}},
		&pgproto3.DataRow{Values: [][]byte{nil, nil, nil, nil}},
	}, *replies)
}

func TestExecuteSimpleQueryResultFormats(t *testing.T) {
	ctrl := gomock.NewController(t)
	cl, replies := resultFormatClient(t, ctrl, nil)
	// Simple-query results are generated in text from the start.
	p := &plan.VirtualPlan{TTS: &tupleslot.TupleTableSlot{
		Desc: tupleslot.TupleDesc{{DataTypeOID: 16}},
		Raw:  [][][]byte{{[]byte("f")}},
	}}
	executor := NewQueryStateExecutor(nil, nil, nil, cl)
	require.NoError(t, executor.ExecuteSlice(&QueryDesc{Msg: &pgproto3.Query{}, simple: true}, p, true))
	require.Equal(t, []pgproto3.BackendMessage{
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{DataTypeOID: 16}}},
		&pgproto3.DataRow{Values: [][]byte{[]byte("f")}},
	}, *replies)
}

func TestExecuteOutputSubqueryPropagatesBinaryFormats(t *testing.T) {
	ctrl := gomock.NewController(t)
	srv := mocksv.NewMockServer(ctrl)
	cl, replies := resultFormatClient(t, ctrl, srv)
	key := kr.ShardKey{Name: "sh1"}
	query := "SELECT custom_value FROM t WHERE id = $1 LIMIT 1"
	input := []byte{0, 0, 0, 42}
	opaque := []byte{0xfe, 0, 0xff}
	const customOID = 12345678
	output := &plan.VirtualPlan{TTS: &tupleslot.TupleTableSlot{}}
	subplan := &plan.ScatterPlan{
		ExecTargets:    []kr.ShardKey{key},
		OverwriteQuery: map[string]string{key.Name: query},
	}
	output.SubPlan = subplan
	subplan.RunF = func(serv server.Server) error {
		for {
			msg, err := serv.ReceiveShard(1)
			if err != nil {
				return err
			}
			switch msg := msg.(type) {
			case *pgproto3.RowDescription:
				output.TTS.Desc = msg.Fields
			case *pgproto3.DataRow:
				output.TTS.Raw = append(output.TTS.Raw, msg.Values)
			case *pgproto3.ReadyForQuery:
				return nil
			}
		}
	}
	srv.EXPECT().Datashards().AnyTimes()
	srv.EXPECT().Name().AnyTimes().Return("server")
	srv.EXPECT().TxStatus().AnyTimes().Return(txstatus.TXACT)
	gomock.InOrder(
		srv.EXPECT().SendShard(&pgproto3.Parse{Query: query, ParameterOIDs: []uint32{23}}, key).Return(nil),
		srv.EXPECT().SendShard(&pgproto3.Bind{
			ParameterFormatCodes: []int16{1}, Parameters: [][]byte{input}, ResultFormatCodes: []int16{1},
		}, key).Return(nil),
		srv.EXPECT().SendShard(&pgproto3.Describe{ObjectType: 'P'}, key).Return(nil),
		srv.EXPECT().SendShard(&pgproto3.Execute{}, key).Return(nil),
		srv.EXPECT().SendShard(&pgproto3.Sync{}, key).Return(nil),
	)
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.ParseComplete{},
		&pgproto3.BindComplete{},
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{DataTypeOID: customOID, Format: 1}}},
		&pgproto3.DataRow{Values: [][]byte{opaque}},
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
		&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXACT)},
	} {
		srv.EXPECT().ReceiveShard(uint(1)).Return(msg, nil)
	}
	bind := &pgproto3.Bind{
		PreparedStatement: "original", DestinationPortal: "client_portal",
		ParameterFormatCodes: []int16{1}, Parameters: [][]byte{input}, ResultFormatCodes: []int16{1},
	}
	executor := NewQueryStateExecutor(nil, nil, nil, cl).(*QueryStateExecutorImpl)
	executor.es.activeShards = []kr.ShardKey{key}
	require.NoError(t, executor.ExecuteSlice(&QueryDesc{Msg: bind, parameterOIDs: []uint32{23}}, output, true))
	require.Equal(t, "client_portal", bind.DestinationPortal)
	require.Equal(t, "original", bind.PreparedStatement)
	require.Equal(t, int16(1), output.TTS.Desc[0].Format)
	// The type is deliberately unknown to SPQR: the backend's binary payload
	// must pass through exactly, without decoding or conversion.
	require.Equal(t, []pgproto3.BackendMessage{&pgproto3.DataRow{Values: [][]byte{opaque}}}, *replies)
}
