package relay

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/catalog"
	mocksh "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	mockpm "github.com/pg-sharding/spqr/router/mock/poolmgr"
	mocksv "github.com/pg-sharding/spqr/router/mock/server"
	"github.com/pg-sharding/spqr/router/server"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestExecuteOutputSubqueryProjectsConsumedParametersAndFormats(t *testing.T) {
	ctrl := gomock.NewController(t)
	srv := mocksv.NewMockServer(ctrl)
	cl, replies := resultFormatClient(t, ctrl, srv)
	key := kr.ShardKey{Name: "sh1"}
	const opaqueOID = 12345678
	opaque := []byte{0xfe, 0, 0xff}
	bind := &pgproto3.Bind{
		PreparedStatement: "original", DestinationPortal: "client_portal",
		Parameters: [][]byte{[]byte("tab")}, ParameterFormatCodes: []int16{1},
		ResultFormatCodes: []int16{0, 1},
	}
	before, err := bind.Encode(nil)
	require.NoError(t, err)
	output := &plan.VirtualPlan{
		TTS: &tupleslot.TupleTableSlot{},
		SubplanProjection: &plan.SubplanProjection{
			ConsumeParameters: true,
			PrefixFields:      tupleslot.TupleDesc{{DataTypeOID: catalog.TEXTOID}},
		},
	}
	subplan := &plan.ScatterPlan{
		ExecTargets:    []kr.ShardKey{key},
		OverwriteQuery: map[string]string{key.Name: "TABLE tab"},
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
				output.TTS.Desc = append(tupleslot.TupleDesc{{DataTypeOID: catalog.TEXTOID}}, msg.Fields...)
			case *pgproto3.DataRow:
				output.TTS.Raw = append(output.TTS.Raw, append([][]byte{[]byte("shard sh1")}, msg.Values...))
			case *pgproto3.ReadyForQuery:
				return nil
			}
		}
	}
	srv.EXPECT().Datashards().AnyTimes()
	srv.EXPECT().Name().AnyTimes().Return("server")
	srv.EXPECT().TxStatus().AnyTimes().Return(txstatus.TXACT)
	gomock.InOrder(
		srv.EXPECT().SendShard(&pgproto3.Parse{Query: "TABLE tab"}, key).Return(nil),
		// TABLE takes no parameters and returns one physical column, although
		// the outer request has a relation parameter and two output columns.
		srv.EXPECT().SendShard(&pgproto3.Bind{ResultFormatCodes: []int16{1}}, key).Return(nil),
		srv.EXPECT().SendShard(&pgproto3.Describe{ObjectType: 'P'}, key).Return(nil),
		srv.EXPECT().SendShard(&pgproto3.Execute{}, key).Return(nil),
		srv.EXPECT().SendShard(&pgproto3.Sync{}, key).Return(nil),
	)
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.ParseComplete{},
		&pgproto3.BindComplete{},
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{DataTypeOID: opaqueOID, Format: 1}}},
		&pgproto3.DataRow{Values: [][]byte{opaque}},
		&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
		&pgproto3.ReadyForQuery{TxStatus: 'T'},
	} {
		srv.EXPECT().ReceiveShard(uint(1)).Return(msg, nil)
	}
	executor := NewQueryStateExecutor(nil, nil, nil, cl).(*QueryStateExecutorImpl)
	executor.es.activeShards = []kr.ShardKey{key}
	require.NoError(t, executor.ExecuteSlice(&QueryDesc{Msg: bind, parameterOIDs: []uint32{catalog.TEXTOID}}, output, true))
	after, err := bind.Encode(nil)
	require.NoError(t, err)
	require.Equal(t, before, after, "subplan dispatch must not change the client's Bind")
	require.Equal(t, []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{[]byte("shard sh1"), opaque}},
	}, *replies, "unknown binary values must survive the subplan's output projection")
}

func TestDescribePreparedOutputSubqueryProjectsRequestAndFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	srv := mocksv.NewMockServer(ctrl)
	cl, replies := resultFormatClient(t, ctrl, srv)
	pm := mockpm.NewMockPoolMgr(ctrl)
	sh := mocksh.NewMockShardHostInstance(ctrl)
	previousShard := mocksh.NewMockShardHostInstance(ctrl)
	key := kr.ShardKey{Name: "sh1"}
	cl.EXPECT().Usr().Return("u")
	cl.EXPECT().DB().Return("d")
	cl.EXPECT().PreparedStatementDefinitionByName("s").Return(&prepstatement.PreparedStatementDefinition{
		Name: "s", Query: "SELECT __spqr__ctid($1)", ParameterOIDs: []uint32{catalog.TEXTOID},
	})
	pm.EXPECT().ValidateGangChange(gomock.Any()).Return(false)
	// An expanded transaction can retain a shard outside this subplan.
	previousShard.EXPECT().SHKey().AnyTimes().Return(kr.ShardKey{Name: "previous"})
	srv.EXPECT().Datashards().AnyTimes().Return([]shard.ShardHostInstance{previousShard, sh})
	sh.EXPECT().ID().Return(uint(1))
	sh.EXPECT().SHKey().AnyTimes().Return(key)
	bind := &pgproto3.Bind{
		PreparedStatement: "original", DestinationPortal: "client_portal",
		Parameters: [][]byte{[]byte("tab")}, ParameterFormatCodes: []int16{1},
		ResultFormatCodes: []int16{1, 0, 1},
	}
	before, err := bind.Encode(nil)
	require.NoError(t, err)
	output := &plan.VirtualPlan{
		TTS: &tupleslot.TupleTableSlot{},
		SubPlan: &plan.ScatterPlan{
			ExecTargets:    []kr.ShardKey{key},
			OverwriteQuery: map[string]string{key.Name: "TABLE tab"},
		},
		SubplanProjection: &plan.SubplanProjection{
			ConsumeParameters: true,
			PrefixFields:      tupleslot.TupleDesc{{Name: []byte("__spqr__ctid"), DataTypeOID: catalog.TEXTOID}},
		},
	}
	// A different relation parameter can change the result shape of the same
	// prepared statement, so an older portal's description cannot be reused.
	stale := &PortalDesc{rd: &pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("other_relation")}}}}
	rst := &RelayStateImpl{
		Cl: cl, poolMgr: pm,
		qse:             NewQueryStateExecutor(nil, nil, pm, cl),
		lastBindNameMP:  map[string]string{"client_portal": "s"},
		saveBindNamed:   map[string]*pgproto3.Bind{"client_portal": bind},
		bindQueryPlanMP: map[string]plan.Plan{"client_portal": output},
		executeMp:       map[string]func(uint32) error{"client_portal": emptyExecFunc},
		savedPortalDesc: map[string]*PortalDesc{"s": stale},
	}
	gomock.InOrder(
		srv.EXPECT().SendShard(&pgproto3.Parse{Query: "TABLE tab"}, key).Return(nil),
		srv.EXPECT().SendShard(&pgproto3.Bind{ResultFormatCodes: []int16{0, 1}}, key).Return(nil),
		srv.EXPECT().SendShard(&pgproto3.Describe{ObjectType: 'P'}, key).Return(nil),
		srv.EXPECT().SendShard(&pgproto3.Close{ObjectType: 'P'}, key).Return(nil),
		srv.EXPECT().SendShard(&pgproto3.Sync{}, key).Return(nil),
	)
	const opaqueOID = 12345678
	for _, msg := range []pgproto3.BackendMessage{
		&pgproto3.ParseComplete{},
		&pgproto3.BindComplete{},
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
			{Name: []byte("id"), DataTypeOID: catalog.INT4OID, Format: 0},
			{Name: []byte("opaque"), DataTypeOID: opaqueOID, Format: 1},
		}},
		&pgproto3.CloseComplete{},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	} {
		srv.EXPECT().ReceiveShard(uint(1)).Return(msg, nil)
	}
	require.NoError(t, rst.DescribePrepared('P', "client_portal", &pgproto3.Describe{ObjectType: 'P', Name: "client_portal"}))
	require.Equal(t, []pgproto3.BackendMessage{&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
		{Name: []byte("__spqr__ctid"), DataTypeOID: catalog.TEXTOID, Format: 1},
		{Name: []byte("id"), DataTypeOID: catalog.INT4OID, Format: 0},
		{Name: []byte("opaque"), DataTypeOID: opaqueOID, Format: 1},
	}}}, *replies)
	after, err := bind.Encode(nil)
	require.NoError(t, err)
	require.Equal(t, before, after, "describing the backend projection must preserve the client Bind")
	require.Same(t, stale, rst.savedPortalDesc["s"], "relation-specific metadata must not replace the statement cache")
}
