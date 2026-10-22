package relay

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/config"
	mocksh "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/planopts"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/session/sessiontest"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	mockcl "github.com/pg-sharding/spqr/router/mock/client"
	mockpm "github.com/pg-sharding/spqr/router/mock/poolmgr"
	mockqr "github.com/pg-sharding/spqr/router/mock/qrouter"
	mocksv "github.com/pg-sharding/spqr/router/mock/server"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestDescribePreparedRebindResultFormats(t *testing.T) {
	ctrl := gomock.NewController(t)
	cl := mockcl.NewMockRouterClient(ctrl)
	pm := mockpm.NewMockPoolMgr(ctrl)
	qr := mockqr.NewMockQueryRouter(ctrl)
	sv := mocksv.NewMockServer(ctrl)
	sh := mocksh.NewMockShardHostInstance(ctrl)
	key := kr.ShardKey{Name: "sh1"}
	def := &prepstatement.PreparedStatementDefinition{Name: "s", Query: "select i from t"}
	desc := &pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("i"), DataTypeOID: 23, DataTypeSize: 4, TypeModifier: -1, Format: 0}}}
	var replies []pgproto3.BackendMessage
	var currentBind *pgproto3.Bind
	cl.EXPECT().CleanupStatementSet().AnyTimes()
	cl.EXPECT().ID().AnyTimes().Return(uint(1))
	cl.EXPECT().Usr().AnyTimes().Return("u")
	cl.EXPECT().DB().AnyTimes().Return("d")
	cl.EXPECT().Rule().AnyTimes().Return(&config.FrontendRule{})
	cl.EXPECT().Server().AnyTimes().Return(sv)
	cl.EXPECT().PreparedStatementDefinitionByName("s").AnyTimes().Return(def)
	cl.EXPECT().PreparedStatementQueryHashByName("s").AnyTimes().Return(uint64(123))
	cl.EXPECT().StorePreparedStatement(gomock.Any()).AnyTimes()
	cl.EXPECT().ReplyBindComplete().AnyTimes().Return(nil)
	cl.EXPECT().SetBindParams(gomock.Any()).AnyTimes()
	cl.EXPECT().SetParamFormatCodes(gomock.Any()).AnyTimes()
	cl.EXPECT().EnhancedMultiShardProcessing().AnyTimes().Return(false)
	cl.EXPECT().FindStrGUC(session.SPQR_EXECUTE_ON).AnyTimes().Return(sessiontest.MustFindStrGUC(session.SPQR_EXECUTE_ON), nil)
	cl.EXPECT().ResolveVirtualStringParam(session.SPQR_EXECUTE_ON, gomock.Any()).AnyTimes().Return("")
	cl.EXPECT().FindBoolGUC(session.SPQR_REPLY_NOTICE).AnyTimes().Return(sessiontest.MustFindBoolGUC(session.SPQR_REPLY_NOTICE), nil)
	cl.EXPECT().ResolveVirtualBoolParam(session.SPQR_REPLY_NOTICE, gomock.Any()).AnyTimes().Return(false)
	cl.EXPECT().Send(gomock.Any()).AnyTimes().DoAndReturn(func(m pgproto3.BackendMessage) error { replies = append(replies, m); return nil })
	pm.EXPECT().ValidateGangChange(gomock.Any()).AnyTimes().Return(false)
	qr.EXPECT().AnalyzeQuery(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&rmeta.RoutingMetadataContext{}, nil)
	sv.EXPECT().Datashards().AnyTimes().Return([]shard.ShardHostInstance{sh})
	sv.EXPECT().Name().AnyTimes().Return("server")
	sv.EXPECT().SendShard(gomock.Any(), key).AnyTimes().DoAndReturn(func(m pgproto3.FrontendMessage, _ kr.ShardKey) error {
		if b, ok := m.(*pgproto3.Bind); ok {
			currentBind = b
		}
		return nil
	})
	sh.EXPECT().ID().AnyTimes().Return(uint(1))
	sh.EXPECT().SHKey().AnyTimes().Return(key)
	sh.EXPECT().HasPrepareStatement(uint64(123), uint(1)).AnyTimes().Return(true, &prepstatement.PreparedStatementDescriptor{RowDesc: desc})
	// The first portal Describe obtains text metadata from PostgreSQL once.
	for _, m := range []pgproto3.BackendMessage{&pgproto3.BindComplete{}, desc, &pgproto3.CloseComplete{}, &pgproto3.ReadyForQuery{TxStatus: 'I'}} {
		sv.EXPECT().ReceiveShard(uint(1)).Return(m, nil)
	}
	// Two executions send backend row payloads according to the forwarded Bind.
	recvIndex := 0
	sv.EXPECT().Receive(gomock.Any()).Times(8).DoAndReturn(func(_ *planopts.PlanOpts) (pgproto3.BackendMessage, uint, error) {
		i := recvIndex % 4
		recvIndex++
		switch i {
		case 0:
			return &pgproto3.BindComplete{}, 0, nil
		case 1:
			value := []byte("42")
			if len(currentBind.ResultFormatCodes) == 1 && currentBind.ResultFormatCodes[0] == 1 {
				value = []byte{0, 0, 0, 42}
			}
			return &pgproto3.DataRow{Values: [][]byte{value}}, 0, nil
		case 2:
			return &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}, 0, nil
		default:
			return &pgproto3.ReadyForQuery{TxStatus: 'I'}, 0, nil
		}
	})
	rst := &RelayStateImpl{
		Cl: cl, Qr: qr, poolMgr: pm,
		qse:                 NewQueryStateExecutor(nil, nil, pm, cl),
		saveBindNamed:       map[string]*pgproto3.Bind{},
		lastBindNameMP:      map[string]string{},
		executeMp:           map[string]func(uint32) error{},
		bindQueryPlanMP:     map[string]plan.Plan{},
		savedPortalDesc:     map[string]*PortalDesc{},
		parseCache:          map[string]ParseCacheEntry{},
		savedRM:             map[string]*rmeta.RoutingMetadataContext{},
		routingDecisionPlan: &plan.ShardDispatchPlan{ExecTarget: key},
	}
	_, err := rst.relayParsePrepared(context.Background(), "s", def.Query, nil)
	require.NoError(t, err)
	for _, format := range []int16{0, 1} {
		require.NoError(t, rst.BindPrepared(context.Background(), "s", "", nil, nil, []int16{format}))
		require.NoError(t, rst.DescribePrepared('P', "", nil))
		require.NoError(t, rst.ExecutePortal("", 0))
		rst.qse.SetTxStatus(txstatus.TXIDLE)
		rst.PipelineCleanup()
	}
	var formats []int16
	var rows [][]byte
	for _, m := range replies {
		switch v := m.(type) {
		case *pgproto3.RowDescription:
			formats = append(formats, v.Fields[0].Format)
		case *pgproto3.DataRow:
			rows = append(rows, v.Values[0])
		}
	}
	require.Equal(t, []int16{0, 1}, formats)
	require.Equal(t, [][]byte{[]byte("42"), {0, 0, 0, 42}}, rows)
	require.Equal(t, int16(0), rst.savedPortalDesc["s"].rd.Fields[0].Format, "describing a binary portal must not mutate cached metadata")
}

func TestDescribePreparedNamedPortalResultFormats(t *testing.T) {
	ctrl := gomock.NewController(t)
	cl := mockcl.NewMockRouterClient(ctrl)
	pm := mockpm.NewMockPoolMgr(ctrl)
	cl.EXPECT().CleanupStatementSet().AnyTimes()
	cl.EXPECT().ID().AnyTimes().Return(uint(1))

	cached := &PortalDesc{rd: &pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
		{Name: []byte("i"), DataTypeOID: 23, Format: 1},
		{Name: []byte("s"), DataTypeOID: 25, Format: 1},
	}}}
	rst := &RelayStateImpl{
		Cl:           cl,
		qse:          NewQueryStateExecutor(nil, nil, pm, cl),
		lastBindName: "unrelated_statement",
		savedPortalDesc: map[string]*PortalDesc{
			"s":                   cached,
			"unrelated_statement": {rd: &pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("wrong")}}}},
		},
		lastBindNameMP: map[string]string{},
		saveBindNamed:  map[string]*pgproto3.Bind{},
		executeMp:      map[string]func(uint32) error{},
	}
	for _, tc := range []struct {
		name    string
		formats []int16
		want    []int16
	}{
		{name: "text", want: []int16{0, 0}},
		{name: "binary", formats: []int16{1}, want: []int16{1, 1}},
		{name: "mixed", formats: []int16{1, 0}, want: []int16{1, 0}},
	} {
		rst.lastBindNameMP[tc.name] = "s"
		rst.saveBindNamed[tc.name] = &pgproto3.Bind{DestinationPortal: tc.name, ResultFormatCodes: tc.formats}
		rst.executeMp[tc.name] = emptyExecFunc
		cl.EXPECT().Send(gomock.Any()).DoAndReturn(func(msg pgproto3.BackendMessage) error {
			rd, ok := msg.(*pgproto3.RowDescription)
			require.True(t, ok)
			require.Len(t, rd.Fields, 2)
			require.Equal(t, []byte("i"), rd.Fields[0].Name)
			require.Equal(t, tc.want, []int16{rd.Fields[0].Format, rd.Fields[1].Format})
			rd.Fields[0].Name[0] = 'x'
			return nil
		})
		require.NoError(t, rst.DescribePrepared('P', tc.name, nil))
	}
	require.Equal(t, []byte("i"), cached.rd.Fields[0].Name)
	require.Equal(t, int16(1), cached.rd.Fields[0].Format)
	require.Equal(t, int16(1), cached.rd.Fields[1].Format)
}

func TestDescribePreparedMissingNamedPortalWithCachedDescription(t *testing.T) {
	ctrl := gomock.NewController(t)
	cl := mockcl.NewMockRouterClient(ctrl)
	pm := mockpm.NewMockPoolMgr(ctrl)
	cl.EXPECT().CleanupStatementSet().AnyTimes()
	rst := &RelayStateImpl{
		Cl:              cl,
		qse:             NewQueryStateExecutor(nil, nil, pm, cl),
		savedPortalDesc: map[string]*PortalDesc{"": {rd: &pgproto3.RowDescription{}}},
	}
	err := rst.DescribePrepared('P', "missing", nil)
	var pgErr *spqrerror.SpqrError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, spqrerror.PG_PORTAL_DOES_NOT_EXISTS, pgErr.ErrorCode)
}

func TestDescribePreparedVirtualResultFormats(t *testing.T) {
	ctrl := gomock.NewController(t)
	cl := mockcl.NewMockRouterClient(ctrl)
	pm := mockpm.NewMockPoolMgr(ctrl)
	cl.EXPECT().CleanupStatementSet().AnyTimes()
	cl.EXPECT().ID().AnyTimes().Return(uint(1))
	desc := tupleslot.TupleDesc{{Name: []byte("i"), DataTypeOID: 23}}
	rst := &RelayStateImpl{
		Cl:                  cl,
		qse:                 NewQueryStateExecutor(nil, nil, pm, cl),
		unnamedPortalExists: true,
		saveBind:            pgproto3.Bind{ResultFormatCodes: []int16{1}},
		bindQueryPlan:       &plan.VirtualPlan{TTS: &tupleslot.TupleTableSlot{Desc: desc}},
	}
	cl.EXPECT().Send(gomock.Any()).DoAndReturn(func(msg pgproto3.BackendMessage) error {
		require.Equal(t, int16(1), msg.(*pgproto3.RowDescription).Fields[0].Format)
		return nil
	})
	require.NoError(t, rst.DescribePrepared('P', "", nil))
	require.Equal(t, int16(0), desc[0].Format)
}
