package relay

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/prepstatement"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/session/sessiontest"
	mockcl "github.com/pg-sharding/spqr/router/mock/client"
	mockpm "github.com/pg-sharding/spqr/router/mock/poolmgr"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestLocalShowResultFormats(t *testing.T) {
	for _, tc := range []struct {
		name  string
		param string
		value string
	}{
		{name: "guc", param: session.SPQR_EXECUTE_ON, value: "sh1"},
		{name: "empty guc", param: session.SPQR_EXECUTE_ON},
		{name: "custom parameter", param: "__spqr__custom", value: "value"},
		{name: "empty custom parameter", param: "__spqr__custom"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			cl := mockcl.NewMockRouterClient(ctrl)
			pm := mockpm.NewMockPoolMgr(ctrl)
			query := "SHOW " + tc.param
			cl.EXPECT().CleanupStatementSet().AnyTimes()
			cl.EXPECT().ID().AnyTimes().Return(uint(1))
			cl.EXPECT().PreparedStatementDefinitionByName("s").Return(&prepstatement.PreparedStatementDefinition{Name: "s", Query: query})
			cl.EXPECT().ReplyBindComplete().Return(nil)
			if tc.param == session.SPQR_EXECUTE_ON {
				cl.EXPECT().FindStrGUC(tc.param).AnyTimes().Return(sessiontest.MustFindStrGUC(tc.param), nil)
				cl.EXPECT().ResolveVirtualStringParam(tc.param, gomock.Any()).AnyTimes().Return(tc.value)
			} else {
				cl.EXPECT().Params().AnyTimes().Return(map[string]string{tc.param: tc.value})
			}
			var descriptions []*pgproto3.RowDescription
			var rows []*pgproto3.DataRow
			cl.EXPECT().Send(gomock.Any()).Times(4).DoAndReturn(func(msg pgproto3.BackendMessage) error {
				switch msg := msg.(type) {
				case *pgproto3.RowDescription:
					descriptions = append(descriptions, msg)
				case *pgproto3.DataRow:
					rows = append(rows, msg)
				}
				return nil
			})
			rst := &RelayStateImpl{
				Cl:              cl,
				qse:             NewQueryStateExecutor(nil, nil, pm, cl),
				savedPortalDesc: map[string]*PortalDesc{},
			}
			require.NoError(t, rst.BindPrepared(context.Background(), "s", "", nil, nil, []int16{1}))
			require.Nil(t, rst.localResultFormats)
			_, err := rst.ProcQueryAdvancedTx(query, func() error {
				t.Fatal("local SHOW should not dispatch to a backend")
				return nil
			}, false)
			require.NoError(t, err)
			require.Len(t, descriptions, 2)
			require.Equal(t, int16(1), descriptions[0].Fields[0].Format)
			require.Equal(t, int16(0), descriptions[1].Fields[0].Format)
			require.Len(t, rows, 2)
			for _, row := range rows {
				require.Equal(t, [][]byte{[]byte(tc.value)}, row.Values)
			}
		})
	}
}

func TestReplyVirtualParamStateNullAndEmpty(t *testing.T) {
	for _, value := range [][]byte{nil, {}} {
		ctrl := gomock.NewController(t)
		cl := mockcl.NewMockRouterClient(ctrl)
		cl.EXPECT().Send(gomock.Any()).DoAndReturn(func(msg pgproto3.BackendMessage) error {
			require.Equal(t, int16(1), msg.(*pgproto3.RowDescription).Fields[0].Format)
			return nil
		})
		cl.EXPECT().Send(gomock.Any()).DoAndReturn(func(msg pgproto3.BackendMessage) error {
			require.Equal(t, [][]byte{value}, msg.(*pgproto3.DataRow).Values)
			return nil
		})
		require.NoError(t, ReplyVirtualParamState(cl, "value", value, 1))
	}
}

func TestLocalShowRestoresResultFormatsOnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	cl := mockcl.NewMockRouterClient(ctrl)
	pm := mockpm.NewMockPoolMgr(ctrl)
	cl.EXPECT().CleanupStatementSet().AnyTimes()
	cl.EXPECT().ID().AnyTimes().Return(uint(1))
	cl.EXPECT().PreparedStatementDefinitionByName("s").Return(&prepstatement.PreparedStatementDefinition{Name: "s", Query: "SHOW __spqr__custom"})
	cl.EXPECT().ReplyBindComplete().Return(nil)
	cl.EXPECT().Params().Return(map[string]string{"__spqr__custom": "value"})
	wantErr := errors.New("client disconnected")
	cl.EXPECT().Send(gomock.Any()).Return(wantErr)
	rst := &RelayStateImpl{
		Cl:  cl,
		qse: NewQueryStateExecutor(nil, nil, pm, cl),
	}
	require.ErrorIs(t, rst.BindPrepared(context.Background(), "s", "", nil, nil, []int16{1}), wantErr)
	require.Nil(t, rst.localResultFormats)
}
