package relay

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/session/sessiontest"
	mockcl "github.com/pg-sharding/spqr/router/mock/client"
	mocksv "github.com/pg-sharding/spqr/router/mock/server"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestDispatchExtendedQueryFormats(t *testing.T) {
	for _, tt := range []struct {
		name       string
		rewritten  bool
		describe   bool
		portal     string
		parameters [][]byte
		formats    []int16
		maxRows    uint32
	}{
		{name: "original binary", formats: []int16{1}},
		{name: "rewritten binary", rewritten: true, formats: []int16{1}},
		{
			name: "rewritten parameters and mixed results", rewritten: true,
			portal: "client_portal", parameters: [][]byte{{0, 0, 0, 42}},
			formats: []int16{1, 0}, maxRows: 4,
		},
		{name: "inner result description", rewritten: true, describe: true, formats: []int16{1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			srv := mocksv.NewMockServer(ctrl)
			cl := mockcl.NewMockRouterClient(ctrl)
			cl.EXPECT().Server().Return(srv)
			cl.EXPECT().FindBoolGUC(session.SPQR_REPLY_NOTICE).
				Return(sessiontest.MustFindBoolGUC(session.SPQR_REPLY_NOTICE), nil)
			cl.EXPECT().ResolveVirtualBoolParam(session.SPQR_REPLY_NOTICE, gomock.Any()).Return(false)

			keys := []kr.ShardKey{{Name: "sh1"}, {Name: "sh2"}}
			p := &plan.ScatterPlan{ExecTargets: keys}
			if tt.rewritten {
				p.OverwriteQuery = map[string]string{
					"sh1": "SELECT 42, 'first'",
					"sh2": "SELECT 42, 'second'",
				}
				if len(tt.parameters) != 0 {
					p.OverwriteQuery["sh1"] = "SELECT $1::int4, 'first'"
					p.OverwriteQuery["sh2"] = "SELECT $1::int4, 'second'"
				}
			}
			var parameterOIDs []uint32
			var parameterFormats []int16
			if len(tt.parameters) != 0 {
				parameterOIDs = []uint32{23}
				parameterFormats = []int16{1}
			}
			bind := &pgproto3.Bind{
				DestinationPortal: tt.portal, PreparedStatement: "original_cached_statement",
				ParameterFormatCodes: parameterFormats, Parameters: tt.parameters,
				ResultFormatCodes: tt.formats,
			}
			exec := &pgproto3.Execute{Portal: tt.portal, MaxRows: tt.maxRows}
			qd := &QueryDesc{
				Msg: bind, exec: exec, parameterOIDs: parameterOIDs, describePortal: tt.describe,
			}

			var calls []any
			for _, key := range keys {
				backendBind := *bind
				if tt.rewritten {
					calls = append(calls, srv.EXPECT().SendShard(&pgproto3.Parse{
						Query: p.OverwriteQuery[key.Name], ParameterOIDs: parameterOIDs,
					}, key).Return(nil))
					backendBind.PreparedStatement = ""
				}
				calls = append(calls, srv.EXPECT().SendShard(&backendBind, key).Return(nil))
				if tt.describe {
					calls = append(calls, srv.EXPECT().SendShard(&pgproto3.Describe{
						ObjectType: 'P', Name: tt.portal,
					}, key).Return(nil))
				}
				calls = append(calls, srv.EXPECT().SendShard(exec, key).Return(nil))
				if tt.maxRows != 0 {
					calls = append(calls, cl.EXPECT().Peek().Return(&pgproto3.Sync{}, nil))
				}
				calls = append(calls, srv.EXPECT().SendShard(&pgproto3.Sync{}, key).Return(nil))
			}
			gomock.InOrder(calls...)

			require.NoError(t, DispatchSlice(qd, p, cl, false))
			require.Equal(t, "original_cached_statement", bind.PreparedStatement)
			require.Equal(t, tt.portal, bind.DestinationPortal)
			require.Equal(t, tt.parameters, bind.Parameters)
			require.Equal(t, parameterFormats, bind.ParameterFormatCodes)
			require.Equal(t, tt.formats, bind.ResultFormatCodes)
			require.Equal(t, tt.rewritten, qd.parseSent)
		})
	}
}
