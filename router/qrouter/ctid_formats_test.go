package qrouter

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/catalog"
	mocksh "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/models/topology"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/session"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	mocksv "github.com/pg-sharding/spqr/router/mock/server"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestSPQRCTIDSubplanResultFormats(t *testing.T) {
	for _, argument := range []struct {
		name          string
		expression    string
		parameters    [][]byte
		parameterOIDs []uint32
	}{
		{name: "literal", expression: "'tab'"},
		{name: "inferred parameter", expression: "$1", parameters: [][]byte{[]byte("tab")}},
		{name: "typed parameter", expression: "$1", parameters: [][]byte{[]byte("tab")}, parameterOIDs: []uint32{catalog.TEXTOID}},
	} {
		for _, result := range []struct {
			name    string
			formats []int16
		}{
			{name: "default text"},
			{name: "all text", formats: []int16{0}},
			{name: "all binary", formats: []int16{1}},
			{name: "mixed", formats: []int16{1, 0, 1}},
		} {
			t.Run(argument.name+"/"+result.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				srv := mocksv.NewMockServer(ctrl)
				sh := mocksh.NewMockShardHostInstance(ctrl)
				sh.EXPECT().ID().AnyTimes().Return(uint(1))
				sh.EXPECT().Name().AnyTimes().Return("sh1")
				srv.EXPECT().Datashards().Return([]shard.ShardHostInstance{sh})

				params := session.NewSimpleHandler("", false, "")
				params.SetBindParams(argument.parameters)
				params.SetParamFormatCodes([]int16{1})
				stmt, _, err := lyx.Parse("SELECT __spqr__ctid(" + argument.expression + ")")
				require.NoError(t, err)
				qr := &ProxyQrouter{tmgr: topology.TopMgrFromMap(map[string]*topology.DataShard{"sh1": {ID: "sh1"}})}
				p, err := qr.planSPQRCTID(context.Background(), &rmeta.RoutingMetadataContext{Stmt: stmt[0], SPH: params})
				require.NoError(t, err)
				output := p.(*plan.VirtualPlan)
				require.NotNil(t, output.SubplanProjection)
				require.Equal(t, "TABLE tab", output.SubPlan.GetGangMemberMsg(kr.ShardKey{Name: "sh1"}))

				bind := pgproto3.Bind{
					DestinationPortal: "client_portal", PreparedStatement: "original",
					Parameters: argument.parameters, ParameterFormatCodes: []int16{1},
					ResultFormatCodes: result.formats,
				}
				before, err := bind.Encode(nil)
				require.NoError(t, err)
				backendBind, backendOIDs, err := output.SubplanProjection.Bind(bind, argument.parameterOIDs)
				require.NoError(t, err)
				// The rewritten TABLE has no placeholders and two physical columns.
				// Forwarding the relation argument or three output format codes is
				// rejected by PostgreSQL with a protocol violation.
				require.Empty(t, backendBind.Parameters)
				require.Empty(t, backendBind.ParameterFormatCodes)
				require.Empty(t, backendOIDs)
				backendFormats, err := tupleslot.ResolveResultFormats(backendBind.ResultFormatCodes, 2)
				require.NoError(t, err)
				outerFormats, err := tupleslot.ResolveResultFormats(result.formats, 3)
				require.NoError(t, err)
				require.Equal(t, outerFormats[1:], backendFormats)
				after, err := bind.Encode(nil)
				require.NoError(t, err)
				require.Equal(t, before, after, "adapting a subplan must preserve the client Bind")

				const opaqueOID = 12345678
				fields := []pgproto3.FieldDescription{
					{Name: []byte("id"), DataTypeOID: catalog.INT4OID, Format: backendFormats[0]},
					{Name: []byte("opaque"), DataTypeOID: opaqueOID, Format: backendFormats[1]},
				}
				integer := []byte("42")
				if backendFormats[0] == 1 {
					integer = binary.BigEndian.AppendUint32(nil, 42)
				}
				opaque := []byte("opaque text")
				if backendFormats[1] == 1 {
					opaque = []byte{0xfe, 0, 0xff}
				}
				for _, msg := range []pgproto3.BackendMessage{
					&pgproto3.ParseComplete{}, &pgproto3.BindComplete{},
					&pgproto3.RowDescription{Fields: fields},
					&pgproto3.DataRow{Values: [][]byte{integer, opaque}},
					&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
					&pgproto3.ReadyForQuery{TxStatus: 'I'},
				} {
					srv.EXPECT().ReceiveShard(uint(1)).Return(msg, nil)
				}
				require.NoError(t, output.SubPlan.RunSlice(srv))
				require.Equal(t, []int16{0, backendFormats[0], backendFormats[1]}, []int16{
					output.TTS.Desc[0].Format, output.TTS.Desc[1].Format, output.TTS.Desc[2].Format,
				})
				formatted, err := output.TTS.WithResultFormats(result.formats)
				require.NoError(t, err)
				require.Equal(t, outerFormats, []int16{formatted.Desc[0].Format, formatted.Desc[1].Format, formatted.Desc[2].Format})
				require.Equal(t, [][][]byte{{[]byte("shard sh1"), integer, opaque}}, formatted.Raw,
					"unknown backend values must pass through while the synthetic text column changes format")
			})
		}
	}
}
