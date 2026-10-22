package qrouter

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/catalog"
	mocksh "github.com/pg-sharding/spqr/pkg/mock/shard"
	"github.com/pg-sharding/spqr/pkg/models/kr"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/shard"
	"github.com/pg-sharding/spqr/pkg/txstatus"
	mocksv "github.com/pg-sharding/spqr/router/mock/server"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/xproto"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestAggregatePostprocessingPreservesBackendFormats(t *testing.T) {
	for _, tt := range []struct {
		name    string
		formats []int16
	}{
		{name: "text", formats: []int16{0, 0}},
		{name: "binary", formats: []int16{1, 1}},
		{name: "mixed", formats: []int16{1, 0}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			srv := mocksv.NewMockServer(ctrl)
			keys := []kr.ShardKey{{Name: "sh1"}, {Name: "sh2"}}
			shards := make([]shard.ShardHostInstance, len(keys))
			for i, key := range keys {
				sh := mocksh.NewMockShardHostInstance(ctrl)
				sh.EXPECT().Name().Return(key.Name).AnyTimes()
				sh.EXPECT().ID().Return(uint(i)).AnyTimes()
				shards[i] = sh
			}
			srv.EXPECT().Datashards().Return(shards)

			stmt, _, err := lyx.Parse("SELECT count(*), count(v) FROM tab")
			require.NoError(t, err)
			rm := &rmeta.RoutingMetadataContext{Stmt: stmt[0]}
			qr := &ProxyQrouter{}
			p, err := qr.addAggregateToPlan(context.Background(), rm, &plan.ScatterPlan{ExecTargets: keys})
			require.NoError(t, err)
			vp := p.(*plan.VirtualPlan)

			fields := make([]pgproto3.FieldDescription, len(tt.formats))
			want := make([][]byte, len(tt.formats))
			for i, format := range tt.formats {
				fields[i] = pgproto3.FieldDescription{DataTypeOID: catalog.INT8OID, Format: format}
				if format == xproto.FormatCodeBinary {
					want[i] = binary.BigEndian.AppendUint64(nil, 42)
				} else {
					want[i] = []byte("42")
				}
			}
			for i, count := range []uint64{17, 25} {
				values := make([][]byte, len(tt.formats))
				for j, format := range tt.formats {
					if format == xproto.FormatCodeBinary {
						values[j] = binary.BigEndian.AppendUint64(nil, count)
					} else {
						values[j] = fmt.Appendf(nil, "%d", count)
					}
				}
				for _, msg := range []pgproto3.BackendMessage{
					&pgproto3.ParseComplete{},
					&pgproto3.BindComplete{},
					&pgproto3.RowDescription{Fields: fields},
					&pgproto3.DataRow{Values: values},
					&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")},
					&pgproto3.ReadyForQuery{TxStatus: byte(txstatus.TXIDLE)},
				} {
					srv.EXPECT().ReceiveShard(uint(i)).Return(msg, nil)
				}
			}
			require.NoError(t, vp.SubPlan.(*plan.ScatterPlan).RunF(srv))
			require.Equal(t, fields, []pgproto3.FieldDescription(vp.TTS.Desc))
			require.Equal(t, [][][]byte{want}, vp.TTS.Raw)
		})
	}
}
