package engine

import (
	"encoding/binary"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/stretchr/testify/require"
)

func TestCountAggregateFormats(t *testing.T) {
	for _, tt := range []struct {
		name   string
		oid    int
		format int16
		values [][]byte
	}{
		{name: "text int4", oid: catalog.INT4OID, values: [][]byte{[]byte("17"), []byte("25")}},
		{name: "text int8", oid: catalog.INT8OID, values: [][]byte{[]byte("17"), []byte("25")}},
		{name: "binary int4", oid: catalog.INT4OID, format: pgtype.BinaryFormatCode, values: [][]byte{{0, 0, 0, 17}, {0, 0, 0, 25}}},
		{name: "binary int8", oid: catalog.INT8OID, format: pgtype.BinaryFormatCode, values: [][]byte{binary.BigEndian.AppendUint64(nil, 17), binary.BigEndian.AppendUint64(nil, 25)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			agg := &CountAggregate{}
			agg.Init(tt.oid, tt.format)
			for _, value := range tt.values {
				require.NoError(t, agg.Aggregate(value))
			}
			require.Equal(t, []byte("42"), agg.Finalize(pgtype.TextFormatCode))
			require.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 42}, agg.Finalize(pgtype.BinaryFormatCode))
		})
	}
}

func TestCountAggregateInvalidInput(t *testing.T) {
	for _, tt := range []struct {
		name   string
		oid    int
		format int16
		value  []byte
	}{
		{name: "invalid text", oid: catalog.INT8OID, value: []byte("invalid")},
		{name: "truncated binary int4", oid: catalog.INT4OID, format: pgtype.BinaryFormatCode, value: []byte{42}},
		{name: "truncated binary int8", oid: catalog.INT8OID, format: pgtype.BinaryFormatCode, value: []byte{0, 0, 0, 42}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			agg := &CountAggregate{}
			agg.Init(tt.oid, tt.format)
			require.Error(t, agg.Aggregate(tt.value))
		})
	}
}
