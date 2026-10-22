package tupleslot

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestTupleDescWithResultFormats(t *testing.T) {
	desc := TupleDesc{
		{Name: []byte("number"), DataTypeOID: pgtype.Int4OID, Format: 1},
		{Name: []byte("flag"), DataTypeOID: pgtype.BoolOID},
	}
	for _, tt := range []struct {
		name    string
		formats []int16
		want    []int16
	}{
		{name: "default text", want: []int16{0, 0}},
		{name: "all text", formats: []int16{0}, want: []int16{0, 0}},
		{name: "all binary", formats: []int16{1}, want: []int16{1, 1}},
		{name: "per column", formats: []int16{0, 1}, want: []int16{0, 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, err := desc.WithResultFormats(tt.formats)
			require.NoError(t, err)
			for i, format := range tt.want {
				require.Equal(t, format, result[i].Format)
			}
			require.Equal(t, int16(1), desc[0].Format)
			require.Equal(t, int16(0), desc[1].Format)
			result[0].Name[0] = 'x'
			require.Equal(t, "number", string(desc[0].Name))
		})
	}
	for _, formats := range [][]int16{{0, 1, 0}, {-1}, {2}, {0, 2}} {
		_, err := desc.WithResultFormats(formats)
		require.Error(t, err)
	}
}
