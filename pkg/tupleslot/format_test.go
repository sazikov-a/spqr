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

func TestResolveResultFormats(t *testing.T) {
	for _, tc := range []struct {
		name    string
		formats []int16
		columns int
		want    []int16
	}{
		{name: "default text", columns: 3, want: []int16{0, 0, 0}},
		{name: "one text", formats: []int16{0}, columns: 3, want: []int16{0, 0, 0}},
		{name: "one binary", formats: []int16{1}, columns: 3, want: []int16{1, 1, 1}},
		{name: "per column", formats: []int16{1, 0, 1}, columns: 3, want: []int16{1, 0, 1}},
		{name: "no columns", want: []int16{}},
		{name: "one format without columns", formats: []int16{1}, want: []int16{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			formats, err := ResolveResultFormats(tc.formats, tc.columns)
			require.NoError(t, err)
			require.Equal(t, tc.want, formats)
			if len(formats) > 0 && len(tc.formats) > 0 {
				formats[0] = 2
				require.NotEqual(t, int16(2), tc.formats[0])
			}
		})
	}
	for _, tc := range []struct {
		formats []int16
		columns int
	}{
		{formats: []int16{0, 1}, columns: 3},
		{formats: []int16{0, 1}, columns: 0},
		{formats: []int16{-1}, columns: 1},
		{formats: []int16{2}, columns: 0},
		{formats: []int16{0, 2}, columns: 2},
		{columns: -1},
	} {
		_, err := ResolveResultFormats(tc.formats, tc.columns)
		require.Error(t, err)
	}
	require.NoError(t, ValidateResultFormatCodes([]int16{0, 1, 1, 0}))
	require.Error(t, ValidateResultFormatCodes([]int16{1, 2}))
}

func TestTupleTableSlotWithResultFormats(t *testing.T) {
	for _, tc := range []struct {
		name         string
		oid          uint32
		sourceFormat int16
		value        []byte
		wantFormat   int16
		want         []byte
	}{
		{name: "true", oid: pgtype.BoolOID, value: []byte("true"), wantFormat: 1, want: []byte{1}},
		{name: "false", oid: pgtype.BoolOID, value: []byte("f"), wantFormat: 1, want: []byte{0}},
		{name: "binary boolean", oid: pgtype.BoolOID, sourceFormat: 1, value: []byte{1}, want: []byte("t")},
		{name: "int2", oid: pgtype.Int2OID, value: []byte("-32768"), wantFormat: 1, want: []byte{0x80, 0}},
		{name: "int4", oid: pgtype.Int4OID, value: []byte("-2147483648"), wantFormat: 1, want: []byte{0x80, 0, 0, 0}},
		{name: "int8", oid: pgtype.Int8OID, value: []byte("9223372036854775807"), wantFormat: 1, want: []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{name: "binary int4", oid: pgtype.Int4OID, sourceFormat: 1, value: []byte{0, 0, 0, 42}, want: []byte("42")},
		{name: "binary int8", oid: pgtype.Int8OID, sourceFormat: 1, value: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, want: []byte("-1")},
		{name: "empty text", oid: pgtype.TextOID, value: []byte{}, wantFormat: 1, want: []byte{}},
		{name: "empty binary text", oid: pgtype.TextOID, sourceFormat: 1, value: []byte{}, want: []byte{}},
		{name: "varchar", oid: pgtype.VarcharOID, value: []byte("some text"), wantFormat: 1, want: []byte("some text")},
		{name: "bpchar", oid: pgtype.BPCharOID, value: []byte("padded   "), wantFormat: 1, want: []byte("padded   ")},
		{name: "name", oid: pgtype.NameOID, value: []byte("column"), wantFormat: 1, want: []byte("column")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &TupleTableSlot{Desc: TupleDesc{{Name: []byte("value"), DataTypeOID: tc.oid, Format: tc.sourceFormat}}, Raw: [][][]byte{{tc.value}}}
			result, err := source.WithResultFormats([]int16{tc.wantFormat})
			require.NoError(t, err)
			require.Equal(t, tc.wantFormat, result.Desc[0].Format)
			require.Equal(t, tc.want, result.Raw[0][0])
			require.NotNil(t, result.Raw[0][0])
			require.Equal(t, tc.sourceFormat, source.Desc[0].Format)
			require.Equal(t, tc.value, source.Raw[0][0])
			sourceValue, ok := pgtype.NewMap().TypeForOID(tc.oid)
			require.True(t, ok)
			decoded, decodeErr := sourceValue.Codec.DecodeValue(pgtype.NewMap(), tc.oid, tc.wantFormat, result.Raw[0][0])
			require.NoError(t, decodeErr)
			if tc.oid == pgtype.BoolOID {
				require.Equal(t, tc.name != "false", decoded)
			}
			result.Desc[0].Name[0] = 'x'
			require.Equal(t, "value", string(source.Desc[0].Name))
		})
	}
}

func TestTupleTableSlotMixedResultFormats(t *testing.T) {
	source := &TupleTableSlot{
		Desc: TupleDesc{
			{DataTypeOID: pgtype.Int4OID, Format: 1},
			{DataTypeOID: pgtype.BoolOID},
			{DataTypeOID: pgtype.TextOID},
			{DataTypeOID: 123456},
			{DataTypeOID: 123456, Format: 1},
		},
		Raw: [][][]byte{{{0, 0, 0, 42}, []byte("t"), {}, nil, {0xfe, 0, 0xff}}},
	}
	result, err := source.WithResultFormats([]int16{0, 1, 1, 1, 1})
	require.NoError(t, err)
	require.Equal(t, [][][]byte{{[]byte("42"), {1}, {}, nil, {0xfe, 0, 0xff}}}, result.Raw)
	require.Nil(t, result.Raw[0][3])
	require.NotNil(t, result.Raw[0][2])
	require.Same(t, &source.Raw[0][4][0], &result.Raw[0][4][0], "opaque matching-format values should be forwarded")
	require.Equal(t, [][][]byte{{{0, 0, 0, 42}, []byte("t"), {}, nil, {0xfe, 0, 0xff}}}, source.Raw)
	textResult, err := result.WithResultFormats([]int16{0, 0, 0, 0, 1})
	require.NoError(t, err)
	require.Equal(t, []byte("t"), textResult.Raw[0][1])
}

func TestTupleTableSlotResultFormatErrorsLeaveInputIntact(t *testing.T) {
	source := &TupleTableSlot{
		Desc: TupleDesc{{DataTypeOID: pgtype.Int4OID}, {DataTypeOID: pgtype.BoolOID}},
		Raw:  [][][]byte{{[]byte("42"), []byte("not a boolean")}},
	}
	result, err := source.WithResultFormats([]int16{1})
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, [][][]byte{{[]byte("42"), []byte("not a boolean")}}, source.Raw)
	require.Zero(t, source.Desc[0].Format)
	for _, tc := range []struct {
		name    string
		slot    *TupleTableSlot
		formats []int16
	}{
		{name: "nil slot"},
		{name: "too few values", slot: &TupleTableSlot{Desc: source.Desc, Raw: [][][]byte{{[]byte("42")}}}},
		{name: "too many values", slot: &TupleTableSlot{Desc: source.Desc, Raw: [][][]byte{{[]byte("42"), []byte("t"), nil}}}},
		{name: "invalid source format", slot: &TupleTableSlot{Desc: TupleDesc{{DataTypeOID: pgtype.TextOID, Format: 2}}}},
		{name: "invalid destination format", slot: source, formats: []int16{2}},
		{name: "wrong format count", slot: source, formats: []int16{0, 1, 0}},
		{name: "unknown conversion", slot: &TupleTableSlot{Desc: TupleDesc{{DataTypeOID: 123456}}, Raw: [][][]byte{{[]byte("value")}}}, formats: []int16{1}},
		{name: "unsupported json conversion", slot: &TupleTableSlot{Desc: TupleDesc{{DataTypeOID: pgtype.JSONOID}}, Raw: [][][]byte{{[]byte("9007199254740993")}}}, formats: []int16{1}},
		{name: "malformed int4", slot: &TupleTableSlot{Desc: TupleDesc{{DataTypeOID: pgtype.Int4OID, Format: 1}}, Raw: [][][]byte{{{1}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.slot.WithResultFormats(tc.formats)
			require.Error(t, err)
			require.Nil(t, result)
		})
	}
}

func TestTupleTableSlotMatchingFormatsPassThrough(t *testing.T) {
	for _, format := range []int16{0, 1} {
		source := &TupleTableSlot{Desc: TupleDesc{{DataTypeOID: 123456, Format: format}}, Raw: [][][]byte{{{0xff, 0, 0xfe}}, {nil}, {{}}}}
		result, err := source.WithResultFormats([]int16{format})
		require.NoError(t, err)
		require.Same(t, source, result, "matching formats should reuse the read-only slot")
		require.Equal(t, source.Raw, result.Raw)
		require.Same(t, &source.Raw[0][0][0], &result.Raw[0][0][0])
		require.Nil(t, result.Raw[1][0])
		require.NotNil(t, result.Raw[2][0])
	}
}
