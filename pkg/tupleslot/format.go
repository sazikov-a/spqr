package tupleslot

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5/pgtype"
)

// ValidateResultFormatCodes checks codes independently of the result shape.
func ValidateResultFormatCodes(formats []int16) error {
	for _, format := range formats {
		if format != pgtype.TextFormatCode && format != pgtype.BinaryFormatCode {
			return fmt.Errorf("unsupported result format code %d", format)
		}
	}
	return nil
}

// ResolveResultFormats expands PostgreSQL's zero, one, or per-column codes.
func ResolveResultFormats(formats []int16, columnCount int) ([]int16, error) {
	if err := ValidateResultFormatCodes(formats); err != nil {
		return nil, err
	}
	if columnCount < 0 {
		return nil, fmt.Errorf("invalid result column count %d", columnCount)
	}
	if len(formats) > 1 && len(formats) != columnCount {
		return nil, fmt.Errorf("bind message has %d result formats but query has %d columns", len(formats), columnCount)
	}
	resolved := make([]int16, columnCount)
	for i := range resolved {
		switch len(formats) {
		case 0:
		case 1:
			resolved[i] = formats[0]
		default:
			resolved[i] = formats[i]
		}
	}
	return resolved, nil
}

// WithResultFormats returns cloned metadata with the formats requested by Bind.
func (td TupleDesc) WithResultFormats(formats []int16) (TupleDesc, error) {
	resolved, err := ResolveResultFormats(formats, len(td))
	if err != nil {
		return nil, err
	}
	desc := cloneDesc(td)
	for i := range desc {
		desc[i].Format = resolved[i]
	}
	return desc, nil
}

// WithResultFormats converts row values and metadata together without changing
// the slot. Unchanged slots and values are shared and must be treated as read-only.
func (tts *TupleTableSlot) WithResultFormats(formats []int16) (*TupleTableSlot, error) {
	if tts == nil {
		return nil, fmt.Errorf("cannot format a nil tuple slot")
	}
	resolved, err := ResolveResultFormats(formats, len(tts.Desc))
	if err != nil {
		return nil, err
	}
	formatsChanged := false
	for i, field := range tts.Desc {
		if err := ValidateResultFormatCodes([]int16{field.Format}); err != nil {
			return nil, fmt.Errorf("column %d source format: %w", i, err)
		}
		formatsChanged = formatsChanged || field.Format != resolved[i]
	}
	for rowIndex, row := range tts.Raw {
		if len(row) != len(tts.Desc) {
			return nil, fmt.Errorf("row %d has %d values but result has %d columns", rowIndex, len(row), len(tts.Desc))
		}
	}
	if !formatsChanged {
		return tts, nil
	}
	desc := cloneDesc(tts.Desc)
	for i := range desc {
		desc[i].Format = resolved[i]
	}
	result := &TupleTableSlot{Desc: desc, Raw: tts.Raw}
	var typeMap *pgtype.Map
	rowsCopied := false
	for rowIndex, row := range tts.Raw {
		var convertedRow [][]byte
		for columnIndex, value := range row {
			field := tts.Desc[columnIndex]
			if value == nil || field.Format == desc[columnIndex].Format {
				continue
			}
			if typeMap == nil {
				typeMap = pgtype.NewMap()
			}
			converted, err := convertResultValue(typeMap, field.DataTypeOID, field.Format, desc[columnIndex].Format, value)
			if err != nil {
				return nil, fmt.Errorf("row %d column %d: %w", rowIndex, columnIndex, err)
			}
			if !rowsCopied {
				result.Raw = slices.Clone(tts.Raw)
				rowsCopied = true
			}
			if convertedRow == nil {
				convertedRow = slices.Clone(row)
				result.Raw[rowIndex] = convertedRow
			}
			convertedRow[columnIndex] = converted
		}
	}
	return result, nil
}

func convertResultValue(typeMap *pgtype.Map, oid uint32, sourceFormat, resultFormat int16, value []byte) ([]byte, error) {
	// Router-produced values use these scalar types. Other codecs may decode
	// into a lossy representation, so leave their conversion to PostgreSQL.
	switch oid {
	case pgtype.BoolOID, pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID,
		pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID, pgtype.NameOID:
	default:
		return nil, fmt.Errorf("cannot convert result type OID %d from format %d to %d", oid, sourceFormat, resultFormat)
	}
	dataType, ok := typeMap.TypeForOID(oid)
	if !ok {
		return nil, fmt.Errorf("cannot convert result type OID %d from format %d to %d", oid, sourceFormat, resultFormat)
	}
	if !dataType.Codec.FormatSupported(sourceFormat) || !dataType.Codec.FormatSupported(resultFormat) {
		return nil, fmt.Errorf("result type %s does not support conversion from format %d to %d", dataType.Name, sourceFormat, resultFormat)
	}
	decoded, err := dataType.Codec.DecodeValue(typeMap, oid, sourceFormat, value)
	if err != nil {
		return nil, fmt.Errorf("decode result type %s: %w", dataType.Name, err)
	}
	return typeMap.Encode(oid, resultFormat, decoded, make([]byte, 0))
}

func cloneDesc(desc TupleDesc) TupleDesc {
	if desc == nil {
		return nil
	}
	clone := make(TupleDesc, len(desc))
	copy(clone, desc)
	for i := range clone {
		clone[i].Name = bytes.Clone(clone[i].Name)
	}
	return clone
}
