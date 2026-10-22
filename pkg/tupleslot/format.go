package tupleslot

import (
	"bytes"
	"fmt"
)

// WithResultFormats returns a descriptor with the formats requested by Bind.
// PostgreSQL accepts no format codes (all text), one code (all columns), or
// one code per result column.
func (td TupleDesc) WithResultFormats(formats []int16) (TupleDesc, error) {
	if len(formats) > 1 && len(formats) != len(td) {
		return nil, fmt.Errorf("bind message has %d result formats but query has %d columns", len(formats), len(td))
	}
	for _, format := range formats {
		if format != 0 && format != 1 {
			return nil, fmt.Errorf("unsupported result format code %d", format)
		}
	}

	desc := cloneDesc(td)
	for i := range desc {
		switch len(formats) {
		case 0:
			desc[i].Format = 0
		case 1:
			desc[i].Format = formats[0]
		default:
			desc[i].Format = formats[i]
		}
	}
	return desc, nil
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
