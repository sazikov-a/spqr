package planner

import (
	"encoding/binary"
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/xproto"
)

type virtualResultBuilder struct {
	rm     *rmeta.RoutingMetadataContext
	fields []pgproto3.FieldDescription
	values [][]byte
}

func (b *virtualResultBuilder) appendColumn(name string, oid uint32, size int16) int16 {
	format := b.rm.GetResultFormatCode(len(b.fields))
	b.fields = append(b.fields, pgproto3.FieldDescription{
		Name:         []byte(name),
		DataTypeOID:  oid,
		DataTypeSize: size,
		TypeModifier: -1,
		Format:       format,
	})
	return format
}

func (b *virtualResultBuilder) appendBool(name string, value bool) {
	format := b.appendColumn(name, catalog.BOOLOID, 1)
	b.values = append(b.values, formatBool(value, format))
}

func (b *virtualResultBuilder) appendInt4(name string, value int32) {
	format := b.appendColumn(name, catalog.INT4OID, 4)
	var encoded []byte
	if format == xproto.FormatCodeBinary {
		encoded = binary.BigEndian.AppendUint32(nil, uint32(value))
	} else {
		encoded = fmt.Appendf(nil, "%d", value)
	}
	b.values = append(b.values, encoded)
}

func (b *virtualResultBuilder) appendText(name string, size int16, value string) {
	b.appendColumn(name, catalog.TEXTOID, size)
	b.values = append(b.values, []byte(value))
}
