package engine

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/catalog"
	"github.com/pg-sharding/spqr/pkg/models/spqrerror"
)

const (
	CountAgg = "count"
	MinAgg   = "min"
	MaxAgg   = "max"
	SumAgg   = "sum"
)

type AggregateState interface {
	Init(oid int, format int16)

	Aggregate(v []byte) error

	Finalize(format int16) []byte
}

type CountAggregate struct {
	v      int64
	oid    int
	format int16
}

// Aggregate implements [AggregateState].
func (c *CountAggregate) Aggregate(v []byte) error {
	var n int64
	switch c.format {
	case pgtype.TextFormatCode:
		var err error
		n, err = strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return err
		}
	case pgtype.BinaryFormatCode:
		switch c.oid {
		case catalog.INT4OID:
			if len(v) != 4 {
				return fmt.Errorf("invalid binary int4 aggregate value length: %d", len(v))
			}
			n = int64(int32(binary.BigEndian.Uint32(v)))
		case catalog.INT8OID:
			if len(v) != 8 {
				return fmt.Errorf("invalid binary int8 aggregate value length: %d", len(v))
			}
			n = int64(binary.BigEndian.Uint64(v))
		default:
			return fmt.Errorf("unsupported binary count aggregate input type: %d", c.oid)
		}
	default:
		return fmt.Errorf("unsupported count aggregate input format: %d", c.format)
	}
	c.v += n
	return nil
}

// Finalize implements [AggregateState].
func (c *CountAggregate) Finalize(format int16) []byte {
	if format == pgtype.BinaryFormatCode {
		return binary.BigEndian.AppendUint64(nil, uint64(c.v))
	}
	return strconv.AppendInt(nil, c.v, 10)
}

// Init implements [AggregateState].
func (c *CountAggregate) Init(oid int, format int16) {
	c.v = 0
	c.oid = oid
	c.format = format
}

var _ AggregateState = &CountAggregate{}

func EngineV2AggregateFunction(n string) bool {
	switch n {
	case CountAgg, MaxAgg, MinAgg, SumAgg:
		return true
	default:
		return false
	}
}

func CreateAggregate(name string, _ []lyx.Node) (AggregateState, error) {
	switch name {
	case CountAgg:
		return &CountAggregate{}, nil
	default:
		return nil, spqrerror.Newf(spqrerror.SPQR_UNEXPECTED, "not supported aggregate %s", name)
	}
}
