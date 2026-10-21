package planner

import (
	"context"
	"fmt"
	"math"

	"github.com/pg-sharding/lyx/lyx"
	"github.com/pg-sharding/spqr/pkg/config"
	"github.com/pg-sharding/spqr/pkg/plan"
	"github.com/pg-sharding/spqr/pkg/tupleslot"
	"github.com/pg-sharding/spqr/router/rmeta"
	"github.com/pg-sharding/spqr/router/virtual"
	"github.com/pg-sharding/spqr/router/xproto"
)

func formatBool(v bool, format int16) []byte {
	switch format {
	case xproto.FormatCodeBinary:
		if v {
			return []byte{1}
		} else {
			return []byte{0}
		}
	default:
		if v {
			return []byte{'t'}
		} else {
			return []byte{'f'}
		}
	}
}

func PlanTargetList(ctx context.Context, rm *rmeta.RoutingMetadataContext, plr QueryPlanner, stmt *lyx.Select) (plan.Plan, error) {
	return planTargetList(ctx, rm, plr, stmt, false)
}

// PlanVirtualTargetList rebuilds a purely local target list without planning backend work.
// A nil plan means the SELECT requires another execution path.
func PlanVirtualTargetList(ctx context.Context, rm *rmeta.RoutingMetadataContext, plr QueryPlanner, stmt *lyx.Select) (*plan.VirtualPlan, error) {
	if len(stmt.FromClause) != 0 || stmt.LArg != nil || stmt.RArg != nil || stmt.WithClause != nil {
		return nil, nil
	}
	p, err := planTargetList(ctx, rm, plr, stmt, true)
	if err != nil {
		return nil, err
	}
	virtualPlan, _ := p.(*plan.VirtualPlan)
	return virtualPlan, nil
}

func planTargetList(ctx context.Context, rm *rmeta.RoutingMetadataContext, plr QueryPlanner, stmt *lyx.Select, virtualOnly bool) (plan.Plan, error) {
	row := virtualResultBuilder{rm: rm}

	var p plan.Plan

	for _, expr := range stmt.TargetList {
		actualExpr := expr
		colname := "?column?"
		if rt, ok := expr.(*lyx.ResTarget); ok {
			actualExpr = rt.Value
			colname = rt.Name
		}

		switch e := actualExpr.(type) {
		case *lyx.SVFOP_CURRENT_USER:
			p = plan.Combine(p, &plan.VirtualPlan{})
			row.appendText(colname, -1, rm.SPH.Usr())
			continue

		case *lyx.AExprNot:
			/* inspect our arg. If this is pg_is_in_recovery, apply NOT */
			switch arg := e.Arg.(type) {
			case *lyx.FuncApplication:
				if arg.Name == "pg_is_in_recovery" {
					p = plan.Combine(p, &plan.VirtualPlan{})

					/* notice this sign */
					row.appendBool("pg_is_in_recovery", rm.SPH.GetTsa() == config.TargetSessionAttrsRW)

					continue
				}
			}
		/* Special cases for SELECT current_schema(), SELECT set_config(...), and SELECT pg_is_in_recovery() */
		case *lyx.FuncApplication:

			/* for queries, that need to access data on shard, ignore these "virtual" func.	 */
			if e.Name == "pg_is_in_recovery" {
				p = plan.Combine(p, &plan.VirtualPlan{})
				row.appendBool("pg_is_in_recovery", rm.SPH.GetTsa() != config.TargetSessionAttrsRW)

				continue
			} else if e.Name == virtual.VirtualFuncIsReady {
				p = plan.Combine(p, &plan.VirtualPlan{})
				row.appendBool(virtual.VirtualFuncIsReady, plr.Ready())
				continue
			} else if e.Name == virtual.VirtualFuncHosts {
				p = plan.Combine(p, &plan.VirtualPlan{})
				if len(e.Args) == 1 {
					var k string

					switch vv := e.Args[0].(type) {
					case *lyx.AExprSConst:
						k = vv.Value
					default:
						return nil, fmt.Errorf("incorrect argument type for %s", virtual.VirtualFuncHosts)
					}

					if v, ok := rm.CSM.InstanceHealthChecks()[k]; ok {
						row.appendText("host", 1, k)
						row.appendText("rw", 1, fmt.Sprintf("%v", v.CR.RW))
					} else {
						return nil, fmt.Errorf("incorrect first argument for %s", virtual.VirtualFuncHosts)
					}
				} else {
					return nil, fmt.Errorf("incorrect argument number for %s", virtual.VirtualFuncHosts)
				}

				continue
			} else if e.Name == "current_setting" && len(e.Args) == 1 {
				if val, ok := e.Args[0].(*lyx.AExprSConst); ok && val.Value == "transaction_read_only" {
					p = plan.Combine(p, &plan.VirtualPlan{})
					row.appendBool("current_setting", rm.SPH.GetTsa() != config.TargetSessionAttrsRW)

					continue
				}
			}

		/* Expression like SELECT 1, SELECT 'a', SELECT 1.0, SELECT true, SELECT false */
		case *lyx.AExprSConst:

			p = plan.Combine(p, &plan.VirtualPlan{})
			row.appendText(colname, -1, e.Value)
			continue

		case *lyx.AExprIConst:
			if e.Value < math.MinInt32 || e.Value > math.MaxInt32 {
				break
			}

			p = plan.Combine(p, &plan.VirtualPlan{})
			row.appendInt4(colname, int32(e.Value))
			continue
		}

		if virtualOnly {
			return nil, nil
		}

		switch e := actualExpr.(type) {
		case *lyx.FuncApplication:
			if e.Name == "current_schema" || e.Name == "now" || e.Name == "set_config" || e.Name == "version" || e.Name == "current_setting" {
				p = plan.Combine(p, &plan.RandomDispatchPlan{})
				continue
			}
			deduced := false
			for _, innerExp := range e.Args {
				switch iE := innerExp.(type) {
				case *lyx.Select:
					if tmp, err := plr.PlanQueryTopLevel(ctx, rm, iE); err != nil {
						return nil, err
					} else {
						p = plan.Combine(p, tmp)
						deduced = true
					}
				case *lyx.SubLink:
					if tmp, err := plr.PlanQueryTopLevel(ctx, rm, iE.SubSelect); err != nil {
						return nil, err
					} else {
						p = plan.Combine(p, tmp)
						deduced = true
					}
				}
			}
			if !deduced {
				/* very questionable. */
				p = plan.Combine(p, &plan.RandomDispatchPlan{})
			}
		case *lyx.AExprIConst, *lyx.AExprNConst, *lyx.AExprBConst:
			p = plan.Combine(p, &plan.RandomDispatchPlan{})

		/* Special case for SELECT current_schema */
		case *lyx.ColumnRef:
			if e.ColName == "current_schema" {
				p = plan.Combine(p, &plan.RandomDispatchPlan{})
			}
		case *lyx.Select:
			if tmp, err := plr.PlanQueryTopLevel(ctx, rm, e); err != nil {
				return nil, err
			} else {
				p = plan.Combine(p, tmp)
			}
		case *lyx.SubLink:
			if tmp, err := plr.PlanQueryTopLevel(ctx, rm, e.SubSelect); err != nil {
				return nil, err
			} else {
				p = plan.Combine(p, tmp)
			}
		}
	}

	switch q := p.(type) {
	case *plan.VirtualPlan:
		q.TTS = &tupleslot.TupleTableSlot{
			Desc: row.fields,
			Raw:  [][][]byte{row.values},
		}
	}

	return p, nil
}

func PlanWithClause(ctx context.Context, rm *rmeta.RoutingMetadataContext, plr QueryPlanner, withClause []*lyx.CommonTableExpr) (plan.Plan, error) {
	var p plan.Plan
	for _, cte := range withClause {
		if tmp, err := plr.PlanQueryTopLevel(ctx, rm, cte.SubQuery); err != nil {
			return nil, err
		} else {
			p = plan.Combine(p, tmp)
		}
	}

	return p, nil
}
