// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

const nullProbability = 0.2
const randTypesProbability = 0.5

func TestSorterAgainstProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())

	seed := rand.Int()
	rng := rand.New(rand.NewSource(int64(seed)))
	nRuns := 10
	nRows := 100
	maxCols := 5
	maxNum := 10
	intTyps := make([]types.T, maxCols)
	for i := range intTyps {
		intTyps[i] = *types.Int
	}

	for run := 0; run < nRuns; run++ {
		for nCols := 1; nCols <= maxCols; nCols++ {
			var (
				rows       sqlbase.EncDatumRows
				inputTypes []types.T
			)
			if rng.Float64() < randTypesProbability {
				inputTypes = generateRandomSupportedTypes(rng, nCols)
				rows = sqlbase.RandEncDatumRowsOfTypes(rng, nRows, inputTypes)
			} else {
				inputTypes = intTyps[:nCols]
				rows = sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)
			}

			// Note: we're only generating column orderings on all nCols columns since
			// if there are columns not in the ordering, the results are not fully
			// deterministic.
			orderingCols := generateColumnOrdering(rng, nCols, nCols)
			sorterSpec := &distsqlpb.SorterSpec{
				OutputOrdering: distsqlpb.Ordering{Columns: orderingCols},
			}
			pspec := &distsqlpb.ProcessorSpec{
				Input: []distsqlpb.InputSyncSpec{{ColumnTypes: inputTypes}},
				Core:  distsqlpb.ProcessorCoreUnion{Sorter: sorterSpec},
			}
			if err := verifyColOperator(false /* anyOrder */, [][]types.T{inputTypes}, []sqlbase.EncDatumRows{rows}, inputTypes, pspec); err != nil {
				fmt.Printf("--- seed = %d nCols = %d types = %v ---\n", seed, nCols, inputTypes)
				t.Fatal(err)
			}
		}
	}
}

func TestSortChunksAgainstProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var da sqlbase.DatumAlloc
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())

	seed := rand.Int()
	rng := rand.New(rand.NewSource(int64(seed)))
	nRuns := 5
	nRows := 100
	maxCols := 5
	maxNum := 10
	intTyps := make([]types.T, maxCols)
	for i := range intTyps {
		intTyps[i] = *types.Int
	}

	for run := 0; run < nRuns; run++ {
		for nCols := 1; nCols <= maxCols; nCols++ {
			for matchLen := 1; matchLen <= nCols; matchLen++ {
				var (
					rows       sqlbase.EncDatumRows
					inputTypes []types.T
				)
				if rng.Float64() < randTypesProbability {
					inputTypes = generateRandomSupportedTypes(rng, nCols)
					rows = sqlbase.RandEncDatumRowsOfTypes(rng, nRows, inputTypes)
				} else {
					inputTypes = intTyps[:nCols]
					rows = sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)
				}

				// Note: we're only generating column orderings on all nCols columns since
				// if there are columns not in the ordering, the results are not fully
				// deterministic.
				orderingCols := generateColumnOrdering(rng, nCols, nCols)
				matchedCols := distsqlpb.ConvertToColumnOrdering(distsqlpb.Ordering{Columns: orderingCols[:matchLen]})
				// Presort the input on first matchLen columns.
				sort.Slice(rows, func(i, j int) bool {
					cmp, err := rows[i].Compare(inputTypes, &da, matchedCols, &evalCtx, rows[j])
					if err != nil {
						t.Fatal(err)
					}
					return cmp < 0
				})

				sorterSpec := &distsqlpb.SorterSpec{
					OutputOrdering:   distsqlpb.Ordering{Columns: orderingCols},
					OrderingMatchLen: uint32(matchLen),
				}
				pspec := &distsqlpb.ProcessorSpec{
					Input: []distsqlpb.InputSyncSpec{{ColumnTypes: inputTypes}},
					Core:  distsqlpb.ProcessorCoreUnion{Sorter: sorterSpec},
				}
				if err := verifyColOperator(false /* anyOrder */, [][]types.T{inputTypes}, []sqlbase.EncDatumRows{rows}, inputTypes, pspec); err != nil {
					fmt.Printf("--- seed = %d nCols = %d types = %v ---\n", seed, nCols, inputTypes)
					t.Fatal(err)
				}
			}
		}
	}
}

func TestHashJoinerAgainstProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	type hjTestSpec struct {
		joinType        sqlbase.JoinType
		onExprSupported bool
	}
	testSpecs := []hjTestSpec{
		{
			joinType:        sqlbase.JoinType_INNER,
			onExprSupported: true,
		},
		{
			joinType: sqlbase.JoinType_LEFT_OUTER,
		},
		{
			joinType: sqlbase.JoinType_RIGHT_OUTER,
		},
		{
			joinType: sqlbase.JoinType_FULL_OUTER,
		},
		{
			joinType: sqlbase.JoinType_LEFT_SEMI,
		},
	}

	seed := rand.Int()
	rng := rand.New(rand.NewSource(int64(seed)))
	nRuns := 3
	nRows := 10
	maxCols := 3
	maxNum := 5
	intTyps := make([]types.T, maxCols)
	for i := range intTyps {
		intTyps[i] = *types.Int
	}

	for run := 1; run < nRuns; run++ {
		for _, testSpec := range testSpecs {
			for nCols := 1; nCols <= maxCols; nCols++ {
				for nEqCols := 1; nEqCols <= nCols; nEqCols++ {
					triedWithoutOnExpr, triedWithOnExpr := false, false
					if !testSpec.onExprSupported {
						triedWithOnExpr = true
					}
					for !triedWithoutOnExpr || !triedWithOnExpr {
						var (
							lRows, rRows     sqlbase.EncDatumRows
							lEqCols, rEqCols []uint32
							inputTypes       []types.T
							usingRandomTypes bool
						)
						if rng.Float64() < randTypesProbability {
							inputTypes = generateRandomSupportedTypes(rng, nCols)
							lRows = sqlbase.RandEncDatumRowsOfTypes(rng, nRows, inputTypes)
							rRows = sqlbase.RandEncDatumRowsOfTypes(rng, nRows, inputTypes)
							lEqCols = generateEqualityColumns(rng, nCols, nEqCols)
							// Since random types might not be comparable, we use the same
							// equality columns for both inputs.
							rEqCols = lEqCols
							usingRandomTypes = true
						} else {
							inputTypes = intTyps[:nCols]
							lRows = sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)
							rRows = sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)
							lEqCols = generateEqualityColumns(rng, nCols, nEqCols)
							rEqCols = generateEqualityColumns(rng, nCols, nEqCols)
						}

						outputTypes := append(inputTypes, inputTypes...)
						if testSpec.joinType == sqlbase.JoinType_LEFT_SEMI {
							outputTypes = inputTypes
						}
						outputColumns := make([]uint32, len(outputTypes))
						for i := range outputColumns {
							outputColumns[i] = uint32(i)
						}

						var onExpr distsqlpb.Expression
						if triedWithoutOnExpr {
							colTypes := append(inputTypes, inputTypes...)
							onExpr = generateOnExpr(rng, nCols, nEqCols, colTypes, usingRandomTypes)
						}
						hjSpec := &distsqlpb.HashJoinerSpec{
							LeftEqColumns:  lEqCols,
							RightEqColumns: rEqCols,
							OnExpr:         onExpr,
							Type:           testSpec.joinType,
						}
						pspec := &distsqlpb.ProcessorSpec{
							Input: []distsqlpb.InputSyncSpec{{ColumnTypes: inputTypes}, {ColumnTypes: inputTypes}},
							Core:  distsqlpb.ProcessorCoreUnion{HashJoiner: hjSpec},
							Post:  distsqlpb.PostProcessSpec{Projection: true, OutputColumns: outputColumns},
						}
						if err := verifyColOperator(
							true, /* anyOrder */
							[][]types.T{inputTypes, inputTypes},
							[]sqlbase.EncDatumRows{lRows, rRows},
							outputTypes,
							pspec,
						); err != nil {
							fmt.Printf("--- join type = %s onExpr = %q seed = %d run = %d ---\n",
								testSpec.joinType.String(), onExpr.Expr, seed, run)
							fmt.Printf("--- lEqCols = %v rEqCols = %v ---\n", lEqCols, rEqCols)
							fmt.Printf("--- inputTypes = %v ---\n", inputTypes)
							t.Fatal(err)
						}
						if onExpr.Expr == "" {
							triedWithoutOnExpr = true
						} else {
							triedWithOnExpr = true
						}
					}
				}
			}
		}
	}
}

// generateEqualityColumns produces a random permutation of nEqCols random
// columns on a table with nCols columns, so nEqCols must be not greater than
// nCols.
func generateEqualityColumns(rng *rand.Rand, nCols int, nEqCols int) []uint32 {
	if nEqCols > nCols {
		panic("nEqCols > nCols in generateEqualityColumns")
	}
	eqCols := make([]uint32, 0, nEqCols)
	for _, eqCol := range rng.Perm(nCols)[:nEqCols] {
		eqCols = append(eqCols, uint32(eqCol))
	}
	return eqCols
}

func TestMergeJoinerAgainstProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var da sqlbase.DatumAlloc
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	type mjTestSpec struct {
		joinType        sqlbase.JoinType
		anyOrder        bool
		onExprSupported bool
	}
	testSpecs := []mjTestSpec{
		{
			joinType:        sqlbase.JoinType_INNER,
			onExprSupported: true,
		},
		{
			joinType: sqlbase.JoinType_LEFT_OUTER,
		},
		{
			joinType: sqlbase.JoinType_RIGHT_OUTER,
		},
		{
			joinType: sqlbase.JoinType_FULL_OUTER,
			// FULL OUTER JOIN doesn't guarantee any ordering on its output (since it
			// is ambiguous), so we're comparing the outputs as sets.
			anyOrder: true,
		},
		{
			joinType: sqlbase.JoinType_LEFT_SEMI,
		},
		{
			joinType: sqlbase.JoinType_LEFT_ANTI,
		},
	}

	seed := rand.Int()
	rng := rand.New(rand.NewSource(int64(seed)))
	nRuns := 3
	nRows := 10
	maxCols := 3
	maxNum := 5
	intTyps := make([]types.T, maxCols)
	for i := range intTyps {
		intTyps[i] = *types.Int
	}

	for run := 1; run < nRuns; run++ {
		for _, testSpec := range testSpecs {
			for nCols := 1; nCols <= maxCols; nCols++ {
				for nOrderingCols := 1; nOrderingCols <= nCols; nOrderingCols++ {
					triedWithoutOnExpr, triedWithOnExpr := false, false
					if !testSpec.onExprSupported {
						triedWithOnExpr = true
					}
					for !triedWithoutOnExpr || !triedWithOnExpr {
						var (
							lRows, rRows                 sqlbase.EncDatumRows
							inputTypes                   []types.T
							lOrderingCols, rOrderingCols []distsqlpb.Ordering_Column
							usingRandomTypes             bool
						)
						if rng.Float64() < randTypesProbability {
							inputTypes = generateRandomSupportedTypes(rng, nCols)
							lRows = sqlbase.RandEncDatumRowsOfTypes(rng, nRows, inputTypes)
							rRows = sqlbase.RandEncDatumRowsOfTypes(rng, nRows, inputTypes)
							lOrderingCols = generateColumnOrdering(rng, nCols, nOrderingCols)
							// We use the same ordering columns in the same order because the
							// columns can be not comparable in different order.
							rOrderingCols = lOrderingCols
							usingRandomTypes = true
						} else {
							inputTypes = intTyps[:nCols]
							lRows = sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)
							rRows = sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)
							lOrderingCols = generateColumnOrdering(rng, nCols, nOrderingCols)
							rOrderingCols = generateColumnOrdering(rng, nCols, nOrderingCols)
						}
						// Set the directions of both columns to be the same.
						for i, lCol := range lOrderingCols {
							rOrderingCols[i].Direction = lCol.Direction
						}

						lMatchedCols := distsqlpb.ConvertToColumnOrdering(distsqlpb.Ordering{Columns: lOrderingCols})
						rMatchedCols := distsqlpb.ConvertToColumnOrdering(distsqlpb.Ordering{Columns: rOrderingCols})
						sort.Slice(lRows, func(i, j int) bool {
							cmp, err := lRows[i].Compare(inputTypes, &da, lMatchedCols, &evalCtx, lRows[j])
							if err != nil {
								t.Fatal(err)
							}
							return cmp < 0
						})
						sort.Slice(rRows, func(i, j int) bool {
							cmp, err := rRows[i].Compare(inputTypes, &da, rMatchedCols, &evalCtx, rRows[j])
							if err != nil {
								t.Fatal(err)
							}
							return cmp < 0
						})
						outputTypes := append(inputTypes, inputTypes...)
						if testSpec.joinType == sqlbase.JoinType_LEFT_SEMI ||
							testSpec.joinType == sqlbase.JoinType_LEFT_ANTI {
							outputTypes = inputTypes
						}
						outputColumns := make([]uint32, len(outputTypes))
						for i := range outputColumns {
							outputColumns[i] = uint32(i)
						}

						var onExpr distsqlpb.Expression
						if triedWithoutOnExpr {
							colTypes := append(inputTypes, inputTypes...)
							onExpr = generateOnExpr(rng, nCols, nOrderingCols, colTypes, usingRandomTypes)
						}
						mjSpec := &distsqlpb.MergeJoinerSpec{
							OnExpr:        onExpr,
							LeftOrdering:  distsqlpb.Ordering{Columns: lOrderingCols},
							RightOrdering: distsqlpb.Ordering{Columns: rOrderingCols},
							Type:          testSpec.joinType,
						}
						pspec := &distsqlpb.ProcessorSpec{
							Input: []distsqlpb.InputSyncSpec{{ColumnTypes: inputTypes}, {ColumnTypes: inputTypes}},
							Core:  distsqlpb.ProcessorCoreUnion{MergeJoiner: mjSpec},
							Post:  distsqlpb.PostProcessSpec{Projection: true, OutputColumns: outputColumns},
						}
						if err := verifyColOperator(
							testSpec.anyOrder,
							[][]types.T{inputTypes, inputTypes},
							[]sqlbase.EncDatumRows{lRows, rRows},
							outputTypes,
							pspec,
						); err != nil {
							fmt.Printf("--- join type = %s onExpr = %q seed = %d run = %d ---\n",
								testSpec.joinType.String(), onExpr.Expr, seed, run)
							t.Fatal(err)
						}
						if onExpr.Expr == "" {
							triedWithoutOnExpr = true
						} else {
							triedWithOnExpr = true
						}
					}
				}
			}
		}
	}
}

// generateColumnOrdering produces a random ordering of nOrderingCols columns
// on a table with nCols columns, so nOrderingCols must be not greater than
// nCols.
func generateColumnOrdering(
	rng *rand.Rand, nCols int, nOrderingCols int,
) []distsqlpb.Ordering_Column {
	if nOrderingCols > nCols {
		panic("nOrderingCols > nCols in generateColumnOrdering")
	}

	orderingCols := make([]distsqlpb.Ordering_Column, nOrderingCols)
	for i, col := range rng.Perm(nCols)[:nOrderingCols] {
		orderingCols[i] = distsqlpb.Ordering_Column{
			ColIdx:    uint32(col),
			Direction: distsqlpb.Ordering_Column_Direction(rng.Intn(2)),
		}
	}
	return orderingCols
}

// generateOnExpr populates a distsqlpb.Expression that contains a single
// comparison which can be either comparing a column from the left against a
// column from the right or comparing a column from either side against a
// constant.
// If forceConstComparison is true, then the comparison against the constant
// will be used.
func generateOnExpr(
	rng *rand.Rand, nCols int, nEqCols int, colTypes []types.T, forceConstComparison bool,
) distsqlpb.Expression {
	var comparison string
	r := rng.Float64()
	if r < 0.25 {
		comparison = "<"
	} else if r < 0.5 {
		comparison = ">"
	} else if r < 0.75 {
		comparison = "="
	} else {
		comparison = "<>"
	}
	// When all columns are used in equality comparison between inputs, there is
	// only one interesting case when a column from either side is compared
	// against a constant. The second conditional is us choosing to compare
	// against a constant.
	if nCols == nEqCols || rng.Float64() < 0.33 || forceConstComparison {
		colIdx := rng.Intn(nCols)
		if rng.Float64() >= 0.5 {
			// Use right side.
			colIdx += nCols
		}
		constDatum := sqlbase.RandDatum(rng, &colTypes[colIdx], true /* nullOk */)
		constDatumString := constDatum.String()
		if strings.Contains(constDatumString, "NaN") || strings.Contains(constDatumString, "Inf") {
			// We need to surround special values with quotes.
			constDatumString = fmt.Sprintf("'%s'", constDatumString)
		}
		return distsqlpb.Expression{Expr: fmt.Sprintf("@%d %s %s", colIdx+1, comparison, constDatumString)}
	}
	// We will compare a column from the left against a column from the right.
	leftColIdx := rng.Intn(nCols) + 1
	rightColIdx := rng.Intn(nCols) + nCols + 1
	return distsqlpb.Expression{Expr: fmt.Sprintf("@%d %s @%d", leftColIdx, comparison, rightColIdx)}
}

func TestWindowFunctionsAgainstProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rng, _ := randutil.NewPseudoRand()

	nRows := 10
	maxCols := 4
	maxNum := 5
	typs := make([]types.T, maxCols)
	for i := range typs {
		// TODO(yuzefovich): randomize the types of the columns once we support
		// window functions that take in arguments.
		typs[i] = *types.Int
	}
	for _, windowFn := range []distsqlpb.WindowerSpec_WindowFunc{
		distsqlpb.WindowerSpec_ROW_NUMBER,
		distsqlpb.WindowerSpec_RANK,
		distsqlpb.WindowerSpec_DENSE_RANK,
	} {
		for _, partitionBy := range [][]uint32{
			{},     // No PARTITION BY clause.
			{0},    // Partitioning on the first input column.
			{0, 1}, // Partitioning on the first and second input columns.
		} {
			for _, nOrderingCols := range []int{
				0, // No ORDER BY clause.
				1, // ORDER BY on at most one column.
				2, // ORDER BY on at most two columns.
			} {
				for nCols := 1; nCols <= maxCols; nCols++ {
					if len(partitionBy) > nCols || nOrderingCols > nCols {
						continue
					}
					inputTypes := typs[:nCols]
					rows := sqlbase.MakeRandIntRowsInRange(rng, nRows, nCols, maxNum, nullProbability)

					windowerSpec := &distsqlpb.WindowerSpec{
						PartitionBy: partitionBy,
						WindowFns: []distsqlpb.WindowerSpec_WindowFn{
							{
								Func:         distsqlpb.WindowerSpec_Func{WindowFunc: &windowFn},
								Ordering:     generateOrderingGivenPartitionBy(rng, nCols, nOrderingCols, partitionBy),
								OutputColIdx: uint32(nCols),
							},
						},
					}
					if windowFn == distsqlpb.WindowerSpec_ROW_NUMBER &&
						len(partitionBy)+len(windowerSpec.WindowFns[0].Ordering.Columns) < nCols {
						// The output of row_number is not deterministic if there are
						// columns that are not present in either PARTITION BY or ORDER BY
						// clauses, so we skip such a configuration.
						continue
					}

					pspec := &distsqlpb.ProcessorSpec{
						Input: []distsqlpb.InputSyncSpec{{ColumnTypes: inputTypes}},
						Core:  distsqlpb.ProcessorCoreUnion{Windower: windowerSpec},
					}
					if err := verifyColOperator(true /* anyOrder */, [][]types.T{inputTypes}, []sqlbase.EncDatumRows{rows}, append(inputTypes, *types.Int), pspec); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}
}

// generateRandomSupportedTypes generates nCols random types that are supported
// by the vectorized engine.
func generateRandomSupportedTypes(rng *rand.Rand, nCols int) []types.T {
	typs := make([]types.T, 0, nCols)
	for len(typs) < nCols {
		typ := sqlbase.RandType(rng)
		converted := typeconv.FromColumnType(typ)
		if converted != coltypes.Unhandled {
			typs = append(typs, *typ)
		}
	}
	return typs
}

// generateOrderingGivenPartitionBy produces a random ordering of up to
// nOrderingCols columns on a table with nCols columns such that only columns
// not present in partitionBy are used. This is useful to simulate how
// optimizer plans window functions - for example, with an OVER clause as
// (PARTITION BY a ORDER BY a DESC), the optimizer will omit the ORDER BY
// clause entirely.
func generateOrderingGivenPartitionBy(
	rng *rand.Rand, nCols int, nOrderingCols int, partitionBy []uint32,
) distsqlpb.Ordering {
	var ordering distsqlpb.Ordering
	if nOrderingCols == 0 || len(partitionBy) == nCols {
		return ordering
	}
	ordering = distsqlpb.Ordering{Columns: make([]distsqlpb.Ordering_Column, 0, nOrderingCols)}
	for len(ordering.Columns) == 0 {
		for _, ordCol := range generateColumnOrdering(rng, nCols, nOrderingCols) {
			usedInPartitionBy := false
			for _, p := range partitionBy {
				if p == ordCol.ColIdx {
					usedInPartitionBy = true
					break
				}
			}
			if !usedInPartitionBy {
				ordering.Columns = append(ordering.Columns, ordCol)
			}
		}
	}
	return ordering
}
