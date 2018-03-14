package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func TestResolveSubqueries(t *testing.T) {
	require := require.New(t)

	table1 := mem.NewTable("foo", sql.Schema{{Name: "a", Type: sql.Int64, Source: "foo"}})
	table2 := mem.NewTable("bar", sql.Schema{{Name: "b", Type: sql.Int64, Source: "bar"}})
	table3 := mem.NewTable("baz", sql.Schema{{Name: "c", Type: sql.Int64, Source: "baz"}})
	db := mem.NewDatabase("mydb")
	memDb, ok := db.(*mem.Database)
	require.True(ok)

	memDb.AddTable("foo", table1)
	memDb.AddTable("bar", table2)
	memDb.AddTable("baz", table3)

	catalog := &sql.Catalog{Databases: []sql.Database{db}}
	a := New(catalog)
	a.CurrentDatabase = "mydb"

	// SELECT * FROM
	// 	(SELECT a FROM foo) t1,
	// 	(SELECT b FROM (SELECT b FROM bar) t2alias) t2,
	//  baz
	node := plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewCrossJoin(
			plan.NewCrossJoin(
				plan.NewSubqueryAlias(
					"t1",
					plan.NewProject(
						[]sql.Expression{expression.NewUnresolvedColumn("a")},
						plan.NewUnresolvedTable("foo"),
					),
				),
				plan.NewSubqueryAlias(
					"t2",
					plan.NewProject(
						[]sql.Expression{expression.NewUnresolvedColumn("b")},
						plan.NewSubqueryAlias(
							"t2alias",
							plan.NewProject(
								[]sql.Expression{expression.NewUnresolvedColumn("b")},
								plan.NewUnresolvedTable("bar"),
							),
						),
					),
				),
			),
			plan.NewUnresolvedTable("baz"),
		),
	)

	subquery := plan.NewSubqueryAlias(
		"t2alias",
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "bar", "b", false),
			},
			table2,
		),
	)
	_ = subquery.Schema()

	expected := plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewCrossJoin(
			plan.NewCrossJoin(
				plan.NewSubqueryAlias(
					"t1",
					plan.NewProject(
						[]sql.Expression{
							expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
						},
						table1,
					),
				),
				plan.NewSubqueryAlias(
					"t2",
					plan.NewProject(
						[]sql.Expression{
							expression.NewGetFieldWithTable(0, sql.Int64, "t2alias", "b", false),
						},
						subquery,
					),
				),
			),
			plan.NewUnresolvedTable("baz"),
		),
	)

	result, err := resolveSubqueries(a, node)
	require.NoError(err)

	require.Equal(expected, result)
}

func TestResolveTables(t *testing.T) {
	require := require.New(t)

	f := getRule("resolve_tables")

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	db := mem.NewDatabase("mydb")
	memDb, ok := db.(*mem.Database)
	require.True(ok)

	memDb.AddTable("mytable", table)

	catalog := &sql.Catalog{Databases: []sql.Database{db}}

	a := New(catalog)
	a.Rules = []Rule{f}

	a.CurrentDatabase = "mydb"
	var notAnalyzed sql.Node = plan.NewUnresolvedTable("mytable")
	analyzed, err := f.Apply(a, notAnalyzed)
	require.NoError(err)
	require.Equal(table, analyzed)

	notAnalyzed = plan.NewUnresolvedTable("nonexistant")
	analyzed, err = f.Apply(a, notAnalyzed)
	require.Error(err)
	require.Nil(analyzed)

	analyzed, err = f.Apply(a, table)
	require.NoError(err)
	require.Equal(table, analyzed)
}

func TestResolveTablesNested(t *testing.T) {
	require := require.New(t)

	f := getRule("resolve_tables")

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	db := mem.NewDatabase("mydb")
	memDb, ok := db.(*mem.Database)
	require.True(ok)

	memDb.AddTable("mytable", table)

	catalog := &sql.Catalog{Databases: []sql.Database{db}}

	a := New(catalog)
	a.Rules = []Rule{f}
	a.CurrentDatabase = "mydb"

	notAnalyzed := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", true)},
		plan.NewUnresolvedTable("mytable"),
	)
	analyzed, err := f.Apply(a, notAnalyzed)
	require.NoError(err)
	expected := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", true)},
		table,
	)
	require.Equal(expected, analyzed)
}

func TestResolveStar(t *testing.T) {
	require := require.New(t)
	f := getRule("resolve_star")

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	db := mem.NewDatabase("mydb")
	memDb, ok := db.(*mem.Database)
	require.True(ok)

	memDb.AddTable("mytable", table)

	catalog := &sql.Catalog{Databases: []sql.Database{db}}

	a := New(catalog)
	a.Rules = []Rule{f}
	a.CurrentDatabase = "mydb"

	notAnalyzed := plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		table,
	)
	analyzed, err := f.Apply(a, notAnalyzed)
	require.NoError(err)
	expected := plan.NewProject(
		[]sql.Expression{expression.NewGetField(0, sql.Int32, "i", false)},
		table,
	)
	require.Equal(expected, analyzed)
}

func TestQualifyColumns(t *testing.T) {
	require := require.New(t)
	f := getRule("qualify_columns")

	table := mem.NewTable("mytable", sql.Schema{{Name: "i", Type: sql.Int32}})
	table2 := mem.NewTable("mytable2", sql.Schema{{Name: "i", Type: sql.Int32}})

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("i"),
		},
		table,
	)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("mytable", "i"),
		},
		table,
	)

	result, err := f.Apply(nil, node)
	require.NoError(err)
	require.Equal(expected, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("mytable", "i"),
		},
		table,
	)

	result, err = f.Apply(nil, node)
	require.NoError(err)
	require.Equal(expected, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("a", "i"),
		},
		plan.NewTableAlias("a", table),
	)

	expected = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("mytable", "i"),
		},
		plan.NewTableAlias("a", table),
	)

	result, err = f.Apply(nil, node)
	require.NoError(err)
	require.Equal(expected, result)

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("foo", "i"),
		},
		plan.NewTableAlias("a", table),
	)

	result, err = f.Apply(nil, node)
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("b"),
		},
		plan.NewTableAlias("a", table),
	)

	_, err = f.Apply(nil, node)
	require.Error(err)
	require.True(ErrColumnTableNotFound.Is(err))

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("i"),
		},
		plan.NewCrossJoin(table, table2),
	)

	_, err = f.Apply(nil, node)
	require.Error(err)
	require.True(ErrAmbiguousColumnName.Is(err))

	subquery := plan.NewSubqueryAlias(
		"b",
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
			},
			table,
		),
	)
	// preload schema
	_ = subquery.Schema()

	node = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("a", "i"),
		},
		plan.NewCrossJoin(
			plan.NewTableAlias("a", table),
			subquery,
		),
	)

	expected = plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("mytable", "i"),
		},
		plan.NewCrossJoin(
			plan.NewTableAlias("a", table),
			subquery,
		),
	)

	result, err = f.Apply(nil, node)
	require.NoError(err)
	require.Equal(expected, result)
}

func TestOptimizeDistinct(t *testing.T) {
	require := require.New(t)
	notSorted := plan.NewDistinct(mem.NewTable("foo", nil))
	sorted := plan.NewDistinct(plan.NewSort(nil, mem.NewTable("foo", nil)))

	rule := getRule("optimize_distinct")

	analyzedNotSorted, err := rule.Apply(nil, notSorted)
	require.NoError(err)

	analyzedSorted, err := rule.Apply(nil, sorted)
	require.NoError(err)

	require.Equal(notSorted, analyzedNotSorted)
	require.Equal(plan.NewOrderedDistinct(sorted.Child), analyzedSorted)
}

func TestPushdownProjection(t *testing.T) {
	require := require.New(t)
	f := getRule("pushdown")

	table := &pushdownProjectionTable{mem.NewTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int32},
		{Name: "f", Type: sql.Float64},
		{Name: "t", Type: sql.Text},
	})}

	table2 := &pushdownProjectionTable{mem.NewTable("mytable2", sql.Schema{
		{Name: "i2", Type: sql.Int32},
		{Name: "f2", Type: sql.Float64},
		{Name: "t2", Type: sql.Text},
	})}

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
		},
		plan.NewFilter(
			expression.NewAnd(
				expression.NewEquals(
					expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
					expression.NewLiteral(3.14, sql.Float64),
				),
				expression.NewIsNull(
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
				),
			),
			plan.NewCrossJoin(table, table2),
		),
	)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
		},
		plan.NewFilter(
			expression.NewAnd(
				expression.NewEquals(
					expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
					expression.NewLiteral(3.14, sql.Float64),
				),
				expression.NewIsNull(
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
				),
			),
			plan.NewCrossJoin(
				plan.NewPushdownProjectionTable([]string{"i", "f"}, table),
				plan.NewPushdownProjectionTable([]string{"i2"}, table2),
			),
		),
	)

	result, err := f.Apply(nil, node)
	require.NoError(err)
	require.Equal(expected, result)
}

func TestPushdownProjectionAndFilters(t *testing.T) {
	require := require.New(t)
	f := getRule("pushdown")

	table := &pushdownProjectionAndFiltersTable{mem.NewTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int32},
		{Name: "f", Type: sql.Float64},
		{Name: "t", Type: sql.Text},
	})}

	table2 := &pushdownProjectionAndFiltersTable{mem.NewTable("mytable2", sql.Schema{
		{Name: "i2", Type: sql.Int32},
		{Name: "f2", Type: sql.Float64},
		{Name: "t2", Type: sql.Text},
	})}

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
		},
		plan.NewFilter(
			expression.NewAnd(
				expression.NewAnd(
					expression.NewEquals(
						expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
						expression.NewLiteral(3.14, sql.Float64),
					),
					expression.NewGreaterThan(
						expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
						expression.NewLiteral(3., sql.Float64),
					),
				),
				expression.NewIsNull(
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
				),
			),
			plan.NewCrossJoin(table, table2),
		),
	)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
		},
		plan.NewFilter(
			expression.NewAnd(
				expression.NewGreaterThan(
					expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
					expression.NewLiteral(3., sql.Float64),
				),
				expression.NewIsNull(
					expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
				),
			),
			plan.NewCrossJoin(
				plan.NewPushdownProjectionAndFiltersTable(
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int32, "mytable", "i", false),
						expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
					},
					[]sql.Expression{
						expression.NewEquals(
							expression.NewGetFieldWithTable(1, sql.Float64, "mytable", "f", false),
							expression.NewLiteral(3.14, sql.Float64),
						),
					},
					table,
				),
				plan.NewPushdownProjectionAndFiltersTable(
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, sql.Int32, "mytable2", "i2", false),
					},
					nil,
					table2,
				),
			),
		),
	)

	result, err := f.Apply(nil, node)
	require.NoError(err)
	require.Equal(expected, result)
}

type pushdownProjectionTable struct {
	sql.Table
}

func (pushdownProjectionTable) WithProject(sql.Session, []string) (sql.RowIter, error) {
	panic("not implemented")
}

func (t *pushdownProjectionTable) TransformUp(f func(sql.Node) (sql.Node, error)) (sql.Node, error) {
	table, err := f(t.Table)
	if err != nil {
		return nil, err
	}
	return f(&pushdownProjectionTable{table.(sql.Table)})
}

type pushdownProjectionAndFiltersTable struct {
	sql.Table
}

func (pushdownProjectionAndFiltersTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	var handled []sql.Expression
	for _, f := range filters {
		if eq, ok := f.(*expression.Equals); ok {
			handled = append(handled, eq)
		}
	}
	return handled
}

func (pushdownProjectionAndFiltersTable) WithProjectAndFilters(_ sql.Session, cols, filters []sql.Expression) (sql.RowIter, error) {
	panic("not implemented")
}

func (t *pushdownProjectionAndFiltersTable) TransformUp(f func(sql.Node) (sql.Node, error)) (sql.Node, error) {
	table, err := f(t.Table)
	if err != nil {
		return nil, err
	}
	return f(&pushdownProjectionAndFiltersTable{table.(sql.Table)})
}

func TestCastCompares(t *testing.T) {
	f := getRule("cast_compares")

	tests := []struct {
		name     string
		node     sql.Node
		expected sql.Node
	}{
		{
			name: `number >= 2`,
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGreaterThanOrEqual(
						expression.NewGetField(1, sql.Int32, "number", false),
						expression.NewLiteral(int64(2), sql.Int64),
					),
				},
				plan.NewUnresolvedTable("foo"),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGreaterThanOrEqual(
						expression.NewConvert(
							expression.NewGetField(1, sql.Int32, "number", false),
							expression.ConvertToSigned,
						),
						expression.NewConvert(
							expression.NewLiteral(int64(2), sql.Int64),
							expression.ConvertToSigned,
						),
					),
				},
				plan.NewUnresolvedTable("foo"),
			),
		},
		{
			name: `text >= 2`,
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGreaterThanOrEqual(
						expression.NewGetField(0, sql.Text, "text", false),
						expression.NewLiteral(int64(2), sql.Int64),
					),
				},
				plan.NewUnresolvedTable("foo"),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGreaterThanOrEqual(
						expression.NewConvert(
							expression.NewGetField(0, sql.Text, "text", false),
							expression.ConvertToSigned,
						),
						expression.NewConvert(
							expression.NewLiteral(int64(2), sql.Int64),
							expression.ConvertToSigned,
						),
					),
				},
				plan.NewUnresolvedTable("foo"),
			),
		},
		{
			name: `text >= "3"`,
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGreaterThanOrEqual(
						expression.NewGetField(0, sql.Text, "text", false),
						expression.NewLiteral("3", sql.Text),
					),
				},
				plan.NewUnresolvedTable("foo"),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGreaterThanOrEqual(
						expression.NewGetField(0, sql.Text, "text", false),
						expression.NewLiteral("3", sql.Text),
					),
				},
				plan.NewUnresolvedTable("foo"),
			),
		},
		{
			name: `date >= "3"`,
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGreaterThanOrEqual(
						expression.NewGetField(0, sql.Timestamp, "date", false),
						expression.NewLiteral("3", sql.Text),
					),
				},
				plan.NewUnresolvedTable("foo"),
			),
			expected: plan.NewProject(
				[]sql.Expression{
					expression.NewGreaterThanOrEqual(
						expression.NewConvert(
							expression.NewGetField(0, sql.Timestamp, "date", false),
							expression.ConvertToChar,
						),
						expression.NewConvert(
							expression.NewLiteral("3", sql.Text),
							expression.ConvertToChar,
						),
					),
				},
				plan.NewUnresolvedTable("foo"),
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			casted, err := f.Apply(nil, test.node)
			require.NoError(t, err)
			require.Equal(t, test.expected, casted)
		})
	}

}

func getRule(name string) Rule {
	for _, rule := range DefaultRules {
		if rule.Name == name {
			return rule
		}
	}
	panic("missing rule")
}
