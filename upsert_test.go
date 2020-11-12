package pg_util

import "testing"

func TestTestBuildupsert(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name, sql     string
		opts          UpsertOpts
		args          []interface{}
		before, after func()
	}

	type inner struct {
		F3 int
	}

	type innerOverlapping struct {
		F2 int
	}

	ch := make(chan struct{})

	cases := [...]testCase{
		{
			name: "simple",
			opts: UpsertOpts{
				Table: "t1",
				Data: struct {
					F1 string
					F2 int
				}{"aaa", 1},
			},
			sql:  `insert into t1 (F1,F2) values ($1,$2) on conflict () do update set F1 = excluded.F1,F2 = excluded.F2`,
			args: []interface{}{"aaa", 1},
			after: func() {
				close(ch)
			},
		},
		{
			name: "cached",
			opts: UpsertOpts{
				Table: "t1",
				Data: struct {
					F1 string
					F2 int
				}{"aaa", 1},
			},
			sql:  `insert into t1 (F1,F2) values ($1,$2) on conflict () do update set F1 = excluded.F1,F2 = excluded.F2`,
			args: []interface{}{"aaa", 1},
			before: func() {
				// Ensure this test always runs after "simple"
				<-ch
			},
		},
		{
			name: "with name tag and unique tag assuming unique field is also primary key",
			opts: UpsertOpts{
				Table: "t1",
				Data: struct {
					F1 string `db:"field_1,unique"`
					F2 int    `db:"field_2"`
				}{"aaa", 1},
			},
			sql:  `insert into t1 (field_1,field_2) values ($1,$2) on conflict (field_1) do update set field_1 = excluded.field_1,field_2 = excluded.field_2`,
			args: []interface{}{"aaa", 1},
		},
		{
			name: "with name tag and unique tag and constrain",
			opts: UpsertOpts{
				Table: "t1",
				Data: struct {
					F1 string `db:"field_1,unique"`
					F2 int    `db:"field_2"`
				}{"aaa", 1},
				Constrain: "test_constrain",
			},
			sql:  `insert into t1 (field_1,field_2) values ($1,$2) on conflict (test_constrain) do update set field_1 = excluded.field_1,field_2 = excluded.field_2`,
			args: []interface{}{"aaa", 1},
		},
		//{
		//	name: "with only string tag",
		//	opts: UpsertOpts{
		//		Table: "t1",
		//		Data: struct {
		//			F1 string `db:"field_1"`
		//			F2 int    `db:",string"`
		//		}{"aaa", 1},
		//	},
		//	sql:  `insert into t1 (field_1,F2) values ($1,$2)`,
		//	args: []interface{}{"aaa", 1},
		//},
		//{
		//	name: "with skipped field",
		//	opts: UpsertOpts{
		//		Table: "t1",
		//		Data: struct {
		//			F1 string
		//			F2 int
		//			F3 int `db:"-"`
		//		}{"aaa", 1, 1},
		//	},
		//	sql:  `insert into t1 (F1,F2) values ($1,$2)`,
		//	args: []interface{}{"aaa", 1},
		//},
		//{
		//	name: "with prefix and suffix",
		//	opts: UpsertOpts{
		//		Table: "t1",
		//		Data: struct {
		//			F1 string
		//			F2 int
		//		}{"aaa", 1},
		//		Prefix: "with v as (select 1)",
		//		Suffix: "returning f1",
		//	},
		//	sql: `with v as (select 1) insert into t1 (F1,F2) values ($1,$2)` +
		//		` returning f1`,
		//	args: []interface{}{"aaa", 1},
		//},
		{
			name: "with embedded struct",
			opts: UpsertOpts{
				Table: "t1",
				Data: struct {
					F1 string
					F2 int
					inner
				}{"aaa", 1, inner{3}},
			},
			sql:  `insert into t1 (F1,F2,F3) values ($1,$2,$3) on conflict () do update set F1 = excluded.F1,F2 = excluded.F2,F3 = excluded.F3`,
			args: []interface{}{"aaa", 1, 3},
		},
		{
			name: "with embedded struct override",
			opts: UpsertOpts{
				Table: "t2",
				Data: struct {
					innerOverlapping
					F1 string
					F2 int
				}{innerOverlapping{3}, "aaa", 1},
			},
			sql:  `insert into t2 (F1,F2) values ($1,$2) on conflict () do update set F1 = excluded.F1,F2 = excluded.F2`,
			args: []interface{}{"aaa", 1},
		},
		//{
		//	name: "with many args",
		//	opts: UpsertOpts{
		//		Table: "t1",
		//		Data: struct {
		//			F1 string
		//			F2 int
		//			F3,
		//			F4,
		//			F5,
		//			F6,
		//			F7,
		//			F8,
		//			F9,
		//			F10 int
		//		}{"aaa", 1, 2, 3, 4, 5, 6, 7, 8, 9},
		//	},
		//	sql: `insert into t1 (F1,F2,F3,F4,F5,F6,F7,F8,F9,F10)` +
		//		` values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		//	args: []interface{}{"aaa", 1, 2, 3, 4, 5, 6, 7, 8, 9},
		//},
	}

	run := func(c testCase) {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			if c.before != nil {
				c.before()
			}

			q, args := BuildUpsert(c.opts)
			if q != c.sql {
				t.Fatalf("SQL mismatch: `%s` != `%s`", q, c.sql)
			}
			if q != c.sql {
				t.Fatalf("argument list mismatch: `%+v` != `%+v`", args, c.args)
			}

			if c.after != nil {
				c.after()
			}
		})
	}

	for i := range cases {
		run(cases[i])
	}
}
