package pg_util

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// Options for building upsert statement
type UpsertOpts struct {
	// Table to upsert into
	Table string

	// Struct that will have all its public fields written to the database.
	//
	// Use `db:"name"` to override the default name of a column.
	//
	// Tags with ",string" after the name will be converted to a string before
	// being passed to the driver. This is useful in some cases like encoding to
	// Postgres domains. This also works, if the name part of the tag is empty.
	// Examples: `db:"name,string"` `db:",string"`
	//
	// Fields with a `db:"-"` tag will be skipped
	//
	// Fields with `db:"unique"` will be set as such including primary key(s)
	// This will only work if all unique fields are primary keys
	// This won't work if primary key is auto-generated
	// If you have unique non pkey field(s) then make a unique constrain and set it into the Constrain option
	//
	// First the fields in struct itself are scanned and then the fields in any
	// embedded structs using depth first search.
	// If duplicate column names (from the struct field name or `db` struct tag)
	// exist, the first found value will be used.
	Data interface{}

	// Optional prefix to statement
	Prefix string

	// Optional suffix to statement
	Suffix string

	// Optional contrain define uniqueness between any non pkey unique field and pkey field(s)
	Constrain string
}

// Build and cache upsert statement for all fields of data. This includes
// embedded struct fields.
//
// See UpsertOpts for further documentation.
func BuildUpsert(o UpsertOpts) (sql string, args []interface{}) {
	rootT := reflect.TypeOf(o.Data)
	k := Data{
		table:     o.Table,
		prefix:    o.Prefix,
		suffix:    o.Suffix,
		constrain: o.Constrain,
		typ:       rootT,
	}
	_sql, cached := upsertCache.Load(k)
	if cached {
		sql = _sql.(string)
	}

	var (
		w        strings.Builder
		dedupMap = dedupMapPool.Get().(map[string]struct{})
	)
	defer func() {
		for k := range dedupMap {
			delete(dedupMap, k)
		}
		dedupMapPool.Put(dedupMap)
	}()
	if !cached {
		if o.Prefix != "" {
			w.WriteString(o.Prefix)
			w.WriteByte(' ')
		}
		fmt.Fprintf(&w, "insert into %s (", o.Table)
	}
	hasContrain := len(o.Constrain) != 0
	args, uniqueFields, s := ScanStruct(reflect.ValueOf(o.Data), rootT, dedupMap, cached, hasContrain)
	w.WriteString(s)
	if !cached {
		w.WriteString(") values (")
		var tmp []byte
		for i := 0; i < len(dedupMap); i++ {
			if i != 0 {
				w.WriteByte(',')
			}
			w.WriteByte('$')
			if i < 9 {
				w.WriteByte(byte(i) + '0' + 1) //  What the fuck???
			} else {
				tmp = strconv.AppendUint(tmp[:0], uint64(i+1), 10)
				w.Write(tmp)
			}
		}
		w.WriteString(") on conflict (")
		if hasContrain {
			w.WriteString(o.Constrain)
		} else {
			for i := 0; i < len(uniqueFields); i++ {
				if i != 0 {
					w.WriteByte(',')
				}
				w.WriteString(uniqueFields[i])
			}
		}
		w.WriteString(") do update set ")

		for name, _ := range dedupMap {
			fmt.Fprintf(&w, "%[1]s = excluded.%[1]s,", name)
		}
		sql = strings.TrimSuffix(w.String(), ",")

		if o.Suffix != "" {
			sql = sql + " " + o.Suffix
			//w.WriteByte(' ')
			//w.WriteString(o.Suffix)
		}

		//sql = w.String()
		upsertCache.Store(k, sql)
	}

	return
}
func ScanStruct(parentV reflect.Value, parentT reflect.Type, dedupMap map[string]struct{}, cached, constrain bool) ([]interface{}, []string, string) {
	type desc struct {
		reflect.Value
		reflect.Type
	}
	var args []interface{}
	var (
		embedded     []desc
		uniqueFields []string
		w            strings.Builder
		l            = parentT.NumField()
	)
	for i := 0; i < l; i++ {
		var (
			f               = parentT.Field(i)
			name            string
			unique          bool
			tag             = f.Tag.Get("db")
			convertToString bool
		)
		if i := strings.IndexByte(tag, ','); i != -1 {
			temp := tag[i+1:]
			if j := strings.IndexByte(temp, ','); j != -1 {
				convertToString = temp[j+1:] == "string"
				tag = tag[:j]
			}
			unique = tag[i+1:] == "unique"
			tag = tag[:i]
		}
		switch tag {
		case "-":
			continue
		case "":
			name = f.Name
		case "unique":
			if constrain {
				name = f.Name
			} else {
				name = f.Name
				unique = true
			}
		default:
			name = tag
		}

		v := parentV.Field(i)
		if f.Anonymous {
			embedded = append(embedded, desc{
				v,
				f.Type,
			})
			continue
		}

		if _, ok := dedupMap[name]; ok {
			continue
		}

		if !cached {
			if len(dedupMap) != 0 {
				w.WriteByte(',')
			}
			w.WriteString(name)
		}
		dedupMap[name] = struct{}{}
		if unique {
			uniqueFields = append(uniqueFields, name)
		}
		val := v.Interface()
		if convertToString {
			val = fmt.Sprint(val)
		}
		args = append(args, val)
	}

	for _, d := range embedded {
		values, uni, s := ScanStruct(d.Value, d.Type, dedupMap, cached, constrain)
		args = append(args, values...)
		uniqueFields = append(uniqueFields, uni...)
		w.WriteString(s)
	}
	return args, uniqueFields, w.String()
}
