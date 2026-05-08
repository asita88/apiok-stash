package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/kevwan/go-stash/stash/config"
	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/logx"

	_ "github.com/go-sql-driver/mysql"
)

type (
	Writer struct {
		db       *sql.DB
		columns  []string
		legacy   bool
		inserter *executors.ChunkExecutor
	}

	valueWithTable struct {
		table string
		val   string
	}
)

func NewWriter(db *sql.DB, c config.MySQLConf) (*Writer, error) {
	cols := nonEmptyInsertColumns(c)
	legacy := len(cols) == 0
	writer := &Writer{db: db, columns: cols, legacy: legacy}
	writer.inserter = executors.NewChunkExecutor(writer.execute, executors.WithChunkBytes(c.MaxChunkBytes))
	return writer, nil
}

func (w *Writer) Write(table, val string) error {
	return w.inserter.Add(valueWithTable{
		table: table,
		val:   val,
	}, len(val))
}

func (w *Writer) execute(vals []interface{}) {
	tableRows := make(map[string][]string)
	for _, val := range vals {
		pair := val.(valueWithTable)
		tableRows[pair.table] = append(tableRows[pair.table], pair.val)
	}

	for table, rows := range tableRows {
		if err := w.bulkInsert(context.Background(), table, rows); err != nil {
			logx.Error(err)
		}
	}
}

func nginxPayload(m map[string]interface{}) map[string]interface{} {
	d, ok := m["data"]
	if !ok {
		return m
	}
	if s, ok := d.(string); ok && s != "" {
		var inner map[string]interface{}
		if err := jsoniter.Unmarshal([]byte(s), &inner); err == nil {
			return inner
		}
	}
	if inner, ok := d.(map[string]interface{}); ok {
		return inner
	}
	return m
}

func cellValue(m map[string]interface{}, key string) interface{} {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return x
	case bool:
		if x {
			return int64(1)
		}
		return int64(0)
	default:
		return fmt.Sprint(x)
	}
}

func parseRowValues(columns []string, rowJSON string) ([]interface{}, error) {
	var outer map[string]interface{}
	if err := jsoniter.Unmarshal([]byte(rowJSON), &outer); err != nil {
		return nil, err
	}
	m := nginxPayload(outer)
	vals := make([]interface{}, len(columns))
	for i, col := range columns {
		vals[i] = cellValue(m, col)
	}
	return vals, nil
}

func (w *Writer) bulkInsert(ctx context.Context, table string, rows []string) error {
	if len(rows) == 0 {
		return nil
	}

	if w.legacy {
		return w.bulkInsertLegacy(ctx, table, rows)
	}

	ncols := len(w.columns)
	if ncols == 0 {
		return nil
	}

	tb := quoteIdent(table)
	colParts := make([]string, ncols)
	for i, c := range w.columns {
		colParts[i] = quoteIdent(c)
	}

	rowTpl := "(" + strings.TrimSuffix(strings.Repeat("?,", ncols), ",") + ")"
	valFragments := make([]string, len(rows))
	args := make([]interface{}, 0, len(rows)*ncols)
	for i, row := range rows {
		vals, err := parseRowValues(w.columns, row)
		if err != nil {
			return err
		}
		valFragments[i] = rowTpl
		args = append(args, vals...)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		tb,
		strings.Join(colParts, ","),
		strings.Join(valFragments, ","),
	)

	_, err := w.db.ExecContext(ctx, query, args...)
	return err
}

func (w *Writer) bulkInsertLegacy(ctx context.Context, table string, rows []string) error {
	tb := quoteIdent(table)
	rowTpl := "(?)"
	parts := make([]string, len(rows))
	args := make([]interface{}, len(rows))
	for i, row := range rows {
		parts[i] = rowTpl
		args[i] = row
	}
	query := fmt.Sprintf("INSERT INTO %s (`data`) VALUES %s", tb, strings.Join(parts, ","))
	_, err := w.db.ExecContext(ctx, query, args...)
	return err
}
