package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kevwan/go-stash/stash/config"
	"github.com/kevwan/go-stash/stash/format"
	"github.com/zeromicro/go-zero/core/lang"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/syncx"
)

type Table struct {
	db             *sql.DB
	tableFormat    func(map[string]interface{}) string
	createTableSQL string
	tables         map[string]lang.PlaceholderType
	lock           sync.RWMutex
	singleFlight   syncx.SingleFlight
}

func NewTable(db *sql.DB, tableFormat string, loc *time.Location, createTableSQL string) *Table {
	return &Table{
		db:             db,
		tableFormat:    format.Format(tableFormat, loc),
		createTableSQL: createTableSQL,
		tables:         make(map[string]lang.PlaceholderType),
		singleFlight:   syncx.NewSingleFlight(),
	}
}

func NewTableWithDB(db *sql.DB, c config.MySQLConf) (*Table, error) {
	var loc *time.Location
	var err error
	if len(c.TimeZone) > 0 {
		loc, err = time.LoadLocation(c.TimeZone)
		if err != nil {
			return nil, err
		}
	} else {
		loc = time.Local
	}
	return NewTable(db, c.Table, loc, ResolveCreateTableSQL(c)), nil
}

func (t *Table) GetIndex(m map[string]interface{}) string {
	table := t.tableFormat(m)
	t.lock.RLock()
	if _, ok := t.tables[table]; ok {
		t.lock.RUnlock()
		return table
	}

	t.lock.RUnlock()
	if err := t.ensureTable(table); err != nil {
		logx.Error(err)
	}
	return table
}

func (t *Table) ensureTable(table string) error {
	_, err := t.singleFlight.Do(table, func() (interface{}, error) {
		t.lock.Lock()
		defer t.lock.Unlock()

		if _, ok := t.tables[table]; ok {
			return nil, nil
		}

		query := fmt.Sprintf(t.createTableSQL, strings.ReplaceAll(table, "`", "``"))
		if _, err := t.db.ExecContext(context.Background(), query); err != nil {
			return nil, err
		}

		t.tables[table] = lang.Placeholder
		return nil, nil
	})
	return err
}
