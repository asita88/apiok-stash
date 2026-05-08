package mysql

import (
	"strings"

	"github.com/kevwan/go-stash/stash/config"
)

func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func DefaultLegacyCreateTableSQL() string {
	return `CREATE TABLE IF NOT EXISTS ` + "`%s`" + ` (
  id bigint unsigned NOT NULL AUTO_INCREMENT,
  data json,
  created_at datetime(3) DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
}

func nonEmptyInsertColumns(c config.MySQLConf) []string {
	var out []string
	for _, s := range c.InsertColumns {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func BuildCreateTableFromColumns(cols []string) string {
	var b strings.Builder
	b.WriteString("CREATE TABLE IF NOT EXISTS `%s` (\n  id bigint unsigned NOT NULL AUTO_INCREMENT,\n")
	for _, name := range cols {
		if name == "" {
			continue
		}
		b.WriteString("  ")
		b.WriteString(quoteIdent(name))
		b.WriteString(" TEXT NULL,\n")
	}
	b.WriteString("  PRIMARY KEY (id)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
	return b.String()
}

func ResolveCreateTableSQL(c config.MySQLConf) string {
	if c.CreateTableSQL != "" {
		return c.CreateTableSQL
	}
	cols := nonEmptyInsertColumns(c)
	if len(cols) > 0 {
		return BuildCreateTableFromColumns(cols)
	}
	return DefaultLegacyCreateTableSQL()
}
