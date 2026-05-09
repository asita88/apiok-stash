package filter

import (
	"strings"
	"time"
)

func ParseTimeFilter(field, timeZone string) FilterFunc {
	if field == "" {
		return func(m map[string]interface{}) map[string]interface{} { return m }
	}
	loc := time.Local
	if timeZone != "" {
		var err error
		loc, err = time.LoadLocation(timeZone)
		if err != nil {
			loc = time.Local
		}
	}
	return func(m map[string]interface{}) map[string]interface{} {
		v, ok := m[field]
		if !ok || v == nil {
			return m
		}
		s, ok := v.(string)
		if !ok {
			return m
		}
		s = strings.TrimSpace(s)
		if s == "" {
			m[field] = nil
			return m
		}
		layouts := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02T15:04:05Z0700",
			"2006-01-02 15:04:05",
		}
		var t time.Time
		var parsed bool
		for _, layout := range layouts {
			var err error
			t, err = time.Parse(layout, s)
			if err == nil {
				parsed = true
				break
			}
		}
		if !parsed {
			return m
		}
		m[field] = t.In(loc).Format("2006-01-02 15:04:05")
		return m
	}
}
