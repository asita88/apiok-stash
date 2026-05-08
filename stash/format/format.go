package format

import (
	"fmt"
	"strings"
	"time"

	"github.com/vjeantet/jodaTime"
)

const (
	timestampFormat = "2006-01-02T15:04:05.000Z"
	timestampKey    = "@timestamp"
	leftBrace       = '{'
	rightBrace      = '}'
	dot             = '.'
)

const (
	stateNormal = iota
	stateWrap
	stateVar
	stateDot
)

func Format(tableFormat string, loc *time.Location) func(map[string]interface{}) string {
	format, attrs, timePos := getFormat(tableFormat)
	if len(attrs) == 0 {
		return func(m map[string]interface{}) string {
			return format
		}
	}

	return func(m map[string]interface{}) string {
		var vals []interface{}
		for i, attr := range attrs {
			if i == timePos {
				vals = append(vals, jodaTime.Format(attr, getTime(m).In(loc)))
				continue
			}

			if val, ok := m[attr]; ok {
				vals = append(vals, val)
			} else {
				vals = append(vals, "")
			}
		}
		return fmt.Sprintf(format, vals...)
	}
}

func getTime(m map[string]interface{}) time.Time {
	if ti, ok := m[timestampKey]; ok {
		if ts, ok := ti.(string); ok {
			if t, err := time.Parse(timestampFormat, ts); err == nil {
				return t
			}
		}
	}
	return time.Now()
}

func getFormat(tableFormat string) (format string, attrs []string, timePos int) {
	var state = stateNormal
	var builder strings.Builder
	var keyBuf strings.Builder
	timePos = -1
	writeHolder := func() {
		if keyBuf.Len() > 0 {
			attrs = append(attrs, keyBuf.String())
			keyBuf.Reset()
			builder.WriteString("%s")
		}
	}

	for _, ch := range tableFormat {
		switch state {
		case stateNormal:
			switch ch {
			case leftBrace:
				state = stateWrap
			default:
				builder.WriteRune(ch)
			}
		case stateWrap:
			switch ch {
			case leftBrace:
				state = stateVar
			case dot:
				state = stateDot
				keyBuf.Reset()
			case rightBrace:
				state = stateNormal
				timePos = len(attrs)
				writeHolder()
			default:
				keyBuf.WriteRune(ch)
			}
		case stateVar:
			switch ch {
			case rightBrace:
				state = stateWrap
			default:
				keyBuf.WriteRune(ch)
			}
		case stateDot:
			switch ch {
			case rightBrace:
				state = stateNormal
				writeHolder()
			default:
				keyBuf.WriteRune(ch)
			}
		default:
			builder.WriteRune(ch)
		}
	}

	format = builder.String()
	return
}
