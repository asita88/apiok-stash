package filter

import "github.com/kevwan/go-stash/stash/config"

const (
	filterDrop         = "drop"
	filterRemoveFields = "remove_field"
	filterTransfer     = "transfer"
	filterIp2Region    = "ip2region"
	filterParseTime    = "parse_time"
	opAnd              = "and"
	opOr               = "or"
	typeContains       = "contains"
	typeMatch          = "match"
)

type FilterFunc func(map[string]interface{}) map[string]interface{}

type Ip2RegionSearcher interface {
	SearchByStr(ipStr string) (string, error)
}

func CreateFilters(p config.Cluster, ip2r Ip2RegionSearcher) []FilterFunc {
	var filters []FilterFunc

	for _, f := range p.Filters {
		switch f.Action {
		case filterDrop:
			filters = append(filters, DropFilter(f.Conditions))
		case filterRemoveFields:
			filters = append(filters, RemoveFieldFilter(f.Fields))
		case filterTransfer:
			filters = append(filters, TransferFilter(f.Field, f.Target))
		case filterIp2Region:
			filters = append(filters, Ip2RegionFilter(ip2r, f.Field, f.Target))
		case filterParseTime:
			tz := f.TimeZone
			if tz == "" {
				tz = p.Output.MySQL.TimeZone
			}
			filters = append(filters, ParseTimeFilter(f.Field, tz))
		}
	}

	return filters
}
