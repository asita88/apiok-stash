package filter

import (
	"strings"
)

func Ip2RegionFilter(searcher Ip2RegionSearcher, field, target string) FilterFunc {
	if searcher == nil {
		return func(m map[string]interface{}) map[string]interface{} { return m }
	}

	prefix := target
	if prefix != "" && !strings.HasSuffix(prefix, "_") {
		prefix += "_"
	}

	return func(m map[string]interface{}) map[string]interface{} {
		val, ok := m[field]
		if !ok {
			return m
		}

		ip, ok := val.(string)
		if !ok {
			return m
		}

		if idx := strings.Index(ip, ":"); idx > 0 {
			ip = ip[:idx]
		}

		region, err := searcher.SearchByStr(ip)
		if err != nil || region == "" {
			return m
		}

		parts := strings.SplitN(region, "|", 5)
		if len(parts) >= 1 && parts[0] != "" {
			m[prefix+"country"] = parts[0]
		}
		if len(parts) >= 2 && parts[1] != "" {
			m[prefix+"province"] = parts[1]
		}
		if len(parts) >= 3 && parts[2] != "" {
			m[prefix+"city"] = parts[2]
		}
		if len(parts) >= 4 && parts[3] != "" {
			m[prefix+"isp"] = parts[3]
		}
		if len(parts) >= 5 && parts[4] != "" {
			m[prefix+"country_code"] = parts[4]
		}

		return m
	}
}
