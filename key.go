package cache

import (
	"regexp"
	"strings"
)

type KeyFunc func(string) bool

var MatchAll KeyFunc = func(string) bool { return true }

// ContainsAny return true if match any substring
var ContainsAny = func(subStrings ...string) KeyFunc {
	return func(key string) bool {
		for _, ss := range subStrings {
			if strings.Contains(key, ss) {
				return true
			}
		}
		return false
	}
}

// ContainsAll return true if match all substrings
var ContainsAll = func(subStrings ...string) KeyFunc {
	return func(key string) bool {
		for _, ss := range subStrings {
			if !strings.Contains(key, ss) {
				return false
			}
		}
		return true
	}
}

var HasPrefix = func(prefix string) KeyFunc {
	return func(key string) bool {
		return strings.HasPrefix(key, prefix)
	}
}

var HasSuffix = func(suffix string) KeyFunc {
	return func(key string) bool {
		return strings.HasSuffix(key, suffix)
	}
}

var Regex = func(pattern string) KeyFunc {
	return func(key string) bool {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return false
		}
		return re.FindString(key) != ""
	}
}

var And = func(kfs ...KeyFunc) KeyFunc {
	return func(s string) bool {
		for _, kf := range kfs {
			if !kf(s) {
				return false
			}
		}
		return true
	}
}

var Or = func(kfs ...KeyFunc) KeyFunc {
	return func(s string) bool {
		for _, kf := range kfs {
			if kf(s) {
				return true
			}
		}
		return false
	}
}
