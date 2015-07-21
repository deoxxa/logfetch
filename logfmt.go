package logfetch

import (
	"github.com/kr/logfmt"
)

type logfmtHandler map[string]string

func (l *logfmtHandler) HandleLogfmt(key, val []byte) error {
	(*l)[string(key)] = string(val)
	return nil
}

func ParseLogfmt(in <-chan map[string]interface{}, key string) <-chan map[string]interface{} {
	return Map(in, func(m map[string]interface{}) map[string]interface{} {
		f, ok := m[key]
		if !ok {
			return m
		}

		s, ok := f.(string)
		if !ok {
			return m
		}

		e := make(logfmtHandler)
		if err := logfmt.Unmarshal([]byte(s), &e); err != nil {
			return m
		}

		for k, v := range e {
			m[k] = v
		}

		return m
	})
}
