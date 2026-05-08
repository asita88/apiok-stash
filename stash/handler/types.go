package handler

type Writer interface {
	Write(index, val string) error
}

type Indexer interface {
	GetIndex(m map[string]interface{}) string
}
