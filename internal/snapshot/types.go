package snapshot

type ChunkInfo struct {
	Database    string                   `json:"database"`
	Table       string                   `json:"table"`
	ChunkIndex  int                      `json:"chunk_index"`
	StartOffset int64                    `json:"start_offset"`
	EndOffset   int64                    `json:"end_offset"`
	WhereClause string                   `json:"where_clause,omitempty"`
	OrderBy     string                   `json:"order_by,omitempty"`
	Data        []map[string]interface{} `json:"data,omitempty"`
}
