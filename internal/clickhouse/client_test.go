package clickhouse

import (
	"testing"
)

func TestInjectOnCluster(t *testing.T) {
	tests := []struct {
		name     string
		ddl      string
		cluster  string
		expected string
	}{
		{
			name:     "empty cluster returns ddl unchanged",
			ddl:      "CREATE TABLE IF NOT EXISTS `db`.`t` (`id` Int32) ENGINE = ReplacingMergeTree()",
			cluster:  "",
			expected: "CREATE TABLE IF NOT EXISTS `db`.`t` (`id` Int32) ENGINE = ReplacingMergeTree()",
		},
		{
			name:     "CREATE TABLE IF NOT EXISTS",
			ddl:      "CREATE TABLE IF NOT EXISTS `db`.`t` (\n  `id` Int32\n)\nENGINE = ReplacingMergeTree()",
			cluster:  "my_cluster",
			expected: "CREATE TABLE IF NOT EXISTS `db`.`t` ON CLUSTER 'my_cluster' (\n  `id` Int32\n)\nENGINE = ReplacingMergeTree()",
		},
		{
			name:     "CREATE TABLE without IF NOT EXISTS",
			ddl:      "CREATE TABLE `db`.`t` (`id` Int32) ENGINE = ReplacingMergeTree()",
			cluster:  "my_cluster",
			expected: "CREATE TABLE `db`.`t` ON CLUSTER 'my_cluster' (`id` Int32) ENGINE = ReplacingMergeTree()",
		},
		{
			name:     "ALTER TABLE ADD COLUMN",
			ddl:      "ALTER TABLE `db`.`t` ADD COLUMN IF NOT EXISTS `name` String",
			cluster:  "my_cluster",
			expected: "ALTER TABLE `db`.`t` ON CLUSTER 'my_cluster' ADD COLUMN IF NOT EXISTS `name` String",
		},
		{
			name:     "ALTER TABLE DROP COLUMN",
			ddl:      "ALTER TABLE `db`.`t` DROP COLUMN IF EXISTS `name`",
			cluster:  "prod",
			expected: "ALTER TABLE `db`.`t` ON CLUSTER 'prod' DROP COLUMN IF EXISTS `name`",
		},
		{
			name:     "ALTER TABLE MODIFY COLUMN",
			ddl:      "ALTER TABLE `db`.`t` MODIFY COLUMN `name` String",
			cluster:  "prod",
			expected: "ALTER TABLE `db`.`t` ON CLUSTER 'prod' MODIFY COLUMN `name` String",
		},
		{
			name:     "DROP TABLE IF EXISTS",
			ddl:      "DROP TABLE IF EXISTS `db`.`t`",
			cluster:  "my_cluster",
			expected: "DROP TABLE IF EXISTS `db`.`t` ON CLUSTER 'my_cluster'",
		},
		{
			name:     "DROP TABLE without IF EXISTS",
			ddl:      "DROP TABLE `db`.`t`",
			cluster:  "my_cluster",
			expected: "DROP TABLE `db`.`t` ON CLUSTER 'my_cluster'",
		},
		{
			name:     "already has ON CLUSTER — no double injection",
			ddl:      "CREATE TABLE IF NOT EXISTS `db`.`t` ON CLUSTER 'existing' (`id` Int32)",
			cluster:  "my_cluster",
			expected: "CREATE TABLE IF NOT EXISTS `db`.`t` ON CLUSTER 'existing' (`id` Int32)",
		},
		{
			name:     "cluster name with special characters is single-quote escaped",
			ddl:      "ALTER TABLE `db`.`t` ADD COLUMN `x` Int32",
			cluster:  "my'cluster",
			expected: "ALTER TABLE `db`.`t` ON CLUSTER 'my\\'cluster' ADD COLUMN `x` Int32",
		},
		{
			name:     "unrecognized DDL passes through unchanged",
			ddl:      "TRUNCATE TABLE `db`.`t`",
			cluster:  "my_cluster",
			expected: "TRUNCATE TABLE `db`.`t`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InjectOnCluster(tt.ddl, tt.cluster)
			if result != tt.expected {
				t.Errorf("InjectOnCluster(%q, %q)\ngot:  %q\nwant: %q", tt.ddl, tt.cluster, result, tt.expected)
			}
		})
	}
}
