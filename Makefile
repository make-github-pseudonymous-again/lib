.PHONY: default
default:
	echo "Please specify a target"

.PHONY: dump
dump:
	@sqlite3 storage.sqlite3 '.mode json' 'SELECT * FROM downloads'

.PHONY: count
count:
	@sqlite3 storage.sqlite3 '.mode json' 'SELECT name, SUM(count) FROM downloads GROUP BY name ORDER BY SUM(count)'
