.PHONY: default
default:
	echo "Please specify a target"

.PHONY: dump
dump:
	@sqlite3 storage.sqlite3 '.mode json' 'SELECT * FROM downloads'
