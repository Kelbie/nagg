# ClickHouse for the `nostr` Railway project.
#
# Same pinned upstream tag the service ran as a bare image — this layer exists
# only to drop a config file in, because Railway image deploys have nowhere to
# mount one (the service's only volume is /var/lib/clickhouse, the data dir).
#
# Keep the tag in sync with whatever the service was last verified on. Changing
# it is a ClickHouse upgrade, not a config change — treat it as such.
FROM clickhouse/clickhouse-server:26.6.1.1193

COPY system-logs.xml /etc/clickhouse-server/config.d/system-logs.xml
