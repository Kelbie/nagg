# ClickHouse sized for a SMALL nagg deployment (NAGG_MODULES=mint).
#
# A separate profile from deploy/clickhouse, not a copy of it. That one is tuned
# for a ~26 GB database whose continuous merges stream GBs/day through the page
# cache; this one holds ~1 MB. The two want opposite things, and the production
# instance has twice been damaged by memory caps that were right for a small
# instance and wrong for it — so they stay apart on purpose.
#
# Keep the tag in sync with deploy/clickhouse: changing it is a ClickHouse
# upgrade, not a config change.
FROM clickhouse/clickhouse-server:26.6.1.1193

COPY small-instance.xml /etc/clickhouse-server/config.d/small-instance.xml
