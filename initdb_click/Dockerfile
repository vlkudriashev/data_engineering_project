FROM clickhouse/clickhouse-server:23.12.3

COPY ./initdb_click/named_collections.xml /etc/clickhouse-server/config.d/named_collections.xml
COPY ./initdb_click/default_user.xml /etc/clickhouse-server/users.d/user_default.xml
