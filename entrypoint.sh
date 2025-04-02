#!/bin/sh

su-exec redis redis-server --daemonize yes --save "" --appendonly no

# if [ ! -d "/data/postgres/data" ]; then
#   su-exec postgres initdb -D /data/postgres/data
#   echo "host all all all trust" >> /data/postgres/data/pg_hba.conf
#   echo "listen_addresses='*'" >> /data/postgres/data/postgresql.conf
# fi

# su-exec postgres pg_ctl start -D /data/postgres/data -l /data/postgres/logfile

until redis-cli ping >/dev/null 2>&1; do sleep 0.1; done
# until pg_isready -h localhost >/dev/null 2>&1; do sleep 0.1; done

exec "$@"