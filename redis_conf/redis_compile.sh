REDIS_HOME=[/PATH/TO/FOLDER]

mkdir -p "$REDIS_HOME"/{src,bin,conf,logs,data}

cd "$REDIS_HOME/src"

wget https://download.redis.io/redis-stable.tar.gz

tar -xzf redis-stable.tar.gz

cd "$REDIS_HOME/src/redis-stable"

make -j16 MALLOC=libc BUILD_TLS=no

make PREFIX="$REDIS_HOME" install