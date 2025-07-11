FROM alpine:3.22

ENV LUA_VERSION=5.1.5
ENV LUAROCKS_VERSION=3.9.2
ENV STYLUA_VERSION=2.1.0

RUN set -ex \
    \
    && apk -U upgrade \
    && apk add --no-cache \
        readline \
    \
    && apk add --no-cache --virtual .build-deps \
        ca-certificates \
        openssl \
        make \
        gcc \
        libc-dev \
        readline-dev \
        ncurses-dev \
    \
    && wget --no-check-certificate -c \
        https://www.lua.org/ftp/lua-${LUA_VERSION}.tar.gz \
        -O lua.tar.gz \
    && echo "b3882111ad02ecc6b972f8c1241647905cb2e3fc  lua.tar.gz" | \
        sha1sum -c -s - \
    && tar xzf lua.tar.gz \
    \
    && cd lua-${LUA_VERSION} \
    && make -j"$(nproc)" linux \
    && make install \
    && cd .. \
    && rm -rf lua.tar.gz lua-${LUA_VERSION} \
    \
    && apk del .build-deps \
    \
    && apk add --no-cache \
        ca-certificates \
        openssl \
        wget \
    \
    && apk add --no-cache --virtual .build-deps \
        make \
        gcc \
        libc-dev \
    \
    && wget https://luarocks.github.io/luarocks/releases/luarocks-${LUAROCKS_VERSION}.tar.gz \
        -O - | tar -xzf - \
    \
    && cd luarocks-${LUAROCKS_VERSION} \
    && ./configure --with-lua=/usr/local \
    && make build \
    && make install \
    && cd .. \
    && rm -rf luarocks-${LUAROCKS_VERSION} \
    \
    && apk add gcc musl-dev build-base openssl-dev git \
    # dependency for stylua
    && apk add libc6-compat \
    && apk add --no-cache curl \
    && curl -L -o stylua.zip "https://github.com/JohnnyMorganz/StyLua/releases/download/v${STYLUA_VERSION}/stylua-linux-x86_64.zip" \
    # Extract the binary into /usr/local/bin
    && unzip stylua.zip -d /usr/local/bin/ \
    # Ensure the binary is executable
    && chmod +x /usr/local/bin/stylua \
    # Clean up the ZIP file
    && rm stylua.zip \
    # Clean
    && apk del .build-deps

# Specific to script
# ADD src/rockspec-dev.rockspec /home/app/rockspec-dev.rockspec
# RUN luarocks install --only-deps /home/app/rockspec-dev.rockspec

# CMD ["luarocks"]

ENTRYPOINT [ "tail", "-f", "/dev/null" ]