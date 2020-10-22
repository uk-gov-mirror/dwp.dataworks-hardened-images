FROM alpine:3.12 as lua-openssl-builder

ENV LUA_OPENSSL_VERSION 0.7.8-0

RUN apk add g++ make openssl openssl-dev lua lua-dev git

RUN git clone --recurse --depth 1 --branch ${LUA_OPENSSL_VERSION} https://github.com/zhaozg/lua-openssl.git 

RUN cd lua-openssl && \
    make && \
    PREFIX=/opt/lua-openssl make install && \
    make clean


FROM openresty/openresty:1.17.8.2-5-alpine

RUN apk add openssl lua-dev

ADD https://github.com/cdbattags/lua-resty-jwt/archive/v0.2.0.tar.gz /lua-resty-jwt.tar.gz
ADD https://github.com/jkeys089/lua-resty-hmac/archive/v0.05.tar.gz /lua-resty-hmac.tar.gz
ADD https://github.com/iskolbin/lbase64/archive/v1.5.2.tar.gz /lua-base64.tar.gz

RUN mkdir -p /usr/local/lib/lua/5.1 && \
    tar -C /usr/local/lib/lua/5.1 -xf /lua-resty-jwt.tar.gz --strip-components=2 lua-resty-jwt-0.2.0/lib && \
    tar -C /usr/local/lib/lua/5.1 -xf /lua-resty-hmac.tar.gz --strip-components=2 lua-resty-hmac-0.05/lib && \
    tar -C /usr/local/lib/lua/5.1 -xf /lua-base64.tar.gz --strip-components=1 && \
    rm -rf /*.tar.gz 

COPY --from=lua-openssl-builder /opt/lua-openssl /usr/local/

COPY nginx.conf /etc/nginx/conf.d/nginx.conf

COPY init.lua /home/lua/init.lua
COPY authenticate.lua /home/lua/authenticate.lua

CMD ["nginx", "-c", "/etc/nginx/conf.d/nginx.conf"]
