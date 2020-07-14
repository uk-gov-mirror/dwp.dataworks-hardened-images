# Extend the official Apache guacd image so that we can add SSL and run Snyk scanning
FROM guacamole/guacd:1.2.0

# # Remove packages which are not requires & introduce vulnerabilities
RUN apt-get remove libavcodec58 libfreerdp-client2-2 libswresample3 libwinpr2-2 libavutil56 -y

# Install netcat for healthcheck
RUN apt-get update && apt-get install -y netcat

HEALTHCHECK --interval=30s --timeout=30s --start-period=10s CMD [ "nc", "-z", "127.0.0.1", "4822" ]
