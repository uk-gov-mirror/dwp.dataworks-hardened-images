# Extend the official Apache guacd image so that we can add SSL and run Snyk scanning
FROM guacamole/guacd:1.1.0

# # Remove packages which are not requires & introduce vulnerabilities
RUN apt remove libavcodec58 libfreerdp-client2-2 libswresample3 libwinpr2-2 libavutil56 -y
