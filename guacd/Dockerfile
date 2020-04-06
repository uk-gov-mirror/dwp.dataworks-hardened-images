# Extend the official Apache guacd image so that we can add SSL and run Snyk scanning
FROM guacamole/guacd:1.1.0

# Remove packages which are not requires & introduce vulnerabilities
RUN apk del ffmpeg
