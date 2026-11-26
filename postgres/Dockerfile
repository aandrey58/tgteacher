FROM postgres:16@sha256:07abc3f43c9395b0bdbbab36bc25ddb0e8d83cd4fcafb5fb7d6abe14ff156dd8

# Preserve the official PostgreSQL entrypoint and data directory.
USER root
RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends libzstd1 zstd \
    && dpkg --compare-versions "$(dpkg-query -W -f='${Version}' libzstd1)" ge '1.5.7+dfsg-1+deb13u1' \
    && dpkg --compare-versions "$(dpkg-query -W -f='${Version}' zstd)" ge '1.5.7+dfsg-1+deb13u1' \
    && rm -rf /var/lib/apt/lists/*
