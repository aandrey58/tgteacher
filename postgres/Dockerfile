# Build the same upstream gosu release using a patched Go toolchain.
FROM golang:1.27.4-trixie@sha256:430985542100b871e108ccc00e8dcc5eef08ca14ee5825e98ebf563d8ffec4ee AS gosu-builder
ENV GOTOOLCHAIN=local CGO_ENABLED=0 GOBIN=/out
RUN go install -trimpath github.com/tianon/gosu@29b939aa5bd4dc588af765b8248f6e39150d2565 \
    && go version -m /out/gosu | grep -F 'go1.27.4' \
    && /out/gosu --version | grep -F 'go1.27.4' \
    && test "$(/out/gosu nobody id -u)" = "$(id -u nobody)"

FROM postgres:16@sha256:07abc3f43c9395b0bdbbab36bc25ddb0e8d83cd4fcafb5fb7d6abe14ff156dd8

# Preserve the official PostgreSQL entrypoint and data directory.
USER root
RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends libzstd1 zstd \
    && dpkg --compare-versions "$(dpkg-query -W -f='${Version}' libzstd1)" ge '1.5.7+dfsg-1+deb13u1' \
    && dpkg --compare-versions "$(dpkg-query -W -f='${Version}' zstd)" ge '1.5.7+dfsg-1+deb13u1' \
    && rm -rf /var/lib/apt/lists/*

# Replace only the bundled gosu, not the official PostgreSQL entrypoint.
COPY --from=gosu-builder --chown=0:0 --chmod=0755 /out/gosu /usr/local/bin/gosu
RUN gosu --version | grep -F 'go1.27.4' \
    && test "$(gosu postgres id -u)" = "$(id -u postgres)" \
    && test "$(gosu postgres id -g)" = "$(id -g postgres)"
