# syntax=docker/dockerfile:1
FROM kestra/kestra:latest

USER root

# Our flows' shell.Commands tasks shell out to `docker compose ...` against the host's Docker
# daemon via the mounted /var/run/docker.sock. The base image has neither the docker CLI nor,
# just as importantly, permission for the non-root "kestra" user (UID 1000) to access the socket
# (it's root:root-owned with 660 perms on the host, so a non-root/non-root-group user can't
# reach it). Installing the CLI and staying root sidesteps both problems - acceptable for this
# local dev setup; not a hardening-appropriate choice for a shared/production deployment.
# docker-buildx is required too: every Dockerfile in this project uses BuildKit-only syntax
# (`RUN --mount=type=cache,...`), which always worked from the host because Docker Desktop
# enables BuildKit by default there - but without the buildx plugin, this container's docker
# CLI silently falls back to the legacy builder, which doesn't understand --mount at all and
# fails partway through every build.
RUN apt-get update && \
    apt-get install -y --no-install-recommends docker.io docker-buildx curl && \
    rm -rf /var/lib/apt/lists/*

# Ubuntu's docker.io package installs the base `docker` CLI but not the `compose` (v2) plugin -
# without it, `docker compose ...` fails with a confusing "unknown shorthand flag: 'f'" because
# the root docker command doesn't recognize "compose" as a subcommand and tries to parse the
# rest of the args as its own flags. Installing the plugin binary directly fixes this.
RUN mkdir -p /usr/local/lib/docker/cli-plugins && \
    curl -fsSL -o /usr/local/lib/docker/cli-plugins/docker-compose \
      https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 && \
    chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
