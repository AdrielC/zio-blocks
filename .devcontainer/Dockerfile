FROM ubuntu:24.04

ARG DEBIAN_FRONTEND=noninteractive
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=1000

ENV TZ=Etc/UTC

# System dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
       curl \
       wget \
       git \
       unzip \
       zip \
       bash \
       bash-completion \
       build-essential \
       pkg-config \
       clang \
       lld \
       llvm \
       zlib1g-dev \
       openssh-client \
       gnupg \
       locales \
       sudo \
       direnv \
       ripgrep \
       fzf \
       fd-find \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/fdfind /usr/local/bin/fd

# Node.js 20.x for Scala.js tooling
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get update \
    && apt-get install -y --no-install-recommends nodejs \
    && npm i -g yarn pnpm \
    && rm -rf /var/lib/apt/lists/*

# Locale setup
RUN sed -i 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen \
    && locale-gen \
    && update-locale LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# Create non-root user for devcontainers (idempotent if UID/GID already exist)
RUN set -eux; \
    if getent group "${USER_GID}" >/dev/null; then \
      true; \
    else \
      groupadd --gid "${USER_GID}" "${USERNAME}"; \
    fi; \
    if id -u "${USERNAME}" >/dev/null 2>&1; then \
      usermod -s /bin/bash -g "${USER_GID}" "${USERNAME}" || true; \
    else \
      if getent passwd "${USER_UID}" >/dev/null; then \
        useradd -m -s /bin/bash -g "${USER_GID}" "${USERNAME}"; \
      else \
        useradd -m -s /bin/bash -u "${USER_UID}" -g "${USER_GID}" "${USERNAME}"; \
      fi; \
    fi; \
    echo "${USERNAME} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/${USERNAME}; \
    chmod 0440 /etc/sudoers.d/${USERNAME}

# Install Coursier launcher (arch-aware)
RUN arch="$(uname -m)" \
    && case "$arch" in \
       x86_64) cs_arch=x86_64 ;; \
       aarch64|arm64) cs_arch=aarch64 ;; \
       *) echo "Unsupported architecture: $arch" && exit 1 ;; \
    esac \
    && curl -fsSL "https://github.com/coursier/launchers/raw/master/cs-${cs_arch}-pc-linux.gz" \
       | gzip -d > /usr/local/bin/cs \
    && chmod +x /usr/local/bin/cs

# Ensure coursier-installed binaries are on PATH for the dev user
ENV PATH=/home/${USERNAME}/.local/share/coursier/bin:/usr/local/bin:${PATH}
ENV COURSIER_JVM=temurin:21

# Install JVM and Scala toolchain as the non-root user
RUN su - ${USERNAME} -c "cs setup -y --jvm 21" \
    && su - ${USERNAME} -c "cs install scala scalac scalafmt scalafix sbt scala-cli ammonite bloop metals"

# Sane defaults for sbt and JVM in containers
ENV SBT_OPTS="-Xms1G -Xmx3G -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -Dsbt.log.noformat=true -Dsbt.color=false -Dsbt.server.autostart=false"
ENV JAVA_OPTS="-Xms512m -Xmx2g"

WORKDIR /workspaces/zio-blocks

# Pre-warm sbt & Coursier caches to speed up first open
COPY --chown=${USERNAME}:${USERNAME} build.sbt /tmp/zio-blocks/build.sbt
COPY --chown=${USERNAME}:${USERNAME} project /tmp/zio-blocks/project
RUN su - ${USERNAME} -c "cd /tmp/zio-blocks && sbt -batch update" || true

# Bloop default port
EXPOSE 8212

# Keep container running for editors that expect a persistent devcontainer
CMD ["sleep", "infinity"]

