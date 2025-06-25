FROM ubuntu:24.04

ARG USERNAME=hluser
ARG USER_UID=10000
ARG USER_GID=$USER_UID

ARG PUB_KEY_URL=https://raw.githubusercontent.com/hyperliquid-dex/node/refs/heads/main/pub_key.asc
ARG HL_VISOR_URL=https://binaries.hyperliquid.xyz/Mainnet/hl-visor
ARG HL_VISOR_ASC_URL=https://binaries.hyperliquid.xyz/Mainnet/hl-visor.asc

# Create user and install dependencies
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && apt-get update -y && apt-get install -y curl gnupg \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Create /data structure and give permission to hluser
RUN mkdir -p /data/hyperliquid/hl/data/logs \
    && mkdir -p /data/hyperliquid/tmp \
    && mkdir -p /data/hyperliquid/hyperliquid_data \
    && mkdir -p /data/hyperliquid/file_mod_time_tracker \
    && ln -s /data/hyperliquid/hl/data/logs /data/hyperliquid/hl/log \
    && chown -R $USERNAME:$USERNAME /data

# Switch to non-root user
USER $USERNAME
WORKDIR /data/hyperliquid

# Create visor config
RUN echo '{"chain": "Mainnet"}' > /data/hyperliquid/visor.json

# Import GPG public key
RUN curl -o /data/hyperliquid/pub_key.asc $PUB_KEY_URL \
    && gpg --import /data/hyperliquid/pub_key.asc

# Download and verify hl-visor binary
RUN curl -o /data/hyperliquid/hl-visor $HL_VISOR_URL \
    && curl -o /data/hyperliquid/hl-visor.asc $HL_VISOR_ASC_URL \
    && gpg --verify /data/hyperliquid/hl-visor.asc /data/hyperliquid/hl-visor \
    && chmod +x /data/hyperliquid/hl-visor

# Create /home/hluser/hl symlinks pointing to /data equivalents
RUN mkdir -p /home/$USERNAME/hl \
    && rm -rf /home/$USERNAME/hl/data \
    && ln -s /data/hyperliquid/hl/data /home/$USERNAME/hl/data \
    && ln -s /data/hyperliquid/tmp /home/$USERNAME/hl/tmp \
    && ln -s /data/hyperliquid/hyperliquid_data /home/$USERNAME/hl/hyperliquid_data \
    && ln -s /data/hyperliquid/file_mod_time_tracker /home/$USERNAME/hl/file_mod_time_tracker

EXPOSE 4000-4010

ENTRYPOINT ["/data/hyperliquid/hl-visor", "run-non-validator", "--write-trades", "--replica-cmds-style", "recent-actions"]