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

# Create /data directory and give permission to hluser
RUN mkdir -p /data/hyperliquid/hl/data/logs \
    && ln -s /data/hyperliquid/hl/data/logs /data/hyperliquid/hl/log \
    && chown -R $USERNAME:$USERNAME /data

# Switch to non-root user
USER $USERNAME
WORKDIR /data/hyperliquid

# Configure chain
RUN echo '{"chain": "Mainnet"}' > /data/hyperliquid/visor.json

# Import GPG public key
RUN curl -o /data/hyperliquid/pub_key.asc $PUB_KEY_URL \
    && gpg --import /data/hyperliquid/pub_key.asc

# Download and verify hl-visor binary
RUN curl -o /data/hyperliquid/hl-visor $HL_VISOR_URL \
    && curl -o /data/hyperliquid/hl-visor.asc $HL_VISOR_ASC_URL \
    && gpg --verify /data/hyperliquid/hl-visor.asc /data/hyperliquid/hl-visor \
    && chmod +x /data/hyperliquid/hl-visor

EXPOSE 4000-4010

ENTRYPOINT ["/data/hyperliquid/hl-visor", "run-non-validator", "--write-trades", "--replica-cmds-style", "recent-actions"]