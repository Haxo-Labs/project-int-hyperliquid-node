FROM debian:12.6-slim

ARG USERNAME=hluser
ARG USER_UID=10000
ARG USER_GID=$USER_UID

# Create user, install tools, and set up directory structure
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && apt-get update -y \
    && apt-get install --no-install-recommends -y curl cron procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /data/hyperliquid/{hl/data,scripts} /home/$USERNAME/hl \
    && chown -R $USERNAME:$USERNAME /data/hyperliquid

# Create symlink from /home/hluser/hl/data → /data/hyperliquid/hl/data
RUN ln -s /data/hyperliquid/hl/data /home/$USERNAME/hl/data

# Copy in prune scripts and cron config
COPY scripts/ /data/hyperliquid/scripts/
COPY cron/ /etc/cron.d/

# Set permissions
RUN chmod 0644 /etc/cron.d/* \
    && chmod +x /data/hyperliquid/scripts/* \
    && chown -R $USERNAME:$USERNAME /data/hyperliquid/scripts

# OPTIONAL: Drop privileges if your scripts support it
# USER $USERNAME

# Run cron in foreground
CMD ["bash", "-c", "crontab /etc/cron.d/prune && cron -f -L 15"]