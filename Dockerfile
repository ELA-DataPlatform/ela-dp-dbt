# =============================================================================
# Stage 1 : Build venv with dbt-bigquery (Alpine + uv for speed)
# =============================================================================
FROM python:3.12-alpine AS builder

# Build deps needed only if some wheels have to be compiled (musllinux fallbacks).
# Kept in the builder stage so they never land in the runtime image.
RUN apk add --no-cache \
        build-base \
        libffi-dev \
        openssl-dev \
        cargo

# uv: fast resolver/installer
RUN pip install --no-cache-dir uv

# Keep the container on the same dbt-bigquery release resolved in uv.lock.
# A broad <2 constraint pulled dbt-bigquery 1.12 and its experimental parser,
# which does not publish a musllinux wheel for Alpine.
ARG DBT_BIGQUERY_VERSION=1.11.1
RUN uv venv /opt/venv \
    && VIRTUAL_ENV=/opt/venv uv pip install --no-cache \
        "dbt-bigquery==${DBT_BIGQUERY_VERSION}"

# =============================================================================
# Stage 2 : Minimal Alpine runtime
# =============================================================================
FROM python:3.12-alpine

# libstdc++ is required by some google-cloud / grpc wheels compiled against libc++.
RUN apk add --no-cache libstdc++

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DBT_PROFILES_DIR=/dbt \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /dbt

COPY --from=builder /opt/venv /opt/venv

# Copy least-changing files first to maximise layer cache hits
COPY dbt_project.yml packages.yml profiles.yml ./
RUN dbt deps

COPY macros/ macros/
COPY models/ models/
COPY snapshots/ snapshots/
COPY seeds/ seeds/
COPY analyses/ analyses/

COPY entrypoint.sh ./
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
CMD ["run"]
