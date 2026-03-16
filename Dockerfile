# =============================================================================
# Stage 1 : Install dbt-bigquery with uv (fast, cached)
# =============================================================================
FROM ghcr.io/astral-sh/uv:0.7-python3.12-bookworm-slim AS builder

WORKDIR /build

# Install only dbt-bigquery — sqlfluff is not needed at runtime
RUN uv venv /opt/venv \
    && VIRTUAL_ENV=/opt/venv uv pip install --no-cache "dbt-bigquery>=1.9.0,<2.0.0"

# =============================================================================
# Stage 2 : Lean runtime image
# =============================================================================
FROM python:3.12-slim-bookworm

# Avoid Python buffering (important for Cloud Run logs)
ENV PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/dbt

WORKDIR /dbt

# Copy virtualenv from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy dbt project files (order: least-changing first for layer cache)
COPY packages.yml ./
RUN dbt deps

COPY dbt_project.yml profiles.yml ./
COPY macros/ macros/
COPY models/ models/
COPY snapshots/ snapshots/
COPY seeds/ seeds/
COPY analyses/ analyses/

# Entrypoint
COPY entrypoint.sh ./
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
