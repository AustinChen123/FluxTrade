"""Durable request evidence and one-way lifecycle; not admin/DDL protection."""

from alembic import op

revision = "d95a2b73e608"
down_revision = "c84f1a92d607"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE strategy_profile_activation_request (
            request_id VARCHAR(64) COLLATE "C" NOT NULL PRIMARY KEY,
            environment VARCHAR(128) COLLATE "C" NOT NULL,
            execution_scope_id VARCHAR(128) COLLATE "C" NOT NULL,
            strategy_id VARCHAR(128) COLLATE "C" NOT NULL REFERENCES strategy(id),
            expected_state_version INTEGER NOT NULL,
            canonical_payload BYTEA NOT NULL,
            payload_digest VARCHAR(64) COLLATE "C" NOT NULL,
            contract_version INTEGER NOT NULL,
            requested_at TIMESTAMPTZ NOT NULL DEFAULT date_trunc('milliseconds', clock_timestamp()),
            status VARCHAR(16) COLLATE "C" NOT NULL,
            terminal_at TIMESTAMPTZ,
            terminal_reason VARCHAR(128) COLLATE "C",
            CONSTRAINT ck_spar_request_id CHECK (request_id COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spar_payload_digest CHECK (payload_digest COLLATE "C" ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_spar_environment CHECK (environment COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_spar_execution_scope_id CHECK (execution_scope_id COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_spar_strategy_id CHECK (strategy_id COLLATE "C" ~ '^[A-Za-z0-9_-]{1,128}$'),
            CONSTRAINT ck_spar_version CHECK (expected_state_version BETWEEN 0 AND 2147483647),
            CONSTRAINT ck_spar_contract CHECK (contract_version = 1),
            CONSTRAINT ck_spar_payload CHECK (octet_length(canonical_payload) BETWEEN 1 AND 65536),
            CONSTRAINT ck_spar_status CHECK (status IN ('PENDING', 'CONSUMED', 'CANCELLED', 'STALE')),
            CONSTRAINT ck_spar_terminal CHECK ((status = 'PENDING' AND terminal_at IS NULL AND terminal_reason IS NULL) OR (status <> 'PENDING' AND terminal_at IS NOT NULL AND terminal_reason IS NOT NULL)),
            CONSTRAINT ck_spar_reason CHECK (terminal_reason COLLATE "C" ~ '^[A-Z][A-Z0-9_]{0,127}$'),
            CONSTRAINT ck_spar_requested CHECK (requested_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00' AND requested_at < TIMESTAMPTZ '10000-01-01 00:00:00+00' AND requested_at = date_trunc('milliseconds', requested_at)),
            CONSTRAINT ck_spar_terminal_time CHECK (terminal_at >= requested_at AND terminal_at >= TIMESTAMPTZ '0001-01-01 00:00:00+00' AND terminal_at < TIMESTAMPTZ '10000-01-01 00:00:00+00' AND terminal_at = date_trunc('milliseconds', terminal_at))
        )
    """)
    op.execute("""
        CREATE UNIQUE INDEX uq_spar_pending ON strategy_profile_activation_request
        (environment, execution_scope_id, strategy_id) WHERE status = 'PENDING'
    """)
    op.execute("""
        CREATE FUNCTION guard_profile_activation_request() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP <> 'UPDATE' THEN
                RAISE EXCEPTION 'activation request deletion forbidden' USING ERRCODE = '55000';
            END IF;
            IF OLD.status <> 'PENDING' OR NEW.status NOT IN ('CONSUMED', 'CANCELLED', 'STALE') OR
               ROW(NEW.request_id, NEW.environment, NEW.execution_scope_id, NEW.strategy_id,
                   NEW.expected_state_version, NEW.canonical_payload, NEW.payload_digest,
                   NEW.contract_version, NEW.requested_at) IS DISTINCT FROM
               ROW(OLD.request_id, OLD.environment, OLD.execution_scope_id, OLD.strategy_id,
                   OLD.expected_state_version, OLD.canonical_payload, OLD.payload_digest,
                   OLD.contract_version, OLD.requested_at) THEN
                RAISE EXCEPTION 'activation request mutation forbidden' USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END;
        $$
    """)
    op.execute("""
        CREATE TRIGGER spar_update BEFORE UPDATE ON strategy_profile_activation_request
        FOR EACH ROW EXECUTE FUNCTION guard_profile_activation_request()
    """)
    op.execute("""
        CREATE TRIGGER spar_delete BEFORE DELETE OR TRUNCATE ON strategy_profile_activation_request
        FOR EACH STATEMENT EXECUTE FUNCTION guard_profile_activation_request()
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER spar_delete ON strategy_profile_activation_request")
    op.execute("DROP TRIGGER spar_update ON strategy_profile_activation_request")
    op.execute("DROP TABLE strategy_profile_activation_request")
    op.execute("DROP FUNCTION guard_profile_activation_request()")
