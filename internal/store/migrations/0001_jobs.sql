CREATE TABLE jobs (
    id         TEXT PRIMARY KEY,
    type       TEXT        NOT NULL,
    payload    BYTEA       NOT NULL,
    state      TEXT        NOT NULL,
    attempt    INT         NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX jobs_state_created_at_idx ON jobs (state, created_at);

CREATE TABLE job_events (
    id         BIGSERIAL PRIMARY KEY,
    job_id     TEXT        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    state      TEXT        NOT NULL,
    message    TEXT        NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX job_events_job_id_id_idx ON job_events (job_id, id);
