ALTER TABLE jobs
    ADD COLUMN lease_expires_at TIMESTAMPTZ,
    ADD COLUMN leased_by        TEXT;

CREATE INDEX jobs_lease_expiry_idx
    ON jobs (lease_expires_at)
    WHERE state = 'leased';
