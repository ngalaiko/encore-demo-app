CREATE TABLE jobs (
    id text PRIMARY KEY CHECK (id ~ '^[A-Za-z0-9_-]{1,64}$'),
    payload text NOT NULL CHECK (octet_length(payload) <= 4096),
    mode text NOT NULL CHECK (mode IN ('ok', 'slow', 'retry-once')),
    delay_ms integer NOT NULL CHECK (delay_ms BETWEEN 0 AND 500),
    log_count integer NOT NULL CHECK (log_count BETWEEN 0 AND 250),
    status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'processing', 'done')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    result_received boolean NOT NULL DEFAULT false
);
