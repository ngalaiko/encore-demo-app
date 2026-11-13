ALTER TABLE jobs ADD COLUMN created_at timestamptz NOT NULL DEFAULT now();

CREATE TABLE admission_budget (
    id integer PRIMARY KEY CHECK (id = 1),
    remaining integer NOT NULL CHECK (remaining BETWEEN 0 AND 128)
);

INSERT INTO admission_budget (id, remaining) VALUES (1, 128);
