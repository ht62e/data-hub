CREATE TABLE IF NOT EXISTS equipment_logs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    occurred_at timestamptz NOT NULL,
    equipment_id text NOT NULL,
    level text NOT NULL,
    message text NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_equipment_logs_time_id ON equipment_logs (occurred_at DESC, id DESC);
INSERT INTO equipment_logs (occurred_at, equipment_id, level, message)
VALUES (now(), 'press-02', 'INFO', 'Production started');
