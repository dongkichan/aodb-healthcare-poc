-- Relational model of the proof of concept. Synthetic data only.
-- Follows the core entities of the data architecture (requests, status events, review
-- decisions, client actions, audit events, outbox) and the vector table for guidance passages.
-- Every table that holds client work carries client_id, so row-level security can filter it.
SET ROLE poc_owner;
SET search_path = poc, public;

CREATE TABLE client (
  client_id  int  PRIMARY KEY,
  name_code  text NOT NULL UNIQUE,
  status     text NOT NULL DEFAULT 'active'
);

CREATE TABLE role (
  role_code  text PRIMARY KEY,
  name       text NOT NULL
);

CREATE TABLE staff_user (
  user_id    uuid PRIMARY KEY,
  handle     text NOT NULL UNIQUE,
  role_code  text NOT NULL REFERENCES role
);

CREATE TABLE staff_client_assignment (
  user_id    uuid        NOT NULL REFERENCES staff_user,
  client_id  int         NOT NULL REFERENCES client,
  valid_from timestamptz NOT NULL DEFAULT now(),
  valid_to   timestamptz,
  PRIMARY KEY (user_id, client_id)
);

CREATE TABLE workflow_type (
  workflow_type_id int  PRIMARY KEY,
  client_id        int  NOT NULL REFERENCES client,
  name             text NOT NULL,
  target_hours     int  NOT NULL,
  UNIQUE (client_id, name)
);

CREATE TABLE request (
  request_id       bigserial   PRIMARY KEY,
  client_id        int         NOT NULL REFERENCES client,
  workflow_type_id int         NOT NULL REFERENCES workflow_type,
  external_ref     text        NOT NULL,          -- the client's own reference number, never a patient record
  summary          text        NOT NULL,          -- synthetic request summary (Restricted)
  status           text        NOT NULL CHECK (status IN ('held', 'open', 'in_review', 'approved', 'completed')),
  owner_id         uuid        NOT NULL REFERENCES staff_user,
  outcome          text,
  received_at      timestamptz NOT NULL DEFAULT now(),
  due_at           timestamptz NOT NULL
);
-- A client reference may be open only once per client and workflow (validation rule).
CREATE UNIQUE INDEX request_open_ref ON request (client_id, workflow_type_id, external_ref)
  WHERE status <> 'completed';
CREATE INDEX request_client_status ON request (client_id, status);

CREATE TABLE request_event (
  event_id    bigserial   PRIMARY KEY,
  request_id  bigint      NOT NULL REFERENCES request,
  client_id   int         NOT NULL REFERENCES client,
  from_status text,
  to_status   text        NOT NULL,
  actor_id    uuid        NOT NULL REFERENCES staff_user,
  occurred_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE review_decision (
  decision_id bigserial   PRIMARY KEY,
  request_id  bigint      NOT NULL REFERENCES request,
  client_id   int         NOT NULL REFERENCES client,
  draft_ref   text        NOT NULL,
  reviewer_id uuid        NOT NULL REFERENCES staff_user,
  decision    text        NOT NULL CHECK (decision IN ('accepted', 'rejected')),
  reason      text        NOT NULL,
  decided_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE client_action (
  action_id       bigserial   PRIMARY KEY,
  request_id      bigint      NOT NULL REFERENCES request,
  client_id       int         NOT NULL REFERENCES client,
  idempotency_key text        NOT NULL UNIQUE,   -- a retry with the same key never runs twice
  channel         text        NOT NULL,
  status          text        NOT NULL,
  attempted_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE outbox_event (
  outbox_id    bigserial   PRIMARY KEY,
  client_id    int         NOT NULL REFERENCES client,
  aggregate_id text        NOT NULL,
  event_type   text        NOT NULL,
  payload      jsonb       NOT NULL,   -- IDs and status only
  created_at   timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);
CREATE INDEX outbox_unpublished ON outbox_event (outbox_id) WHERE published_at IS NULL;

-- Vector table: one embedding per approved guidance passage (all-MiniLM-L6-v2, 384 dimensions).
-- section_id is an addition to the published table definition: it joins a result back to
-- the passage text held in the MongoDB guidance document.
CREATE TABLE guidance_chunk (
  chunk_id      bigserial PRIMARY KEY,
  guidance_id   text NOT NULL,
  section_id    text NOT NULL,
  version       int  NOT NULL,
  client_id     int  NOT NULL REFERENCES client,
  workflow_type text NOT NULL,
  content_hash  text NOT NULL,
  embedding     vector(384) NOT NULL,
  UNIQUE (section_id, version)
);
CREATE INDEX guidance_chunk_hnsw ON guidance_chunk USING hnsw (embedding vector_cosine_ops);

-- Audit trail (append-only). audit_id and prev_hash are assigned by the trigger in 03_audit.sql.
CREATE TABLE audit_event (
  audit_id       bigint      PRIMARY KEY,
  occurred_at    timestamptz NOT NULL DEFAULT now(),
  actor_id       uuid        NOT NULL REFERENCES staff_user,
  actor_role     text        NOT NULL,
  client_id      int         NOT NULL REFERENCES client,
  action         text        NOT NULL CHECK (action IN
                   ('create', 'status_change', 'draft', 'review', 'complete', 'read_restricted', 'read_audit')),
  object_ref     text        NOT NULL,
  changed_fields text[],                      -- names of changed fields, never their values
  prev_hash      text        NOT NULL
);
CREATE INDEX audit_object ON audit_event (object_ref);

RESET ROLE;
