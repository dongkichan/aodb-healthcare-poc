-- Row-level security: every query by app_rw sees only the clients assigned to the signed-in
-- staff member. The API sets app.user_id with set_config(..., true) at the start of every
-- transaction, so the setting ends with the transaction and never stays on a pooled connection.
-- With no user set, the helper returns no clients and every policy returns no rows.
SET ROLE poc_owner;
SET search_path = poc, public;

CREATE FUNCTION app_user_id() RETURNS uuid
LANGUAGE sql STABLE AS $$
  SELECT nullif(current_setting('app.user_id', true), '')::uuid
$$;

-- SECURITY DEFINER: runs as poc_owner, which owns the lookup tables, so app_rw needs no
-- direct access to staff_user or staff_client_assignment.
CREATE FUNCTION app_role() RETURNS text
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = poc, public AS $$
  SELECT role_code FROM staff_user WHERE user_id = app_user_id()
$$;

CREATE FUNCTION app_user_clients() RETURNS int[]
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = poc, public AS $$
  SELECT coalesce(array_agg(client_id ORDER BY client_id), '{}')
  FROM staff_client_assignment
  WHERE user_id = app_user_id()
    AND valid_from <= now()
    AND (valid_to IS NULL OR valid_to > now())
$$;

-- The client codes for the gateway session (cached in Redis as session:{token}).
CREATE FUNCTION app_user_client_codes() RETURNS text[]
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = poc, public AS $$
  SELECT coalesce(array_agg(c.name_code ORDER BY c.name_code), '{}')
  FROM client c WHERE c.client_id = ANY (app_user_clients())
$$;

-- Stands in for the identity provider's subject lookup: maps a sign-in handle to a user ID.
CREATE FUNCTION user_id_for_handle(h text) RETURNS uuid
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = poc, public AS $$
  SELECT user_id FROM staff_user WHERE handle = h
$$;

-- Staff handle for display in the audit trail (no other staff details).
CREATE FUNCTION handle_of(u uuid) RETURNS text
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = poc, public AS $$
  SELECT handle FROM staff_user WHERE user_id = u
$$;

-- One policy per table that holds client work. (SELECT ...) makes the planner evaluate the
-- helper once per query instead of once per row.
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['client', 'workflow_type', 'request', 'request_event', 'review_decision',
                           'client_action', 'outbox_event', 'guidance_chunk'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('CREATE POLICY assigned_clients ON %I USING (client_id = ANY ((SELECT app_user_clients())::int[])) '
                   'WITH CHECK (client_id = ANY ((SELECT app_user_clients())::int[]))', t);
  END LOOP;
END $$;

-- Audit trail: anyone acting may append an event in their own name; only team leads and
-- managers (their assigned clients) and the DPO (all clients) may read it.
ALTER TABLE audit_event ENABLE ROW LEVEL SECURITY;
CREATE POLICY audit_append ON audit_event FOR INSERT
  WITH CHECK (actor_id = app_user_id() AND actor_role = (SELECT app_role())
              AND client_id = ANY ((SELECT app_user_clients())::int[]));
CREATE POLICY audit_read ON audit_event FOR SELECT
  USING ((SELECT app_role()) = 'dpo'
         OR ((SELECT app_role()) IN ('manager', 'team_lead') AND client_id = ANY ((SELECT app_user_clients())::int[])));

RESET ROLE;

-- Privileges for the API login. No UPDATE or DELETE on the audit trail, and no direct access
-- to users, roles, or assignments.
GRANT USAGE ON SCHEMA poc TO app_rw;
GRANT SELECT ON poc.client, poc.workflow_type, poc.guidance_chunk TO app_rw;
GRANT SELECT, INSERT, UPDATE (status, outcome) ON poc.request TO app_rw;
GRANT SELECT, INSERT ON poc.request_event, poc.review_decision, poc.client_action TO app_rw;
GRANT SELECT, INSERT ON poc.outbox_event TO app_rw;
GRANT SELECT, INSERT ON poc.audit_event TO app_rw;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA poc TO app_rw;

REVOKE ALL ON FUNCTION poc.app_user_id(), poc.app_role(), poc.app_user_clients(), poc.app_user_client_codes(),
  poc.user_id_for_handle(text), poc.handle_of(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION poc.app_user_id(), poc.app_role(), poc.app_user_clients(), poc.app_user_client_codes(),
  poc.user_id_for_handle(text), poc.handle_of(uuid) TO app_rw;

-- The outbox relay and the projections work across clients. Instead of BYPASSRLS, worker_rw gets
-- its own read policies, and the only value it may change is outbox_event.published_at.
SET ROLE poc_owner;
SET search_path = poc, public;
CREATE POLICY worker_relay ON outbox_event TO worker_rw USING (true) WITH CHECK (true);
CREATE POLICY worker_read ON request FOR SELECT TO worker_rw USING (true);
CREATE POLICY worker_read ON client FOR SELECT TO worker_rw USING (true);
CREATE POLICY worker_read ON workflow_type FOR SELECT TO worker_rw USING (true);
CREATE POLICY worker_read ON guidance_chunk FOR SELECT TO worker_rw USING (true);
RESET ROLE;
GRANT USAGE ON SCHEMA poc TO worker_rw;
-- Column-level read on request: the worker never sees summaries, client references, or outcomes.
GRANT SELECT (request_id, client_id, workflow_type_id, status, due_at) ON poc.request TO worker_rw;
GRANT SELECT ON poc.outbox_event, poc.client, poc.workflow_type, poc.guidance_chunk TO worker_rw;
GRANT UPDATE (published_at) ON poc.outbox_event TO worker_rw;
