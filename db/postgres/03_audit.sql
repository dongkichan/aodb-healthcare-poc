-- Hash chain for the append-only audit trail.
-- Each new event takes the next audit_id and the hash of the previous event under a lock on
-- one head row, so concurrent writers cannot interleave the chain. Changing, removing, or
-- reordering a stored event breaks the chain at that point, and audit_verify() reports it.
SET ROLE poc_owner;
SET search_path = poc, public;

CREATE TABLE audit_chain_head (
  id        int    PRIMARY KEY CHECK (id = 1),
  last_id   bigint NOT NULL,
  last_hash text   NOT NULL
);
INSERT INTO audit_chain_head VALUES (1, 0, repeat('0', 64));

CREATE FUNCTION audit_row_hash(e audit_event) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
  SELECT encode(sha256(convert_to(concat_ws('|',
           e.prev_hash, e.audit_id, (extract(epoch FROM e.occurred_at) * 1000000)::bigint,
           e.actor_id, e.actor_role, e.client_id, e.action, e.object_ref,
           coalesce(array_to_json(e.changed_fields)::text, '')), 'UTF8')), 'hex')
$$;

CREATE FUNCTION audit_chain() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = poc, public AS $$
DECLARE h audit_chain_head;
BEGIN
  SELECT * INTO h FROM audit_chain_head WHERE id = 1 FOR UPDATE;
  NEW.audit_id    := h.last_id + 1;
  NEW.prev_hash   := h.last_hash;
  NEW.occurred_at := clock_timestamp();
  UPDATE audit_chain_head SET last_id = NEW.audit_id, last_hash = audit_row_hash(NEW) WHERE id = 1;
  RETURN NEW;
END
$$;
CREATE TRIGGER audit_chain BEFORE INSERT ON audit_event
  FOR EACH ROW EXECUTE FUNCTION audit_chain();

-- Walks the whole chain in audit_id order. Returns ok = false and the first event whose
-- prev_hash does not match, or the head ID when the last events were removed.
-- Limit: the hash is unkeyed and the head row lives in this database, so the table owner or a
-- superuser can edit or delete events and then repair the later hashes and the head. Anchoring
-- the head hash outside the database closes that gap.
CREATE FUNCTION audit_verify() RETURNS TABLE (ok boolean, checked bigint, first_bad bigint)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = poc, public AS $$
DECLARE
  r        audit_event;
  expected text := repeat('0', 64);
  n        bigint := 0;
  h        audit_chain_head;
BEGIN
  -- Only the DPO (or a superuser session, for operations) may run the full-chain check.
  IF app_role() IS DISTINCT FROM 'dpo'
     AND NOT (SELECT rolsuper FROM pg_roles WHERE rolname = session_user) THEN
    RAISE EXCEPTION 'audit_verify is restricted to the DPO' USING ERRCODE = 'insufficient_privilege';
  END IF;
  FOR r IN SELECT * FROM audit_event ORDER BY audit_id LOOP
    n := n + 1;
    IF r.prev_hash <> expected OR r.audit_id <> n THEN
      RETURN QUERY SELECT false, n, r.audit_id; RETURN;
    END IF;
    expected := audit_row_hash(r);
  END LOOP;
  SELECT * INTO h FROM audit_chain_head WHERE id = 1;
  IF h.last_hash <> expected OR h.last_id <> n THEN
    RETURN QUERY SELECT false, n, h.last_id; RETURN;
  END IF;
  RETURN QUERY SELECT true, n, NULL::bigint;
END
$$;

RESET ROLE;

REVOKE ALL ON FUNCTION poc.audit_verify(), poc.audit_row_hash(poc.audit_event) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION poc.audit_verify() TO app_rw;
