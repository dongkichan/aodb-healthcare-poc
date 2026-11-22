"""Synthetic seed data for the proof of concept.

Everything here is invented for testing: three client codes, staff handles without names,
administrative workflows, and short guidance passages. There are no patient details, no real
organizations, and no real people.
"""
import hashlib
import random
import uuid

NAMESPACE = uuid.UUID("6f1c2b7e-2f43-4d7a-9a59-0c3e2d5b8a10")


def user_uuid(handle: str) -> str:
    return str(uuid.uuid5(NAMESPACE, handle))


CLIENTS = [(1, "CL-01"), (2, "CL-02"), (3, "CL-03")]

ROLES = [
    ("staff", "Staff"),
    ("team_lead", "Team lead"),
    ("client_coordinator", "Client coordinator"),
    ("manager", "Manager"),
    ("it_custodian", "IT custodian"),
    ("dpo", "Data protection officer"),
]

# handle, role, assigned client codes
USERS = [
    ("staff-a", "staff", ["CL-01", "CL-02"]),
    ("staff-b", "staff", ["CL-01"]),
    ("staff-c", "staff", ["CL-03"]),
    ("lead-a", "team_lead", ["CL-01", "CL-02"]),
    ("lead-c", "team_lead", ["CL-03"]),
    ("coord-1", "client_coordinator", ["CL-01"]),
    ("manager-1", "manager", ["CL-01", "CL-02", "CL-03"]),
    ("custodian-1", "it_custodian", []),
    ("dpo-1", "dpo", ["CL-01", "CL-02", "CL-03"]),
]

WORKFLOWS = ["appointment_change", "referral_follow_up", "records_request"]
TARGET_HOURS = {"appointment_change": 24, "referral_follow_up": 48, "records_request": 72}

# Required fields differ by client: the same workflow needs different checks per client.
REQUIRED = {
    ("CL-01", "appointment_change"): ["client_reference", "requested_window", "callback_confirmed"],
    ("CL-02", "appointment_change"): ["client_reference", "requested_window", "preferred_channel"],
    ("CL-03", "appointment_change"): ["client_reference", "requested_window"],
    ("CL-01", "referral_follow_up"): ["client_reference", "referral_type", "follow_up_date"],
    ("CL-02", "referral_follow_up"): ["client_reference", "referral_type", "follow_up_date", "authorization_ref"],
    ("CL-03", "referral_follow_up"): ["client_reference", "referral_type", "follow_up_date"],
    ("CL-01", "records_request"): ["client_reference", "requester_type", "delivery_method", "consent_on_file"],
    ("CL-02", "records_request"): ["client_reference", "requester_type", "delivery_method"],
    ("CL-03", "records_request"): ["client_reference", "requester_type", "delivery_method"],
}

# Roles allowed to perform each workflow (graph rule; Listing-2 style MAY_PERFORM edges).
MAY_PERFORM = {
    "staff": WORKFLOWS,
    "team_lead": WORKFLOWS,
    "client_coordinator": ["appointment_change"],
}


def workflow_id(client_id: int, workflow: str) -> int:
    return client_id * 10 + WORKFLOWS.index(workflow) + 1


# Hand-written guidance passages: (section_id, client, workflow, text, refers_to).
# CL-01 s1 and CL-02 s1 are deliberately near-identical, to test that a search for one
# client never returns the other client's passage.
HANDWRITTEN = [
    ("GD-0101#s1", "CL-01", "appointment_change",
     "Before moving an appointment, confirm the callback number with the caller and record that it was confirmed.", ["GD-0101#s2"]),
    ("GD-0101#s2", "CL-01", "appointment_change",
     "Offer the first two open slots inside the requested window. If none fit, add the request to the waitlist and tell the caller.", ["GD-0101#s3"]),
    ("GD-0101#s3", "CL-01", "appointment_change",
     "Update the appointment in the client application yourself, then record the new slot reference on the work package.", []),
    ("GD-0101#s4", "CL-01", "appointment_change",
     "Same-day changes need a team lead's approval before the client application is updated.", []),
    ("GD-0102#s1", "CL-01", "referral_follow_up",
     "Check the referral type against the client's list before calling. Unknown referral types go back to the client coordinator.", ["GD-0102#s2"]),
    ("GD-0102#s2", "CL-01", "referral_follow_up",
     "Call on or before the follow-up date. After two unanswered calls, send the standard follow-up notice and close the attempt.", []),
    ("GD-0102#s3", "CL-01", "referral_follow_up",
     "Record each call attempt with its time and result. Do not copy referral notes into the work package.", []),
    ("GD-0103#s1", "CL-01", "records_request",
     "A records request needs consent on file before anything is prepared. If consent is missing, hold the request and ask the client.", ["GD-0103#s2"]),
    ("GD-0103#s2", "CL-01", "records_request",
     "Send records only by the delivery method the client approved. Never attach records to email notices.", []),
    ("GD-0103#s3", "CL-01", "records_request",
     "Log the requester type: patient, legal representative, or another provider.", []),
    ("GD-0201#s1", "CL-02", "appointment_change",
     "Before moving an appointment, confirm the callback number with the caller and note that it was confirmed.", ["GD-0201#s2"]),
    ("GD-0201#s2", "CL-02", "appointment_change",
     "Ask which contact channel the caller prefers for the confirmation: phone, text message, or the client portal.", []),
    ("GD-0201#s3", "CL-02", "appointment_change",
     "This client allows changes up to 2 hours before the slot. Later changes go to the client coordinator.", []),
    ("GD-0202#s1", "CL-02", "referral_follow_up",
     "Referrals for this client need an authorization reference before the first call. Hold the request until it arrives.", ["GD-0202#s2"]),
    ("GD-0202#s2", "CL-02", "referral_follow_up",
     "Confirm the authorization reference has not expired. Expired authorizations go back to the client.", []),
    ("GD-0202#s3", "CL-02", "referral_follow_up",
     "Make three call attempts on different days before sending the written notice.", []),
    ("GD-0203#s1", "CL-02", "records_request",
     "Check that the delivery method is one this client accepts: secure portal or registered mail.", []),
    ("GD-0203#s2", "CL-02", "records_request",
     "Requests from another provider are routed to the client's records desk instead of being prepared here.", []),
    ("GD-0301#s1", "CL-03", "appointment_change",
     "Find a new slot inside the requested window and read the new date and time back to the caller.", ["GD-0301#s2"]),
    ("GD-0301#s2", "CL-03", "appointment_change",
     "If the caller asks for a different clinic site, transfer the request to the client coordinator.", []),
    ("GD-0301#s3", "CL-03", "appointment_change",
     "Close the request once the client application shows the new slot.", []),
    ("GD-0302#s1", "CL-03", "referral_follow_up",
     "Confirm the referral type and the follow-up date with the client's referral list before calling.", ["GD-0302#s2"]),
    ("GD-0302#s2", "CL-03", "referral_follow_up",
     "If the caller declines the referral, record the decline and notify the client coordinator the same day.", []),
    ("GD-0302#s3", "CL-03", "referral_follow_up",
     "Do not reschedule a referral visit without the client's written instruction.", []),
    ("GD-0303#s1", "CL-03", "records_request",
     "Verify the requester type and the delivery method before preparing anything.", ["GD-0303#s2"]),
    ("GD-0303#s2", "CL-03", "records_request",
     "Records for this client leave only through the client's own portal; staff never send them directly.", []),
    ("GD-0303#s3", "CL-03", "records_request",
     "Mark the request complete only after the client confirms delivery.", []),
]

_TOPICS = {
    "appointment_change": ["the requested window", "the callback number", "a waitlist entry", "the new slot reference",
                           "a same-day change", "the caller's preferred time", "a cancelled slot", "the confirmation call"],
    "referral_follow_up": ["the referral type", "the follow-up date", "a missed call", "the written notice",
                           "the authorization reference", "a declined referral", "the call log", "the referral list"],
    "records_request": ["consent on file", "the delivery method", "the requester type", "the records desk",
                        "a provider request", "a partial release", "the delivery confirmation", "a missing signature"],
}
_ACTIONS = ["Check", "Confirm", "Record", "Review", "Verify", "Note", "Escalate", "Hold the request until you confirm"]
_CONDITIONS = ["before the first call", "when the caller asks", "if the client application is down",
               "before updating the client application", "after a second attempt", "when the team lead asks",
               "if the work package is missing a field", "at the end of the shift"]
_OUTCOMES = ["and note it on the work package", "and tell the caller what happens next",
             "and send the clarification to the client coordinator", "and keep the request pending",
             "and record the result with its time", "and ask a team lead to review it"]


def generated_passages(total: int) -> list:
    """Deterministic template passages that bring the corpus to `total` sections."""
    rng = random.Random(20261122)
    out = []
    i = 0
    combos = [(c, w) for _, c in CLIENTS for w in WORKFLOWS]
    while len(HANDWRITTEN) + len(out) < total:
        client, workflow = combos[i % len(combos)]
        n = i // len(combos)
        gid = f"GG-{client[-2:]}{WORKFLOWS.index(workflow) + 1:02d}-{n // 10:04d}"
        text = (f"{rng.choice(_ACTIONS)} {rng.choice(_TOPICS[workflow])} {rng.choice(_CONDITIONS)} "
                f"{rng.choice(_OUTCOMES)}.")
        out.append((f"{gid}#s{n % 10 + 1}", client, workflow, text, []))
        i += 1
    return out


def all_passages(total: int) -> list:
    return HANDWRITTEN + generated_passages(total)


def guidance_id(section_id: str) -> str:
    return section_id.split("#")[0]


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
