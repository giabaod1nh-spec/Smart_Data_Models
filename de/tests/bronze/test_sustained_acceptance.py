import pytest

from de.tools.bronze_sustained_acceptance import completion_failures


def valid_snapshot():
    return dict(source_process_ok=True,expected_events=10,outbox_rows=10,outbox_acked=10,
        participants=['A','B'],cycles=5,raw_rows=10,raw_group_lag=0,canonical_rows=10,
        lineage_rows=10,bronze_partitions={'0':{'offset_distance':0}},pending_intents=0,
        pending_reconciliation=0,integrity_faults=0,ready=True,observed_at=101.)


def test_completion_predicate_accepts_only_complete_fresh_reconciliation():
    assert completion_failures(valid_snapshot(),source_completed_at=100.,now=101.)==[]


@pytest.mark.parametrize(('field','value','reason'),[
    ('raw_group_lag',1,'Raw consumer lag'),
    ('canonical_rows',9,'selected-run canonical output incomplete'),
    ('bronze_partitions',{},'empty Bronze partition inventory'),
    ('ready',False,'unhealthy readiness'),
    ('pending_intents',1,'pending persistence'),
    ('pending_reconciliation',1,'pending reconciliation'),
    ('integrity_faults',1,'integrity faults'),
    ('observed_at',99.,'stale pre-completion metrics'),
])
def test_completion_predicate_rejects_false_positive_states(field,value,reason):
    snapshot=valid_snapshot();snapshot[field]=value
    assert reason in completion_failures(snapshot,source_completed_at=100.,now=101.)


def test_completion_predicate_rejects_raw_lag_even_when_bronze_matches_old_safe_boundary():
    snapshot=valid_snapshot();snapshot['raw_group_lag']=5
    snapshot['bronze_partitions']={'0':{'offset_distance':0}}
    assert completion_failures(snapshot,source_completed_at=100.,now=101.)


def test_completion_predicate_enforces_deadline_before_success():
    assert 'catch-up deadline exceeded' in completion_failures(valid_snapshot(),source_completed_at=100.,now=161.)

