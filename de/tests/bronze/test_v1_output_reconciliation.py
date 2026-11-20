from types import SimpleNamespace
import pytest
from de.bronze.clickhouse_repository import BronzeClickHouseRepository
from de.bronze.config import ObservationSettings
from de.kafka_raw.v1_ledger_store import IntegrityFault


def test_observation_hash_is_verified_content_not_lookup_filter():
    expected=dict(namespace='live',event_id='E',semantic_revision=1,bronze_canonical_hash='expected')
    class Client:
        def query(self,sql,parameters):
            assert 'bronze_canonical_hash=' not in sql
            return SimpleNamespace(column_names=list(expected),result_rows=[['live','E',1,'different']])
    repo=BronzeClickHouseRepository(ObservationSettings())
    repo._client=Client()
    with pytest.raises(IntegrityFault,match='bronze_canonical_hash'):
        repo.reconcile_v1_outputs([dict(table='bronze_observations_v1',row=expected)])
