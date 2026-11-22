"""Reuse real owner/CH acceptance after abrupt exit at each durable boundary."""
import os
import pytest

from de.tests.silver.test_real_lineage_v1 import (
    test_real_benign_duplicate_preserves_silver_and_grows_lineage as run_fixture,
)

pytestmark = pytest.mark.skipif(os.environ.get('SILVER_LIVE_ACCEPTANCE') != '1',
                                reason='isolated subprocess/ClickHouse acceptance')


@pytest.mark.parametrize('boundary', ['freeze', 'insert', 'insert-fact', 'acknowledge', 'finalize'])
def test_abrupt_process_exit_resumes_exact_frozen_bundle(monkeypatch, boundary):
    run_fixture(monkeypatch, False, 'completion', crash_boundary=boundary)
