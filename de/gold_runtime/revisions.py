"""Revision policy for post-close data (Gold Runtime Contract v1 §G3-P0-002).

A closed window never reopens. Data arriving after close creates ``revision_seq +
1``; the maximum revision is ``1``, after which the window is immutable and any
further late row is quarantined. An unchanged source-set hash is idempotent and a
conflicting identity is ``CONFLICTED``. Nothing is silently overwritten.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from de.gold_runtime.config import MAX_REVISION_SEQ, LateClass, WindowState
from de.gold_runtime.checkpoint_store import WindowStateRow


class RevisionAction(str, Enum):
    NO_ACTION = "NO_ACTION"
    IDEMPOTENT = "IDEMPOTENT"
    REVISE = "REVISE"
    QUARANTINE = "QUARANTINE"
    CONFLICTED = "CONFLICTED"


@dataclass(frozen=True)
class RevisionDecision:
    action: RevisionAction
    revision_seq: int
    reason: str

    @property
    def creates_batch(self) -> bool:
        return self.action is RevisionAction.REVISE


def decide_revision(
    *,
    window_state: Optional[WindowStateRow],
    observed_source_set_hash: str,
    late_class: LateClass = LateClass.ON_TIME,
    identity_conflict: bool = False,
) -> RevisionDecision:
    if identity_conflict or late_class is LateClass.CONFLICT:
        return RevisionDecision(
            RevisionAction.CONFLICTED,
            -1 if window_state is None else int(window_state.revision_seq),
            "CONFLICTING_SOURCE_IDENTITY",
        )
    if window_state is None:
        return RevisionDecision(RevisionAction.NO_ACTION, 0, "WINDOW_NOT_YET_PROCESSED")

    current_revision = int(window_state.revision_seq)
    is_closed = window_state.state in {WindowState.CLOSED.value, WindowState.REVISED.value}
    if not is_closed:
        return RevisionDecision(
            RevisionAction.NO_ACTION, current_revision, "WINDOW_NOT_CLOSED"
        )
    if (window_state.source_set_hash or "") == observed_source_set_hash:
        return RevisionDecision(
            RevisionAction.IDEMPOTENT, current_revision, "SAME_SOURCE_SET_HASH"
        )
    if current_revision >= MAX_REVISION_SEQ:
        return RevisionDecision(
            RevisionAction.QUARANTINE,
            current_revision,
            f"MAX_REVISION_{MAX_REVISION_SEQ}_REACHED",
        )
    return RevisionDecision(
        RevisionAction.REVISE, current_revision + 1, "LATE_AFTER_CLOSE"
    )


def next_revision_seq(current_revision: int) -> int:
    proposed = int(current_revision) + 1
    if proposed > MAX_REVISION_SEQ:
        raise ValueError(
            f"revision_seq {proposed} exceeds the locked maximum {MAX_REVISION_SEQ}"
        )
    return proposed


def is_immutable(window_state: Optional[WindowStateRow]) -> bool:
    if window_state is None:
        return False
    return (
        window_state.state in {WindowState.CLOSED.value, WindowState.REVISED.value}
        and int(window_state.revision_seq) >= MAX_REVISION_SEQ
    )
