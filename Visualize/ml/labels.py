"""Class labels for traffic anomaly Random Forest."""

from __future__ import annotations

LABEL_NORMAL = "NORMAL"
LABEL_CONGESTION = "CONGESTION"
LABEL_ACCIDENT = "ACCIDENT"

LABEL_NAMES = (LABEL_NORMAL, LABEL_CONGESTION, LABEL_ACCIDENT)

# Map classifier label → NGSI-ish traffic status hint
LABEL_TO_TRAFFIC_STATUS = {
    LABEL_NORMAL: "FREE_FLOW",
    LABEL_CONGESTION: "CONGESTED",
    LABEL_ACCIDENT: "CONGESTED",  # density status; cause separated via anomalyLabel
}
