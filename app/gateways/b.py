from .base import GatewayAdapter


class BAdapter(GatewayAdapter):
    source_evidence = "trae-反代/analysis_shtu:284-481; src/trae_remote_client.py:create_session/stream_events"
    missing_evidence = ("Authorized Remote session/events samples", "Current model and real identity metadata", "Third-party license provenance")
