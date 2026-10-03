from . import b_protocol
from .base import EvidenceError, GatewayAdapter, upstream_url


class BAdapter(GatewayAdapter):
    protocol = "b-remote"
    source_evidence = "trae_remote_client.py:162-195,808-944,947-1002; sse.py:1691-1879"

    def capabilities(self):
        caps = super().capabilities()
        supported = {"status": "supported", "evidence": [self.source_evidence],
                     "missing_evidence": [], "upstream_verified": False}
        caps["proxy"] = supported
        caps["stream"] = supported
        return caps

    def session_request(self, payload, lease):
        if self.config.mode != "b-remote":
            raise EvidenceError(["b-remote protocol requires B_UPSTREAM_MODE=b-remote"])
        if not self.config.upstream_url:
            raise ValueError("Upstream URL not configured")
        if not isinstance(payload, dict) or not isinstance(payload.get("messages"), list):
            raise ValueError("messages must be an array")
        data = {k: v for k, v in payload.items() if not k.startswith("_") and k not in {"api_token", "authorization"}}
        body = b_protocol.session_body(data, lease.token, lease.account.get("provider_account_id") or "")
        url = upstream_url(self.config.upstream_url, "chat_sessions")
        intl = "trae.ai" in self.config.upstream_url.lower() and "trae-api-cn" not in self.config.upstream_url.lower()
        return url, b_protocol.session_headers(lease.token, stream=False, intl=intl), body

    def events_request(self, lease, session_id, message_id):
        intl = "trae.ai" in self.config.upstream_url.lower() and "trae-api-cn" not in self.config.upstream_url.lower()
        url = upstream_url(self.config.upstream_url, f"chat_sessions/{session_id}/events") + f"?reply_to_message_id={message_id}"
        return url, b_protocol.session_headers(lease.token, stream=True, intl=intl)

    def prepare(self, path, payload, lease):  # pragma: no cover - proxy() 分支直接调 session_request
        raise EvidenceError(["b-remote uses the two-step session flow in main.proxy_b_remote"])
