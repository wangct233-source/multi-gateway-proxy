from . import c_protocol
from .base import EvidenceError, GatewayAdapter, upstream_url


class CAdapter(GatewayAdapter):
    protocol = "c-anthropic"
    source_evidence = "upstream.ts:58-128; providers.ts:36-46; client-signing.ts:42-44 (LLM path unsigned)"

    def capabilities(self):
        caps = super().capabilities()
        supported = {"status": "supported", "evidence": [self.source_evidence],
                     "missing_evidence": [], "upstream_verified": False}
        caps["proxy"] = supported
        caps["stream"] = supported
        return caps

    def prepare(self, path, payload, lease):
        if self.config.mode != "c-anthropic":
            raise EvidenceError(["c-anthropic protocol requires C_UPSTREAM_MODE=c-anthropic"])
        if path != "v1/chat/completions":
            raise EvidenceError(["Only chat completions conversion is implemented"])
        if not self.config.upstream_url:
            raise ValueError("Upstream URL not configured")
        data = {k: v for k, v in (payload or {}).items()
                if not k.startswith("_") and k not in {"api_token", "authorization"}}
        body = c_protocol.openai_to_anthropic(data)
        url = upstream_url(self.config.upstream_url, "v1/messages")
        headers = c_protocol.anthropic_headers(lease.token, stream=bool(data.get("stream")))
        return url, headers, body
