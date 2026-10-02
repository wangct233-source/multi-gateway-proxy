from __future__ import annotations

import json
from urllib.parse import urlsplit, urlunsplit

from starlette.responses import JSONResponse


class EvidenceError(Exception):
    def __init__(self, evidence):
        self.evidence = evidence


def error(status, code, message, **extra):
    return JSONResponse({"error": {"code": code, "message": message, **extra}}, status,
                        headers={"Retry-After": "5"} if status in {429, 503} else None)


def upstream_url(base: str, path: str) -> str:
    parsed = urlsplit(base)
    base_parts = parsed.path.strip("/").split("/") if parsed.path.strip("/") else []
    parts = path.strip("/").split("/")
    overlap = 0
    for length in range(1, min(len(base_parts), len(parts)) + 1):
        if base_parts[-length:] == parts[:length]:
            overlap = length
    return urlunsplit((parsed.scheme, parsed.netloc, "/" + "/".join(base_parts + parts[overlap:]), "", ""))


class GatewayAdapter:
    def __init__(self, config):
        self.config = config

    def capabilities(self):
        native = self.config.mode in {"disabled", "b-remote", "c-anthropic"}
        proxy = {"status": "evidence_required" if native else "supported",
                 "evidence": ["docs/evidence.md: explicit authorized OpenAI-compatible transport"],
                 "upstream_verified": False,
                 "missing_evidence": list(getattr(self, "missing_evidence", ("Python provider adapter and authorized upstream regression samples",))) if native else []}
        absent = {"status": "evidence_required", "evidence": [],
                  "missing_evidence": ["Reviewed protocol, authorized credentials, idempotency and server outcome samples"]}
        from app.config import boolean
        from app.tasks.checkin import EVIDENCE
        checkin = dict(absent)
        if self.config.id in EVIDENCE and boolean(self.config.prefix + "_CHECKIN_VERIFIED"):
            checkin = {"status": "supported", "evidence": EVIDENCE[self.config.id], "missing_evidence": []}
        if self.config.id == "c":
            checkin = {"status": "unsupported", "evidence": ["zcode-反代/analysis_shtu:1252-1255"],
                       "missing_evidence": ["No traditional checkin implementation in reviewed report"]}
        return {"proxy": proxy, "stream": proxy, "responses": absent, "websocket": absent,
                "checkin": checkin, "claim": absent, "activity": absent}

    def _validated_payload(self, path, payload):
        if path != "v1/chat/completions" or self.config.mode not in {"openai", "a"}:
            raise EvidenceError(self.capabilities().get("proxy", {}).get("missing_evidence") or
                                ["Protocol converter not implemented and regression-tested"])
        if not self.config.upstream_url:
            raise ValueError("Upstream URL not configured")
        if not isinstance(payload, dict) or not isinstance(payload.get("messages"), list):
            raise ValueError("messages must be an array")
        data = {k: v for k, v in payload.items() if not k.startswith("_") and k not in {"api_token", "authorization"}}
        if self.config.mode == "a":
            if payload.get("tools") is not None or payload.get("functions") is not None:
                raise EvidenceError(["A tool-pairing and Responses conversion port not yet verified"])
            path = "v2/chat/completions"
        return path, data

    def prepare(self, path, payload, lease):
        path, data = self._validated_payload(path, payload)
        headers = {"Authorization": "Bearer " + lease.token, "Content-Type": "application/json",
                   "Accept": "text/event-stream" if payload.get("stream") else "application/json",
                   "Accept-Encoding": "identity"}
        return upstream_url(self.config.upstream_url, path), headers, data
