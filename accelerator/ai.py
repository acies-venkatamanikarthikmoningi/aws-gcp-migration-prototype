"""Model abstraction (blueprint 11/16). The accelerator runs fully WITHOUT an LLM.

If a provider is configured (e.g. Gemini on Vertex AI), it may only *explain* deterministic results. Inputs are
redacted first and every call is recorded in agent_runs. The provider is never asked to produce counts, hashes,
owners, e-mail addresses or validation outcomes (guardrail: return UNKNOWN / NEEDS REVIEW instead).
"""
from __future__ import annotations

import os
from typing import Protocol

from .parsers import redact


class ExplainProvider(Protocol):
    name: str

    def explain(self, prompt: str) -> str: ...


class NullProvider:
    name = "none (deterministic rules only)"

    def explain(self, prompt: str) -> str:
        return ""


class VertexGeminiProvider:  # pragma: no cover - requires customer-approved project
    name = "vertex-ai:gemini"

    def __init__(self, project: str, location: str, model: str = "gemini-2.5-pro"):
        from google import genai  # type: ignore
        self.client = genai.Client(vertexai=True, project=project, location=location)
        self.model = model

    def explain(self, prompt: str) -> str:
        safe, _ = redact(prompt)
        r = self.client.models.generate_content(model=self.model, contents=(
            "Explain the following deterministic migration finding for an architect in <=80 words. "
            "Do not invent resources, owners, numbers or outcomes; say NEEDS REVIEW if evidence is missing.\n\n" + safe))
        return r.text or ""


def provider() -> ExplainProvider:
    if os.environ.get("ACCEL_LLM") == "vertex" and os.environ.get("GCP_PROJECT"):
        return VertexGeminiProvider(os.environ["GCP_PROJECT"], os.environ.get("GCP_LOCATION", "us-central1"))
    return NullProvider()
