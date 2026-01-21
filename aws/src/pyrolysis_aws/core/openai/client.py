from __future__ import annotations

import logging
from dataclasses import asdict
from enum import Enum

from pydantic import BaseModel, Field, ValidationError
from pydantic_ai import Agent, BinaryContent
from pydantic_ai.exceptions import UnexpectedModelBehavior
from pydantic_ai.models.openai import OpenAIResponsesModel
from pydantic_ai.providers.openai import OpenAIProvider

from ..aws_clients.secrets import get_secret_json

_logger = logging.getLogger(__name__)

MODEL_NAME = "gpt-5-mini"


class AnalysisStatus(str, Enum):
    """Classification result from the vision model."""

    YES = "YES"
    NO = "NO"
    MAYBE = "MAYBE"


class AgentOutput(BaseModel):
    """Structured output from the pyrolysis detection agent."""

    status: AnalysisStatus = Field(..., description="Model classification")
    reasoning: str = Field(..., description="Explanation for the classification")


class AnalysisError(Exception):
    """Raised when image analysis fails."""

    def __init__(self, message: str, cause: Exception | None = None) -> None:
        self.cause = cause
        super().__init__(message)


def _make_agent(api_key: str) -> Agent[None, AgentOutput]:
    """
    Build a PydanticAI Agent for pyrolysis detection.

    Uses OpenAI Responses API with structured output validation.
    """
    model_settings = {"openai_reasoning_effort": "low"}

    model = OpenAIResponsesModel(
        model_name=MODEL_NAME,
        settings=model_settings,
        provider=OpenAIProvider(api_key=api_key),
    )

    instructions = """
        You are a vision model trained to identify tyre pyrolysis activity from satellite imagery.

        Return ONLY a JSON object matching this exact schema:
        {
            "status": "YES" | "NO" | "MAYBE",
            "reasoning": "<brief explanation>"
        }

        Decision guide:
        - **YES** → Black soot, piles of tyres, or obvious burning/pyrolysis signs are visible.
        - **MAYBE** → The image shows some kind of industrial area or infrastructure but no clear tyres or soot.
        - **NO** → No signs of an industry, tyres, or soot are visible (e.g., open land, fields, or roads, or residential houses).

        Be conservative with YES — choose MAYBE when uncertain.
        Keep reasoning short and factual, focusing on what visual cues led to your decision.
        """

    return Agent(
        model,
        output_type=AgentOutput,
        instructions=instructions,
    )


async def analyze_image(
    image_bytes: bytes,
    *,
    secrets_id: str,
    agent: Agent[None, AgentOutput] | None = None,
) -> tuple[AgentOutput, dict]:
    """
    Analyze satellite imagery for pyrolysis activity.

    Args:
        image_bytes: Image data (PNG or JPEG)
        secrets_id: Secrets Manager secret ID containing OPENAI_API_KEY
        agent: Optional pre-configured agent (for reuse across calls)

    Returns:
        Tuple of (AgentOutput, usage_dict) where usage_dict contains
        token usage and model info.

    Raises:
        AnalysisError: On validation failure or LLM invocation error
    """
    if agent is None:
        secrets = get_secret_json(secrets_id)
        api_key = secrets.get("OPENAI_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY not found in secrets")
        agent = _make_agent(api_key)

    try:
        run = await agent.run(
            [
                "Decide if this image shows tyre pyrolysis activity:",
                BinaryContent(image_bytes, media_type="image/png"),
            ]
        )

        output: AgentOutput = run.output
        usage_dict = asdict(run.usage())
        usage_dict["model"] = MODEL_NAME

        return output, usage_dict

    except ValidationError as exc:
        _logger.error("Structured output failed validation: %s", exc)
        raise AnalysisError("Agent response failed validation", exc) from exc

    except UnexpectedModelBehavior as exc:
        _logger.exception("LLM call failed: %s", exc)
        raise AnalysisError("LLM invocation failed", exc) from exc


__all__ = [
    "AnalysisStatus",
    "AgentOutput",
    "AnalysisError",
    "analyze_image",
]
