from __future__ import annotations

from autoagent.evaluation import EvalCase, Evaluation, evaluators


class WeatherAssistantEvaluation(Evaluation):
    """Evaluate the public answer, not ReAct's generated internal graph."""

    async def eval_tokyo_weather_uses_verified_tool_data(
        self,
        case: EvalCase,
    ) -> None:
        await case.invoke(
            {
                "input": "What is the weather in Tokyo?",
                "mode": "stream",
            },
            evaluators=(
                evaluators.InvocationState(expected="completed"),
                evaluators.InvocationResult(
                    expected={
                        "output": {
                            "city": "Tokyo",
                            "country": "Japan",
                            "condition": "partly cloudy",
                            "temperature_celsius": 27.0,
                            "recommendation": (
                                "Carry water and a light layer."
                            ),
                            "data_source": "mock",
                        }
                    }
                ),
            ),
        )
