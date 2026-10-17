"""Media models, persistent assets, explicit upload/cache/delete and limitations.

Run: .venv/bin/python -m examples.agent_usage_guide.media
"""

import asyncio
import base64
from tempfile import TemporaryDirectory

from autoagent.agent import (
    AssetMediaResolver, AudioContent, ChatCompletionsAdapter, DataSource,
    FileContent, ImageContent, LiteLLMChatCompletionsAdapter, LLMAudioOutput, LLMMessage,
    LLMModelProfile, LLMOutputOptions, LLMRequest, LLMUnsupportedRequest, LocalMediaStore, OpenAIFileClient,
    OpenAIResponsesProvider, ProviderSource, TextContent, URLSource, VideoContent,
)
from .support import connection, offline_http, offline_provider, show


def with_content(*parts):
    return LLMRequest(model="demo-model", input=(LLMMessage(role="user", content=parts),))


async def main():
    image = ImageContent(source=URLSource(url="https://example.com/chart.png"), detail="high")
    # Fixture bytes demonstrate transport, not image/audio/PDF quality.
    pdf = FileContent(filename="report.pdf", source=DataSource(
        mime_type="application/pdf", data=base64.b64encode(b"%PDF-1.4\nfixture").decode()))
    multimodal = with_content(TextContent(text="Explain the chart and report."), image, pdf)
    assert LLMRequest.model_validate_json(multimodal.model_dump_json()) == multimodal
    async with offline_provider() as (provider, api):
        await provider.ainvoke(multimodal)
        kinds = [p["type"] for p in api.requests[0]["body"]["input"][0]["content"]]
        assert kinds == ["input_text", "input_image", "input_file"]
        show("Responses content types", kinds)

    audio = AudioContent(source=DataSource(mime_type="audio/wav", data="YQ=="))
    encoded = ChatCompletionsAdapter().encode_request(with_content(audio), provider_id="openai")
    assert encoded["messages"][0]["content"][0]["type"] == "input_audio"
    audio_output = LLMRequest(model="demo-audio-model", input=with_content(TextContent(text="Read this aloud")).input,
        output_options=LLMOutputOptions(modalities=("text", "audio"), audio=LLMAudioOutput(voice="alloy", format="wav")))
    audio_wire = ChatCompletionsAdapter().encode_request(audio_output, provider_id="openai")
    assert audio_wire["audio"] == {"voice": "alloy", "format": "wav"}
    show("audio output configuration (encoding only)", audio_wire["audio"])
    video = VideoContent(source=URLSource(url="https://example.com/clip.mp4"))
    wire = LiteLLMChatCompletionsAdapter().encode_request(with_content(video), provider_id="litellm")
    assert wire["messages"][0]["content"][0]["type"] == "video_url"
    try:
        ChatCompletionsAdapter().encode_request(with_content(video), provider_id="openai")
    except LLMUnsupportedRequest:
        show("capability boundary", "The base OpenAI Chat adapter rejects direct video input.")
    else:
        raise AssertionError("Video requires an adapter/endpoint that supports it")

    async with offline_provider(model_profiles={"demo-model": LLMModelProfile(input_modalities=("text",))}) as (provider, api):
        try:
            await provider.ainvoke(multimodal)
        except LLMUnsupportedRequest:
            assert not api.requests
            show("explicit model profile", "A text-only profile rejects media before HTTP dispatch.")
        else:
            raise AssertionError("Expected a model capability rejection")

    with TemporaryDirectory() as directory:
        store = LocalMediaStore(directory)
        asset = store.put_bytes(b"%PDF-1.4\nfixture", mime_type="application/pdf", filename="report.pdf")
        # AssetSource stores an application reference; re-open the store after restart.
        restored = LocalMediaStore(directory)
        inline = await AssetMediaResolver(restored)(asset)
        assert isinstance(inline, DataSource)
        async with offline_http() as api:
            async with OpenAIFileClient(connection()) as files:
                resolver = AssetMediaResolver(restored, files=files)
                try:
                    uploaded = await resolver(asset)
                    assert isinstance(uploaded, ProviderSource)
                    assert await resolver(asset) == uploaded
                    assert api.uploads == 1
                    assert (await files.aretrieve(uploaded))["id"] == uploaded.file_id
                    async with OpenAIResponsesProvider(connection(), media_resolver=resolver) as provider:
                        await provider.ainvoke(with_content(FileContent(source=asset, filename="report.pdf")))
                    show("asset and uploaded reference", {"asset": asset.model_dump(), "uploaded": uploaded.model_dump()})
                finally:
                    # Closing the client does not delete remote files. Cleanup is explicit.
                    await files.acleanup()
                assert api.deletions == 1


if __name__ == "__main__":
    asyncio.run(main())
