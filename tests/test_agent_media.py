"""Media retention, bounded storage and actual OpenAI Files SDK wire behavior."""

import asyncio
import tempfile
from pathlib import Path
import unittest

from autoagent.agent import (
    AssetSource, AssetMediaResolver, LocalMediaStore, MemoryMediaStore, MediaError,
    OpenAIFileClient, OpenAIProviderConfig, ProviderSource, ProviderError, media_content,
    ImageContent, AudioContent, VideoContent, FileContent,
)
from tests import test_agent_providers as provider_tests


class MediaStoreTests(unittest.TestCase):
    def test_content_identity_includes_filename_and_mime_and_is_bounded(self):
        """Reused bytes share storage only when their media interpretation is identical."""
        store = MemoryMediaStore(max_asset_bytes=4, max_total_bytes=8)
        source = store.put_bytes(b"data", mime_type="image/png", filename="a.png")
        self.assertEqual(source, store.put_bytes(b"data", mime_type="image/png", filename="a.png"))
        other = store.put_bytes(b"data", mime_type="audio/wav", filename="a.wav")
        self.assertNotEqual(source, other)
        with self.assertRaises(MediaError):
            store.put_bytes(b"other", mime_type="text/plain")
        with self.assertRaises(MediaError):
            store.put_bytes(b"a", mime_type="text/plain")
        store.delete(other)
        store.put_bytes(b"a", mime_type="text/plain")

    def test_references_cannot_replace_mime_or_escape_the_store(self):
        """Asset references cannot change the meaning or location of stored content."""
        store = MemoryMediaStore()
        source = store.put_bytes(b"data", mime_type="image/png")
        with self.assertRaises(MediaError):
            store.get(AssetSource(uri=source.uri, mime_type="application/pdf"))
        for filename in ("../file", "/file", "a\\b", "bad\nname"):
            with self.subTest(filename=filename), self.assertRaises(MediaError):
                store.put_bytes(b"data", mime_type="image/png", filename=filename)

    def test_persistent_store_reopens_assets_and_detects_content_changes(self):
        """Stored references survive restart and corrupted bytes cannot be replayed."""
        with tempfile.TemporaryDirectory() as directory:
            first = LocalMediaStore(directory)
            source = first.put_bytes(b"pdf", mime_type="application/pdf", filename="report.pdf")
            reopened = LocalMediaStore(directory)
            self.assertEqual(reopened.get(source).data, b"pdf")
            next(Path(directory).glob("*.bin")).write_bytes(b"changed")
            with self.assertRaises(MediaError):
                reopened.get(source)

    def test_file_read_is_bounded_and_mime_selects_content_container(self):
        """Explicit local imports enforce size limits and retain MIME-driven content types."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.pdf"
            path.write_bytes(b"pdf")
            store = MemoryMediaStore(max_asset_bytes=3)
            self.assertIsInstance(media_content(store.put_file(path), filename=path.name), FileContent)
            path.write_bytes(b"large")
            with self.assertRaises(MediaError):
                store.put_file(path)
        for mime, expected in (("image/png", ImageContent), ("audio/wav", AudioContent), ("video/mp4", VideoContent)):
            source = MemoryMediaStore().put_bytes(b"data", mime_type=mime)
            self.assertIsInstance(media_content(source), expected)


class MediaSDKTests(unittest.IsolatedAsyncioTestCase):
    def files(self, *, blocked=None, delete_failure=False):
        import httpx
        from openai import AsyncOpenAI
        captured = []
        uploaded = []
        async def handle(request):
            captured.append(request)
            if request.url.path.endswith("/responses"):
                return httpx.Response(200, json=provider_tests.response_body())
            if request.method == "POST":
                if blocked is not None:
                    blocked.set()
                    await asyncio.Future()
                file_id = "file-" + str(len(uploaded) + 1)
                uploaded.append(file_id)
                return httpx.Response(200, json={"id":file_id, "object":"file", "bytes":3, "created_at":1, "filename":"report.pdf", "purpose":"user_data", "status":"processed"})
            file_id = request.url.path.split("/")[-1]
            if request.method == "DELETE":
                if delete_failure:
                    return httpx.Response(500, json={"error":{"message":"failed", "type":"server_error"}})
                return httpx.Response(200, json={"id":file_id, "object":"file", "deleted":True})
            return httpx.Response(200, json={"id":file_id, "object":"file", "bytes":3, "created_at":1, "filename":"report.pdf", "purpose":"user_data", "status":"processed"})
        sdk = AsyncOpenAI(api_key="test", max_retries=0, base_url="https://files.test/v1", http_client=httpx.AsyncClient(transport=httpx.MockTransport(handle)))
        sdk._platform = "Linux"
        files = OpenAIFileClient(OpenAIProviderConfig(provider_id="test"), client=sdk)
        self.addAsyncCleanup(sdk.close)
        self.addAsyncCleanup(files.aclose)
        return files, captured, sdk

    async def test_inline_resolver_and_provider_upload_resolver_share_explicit_store(self):
        """One AssetSource can resolve inline or upload through the selected SDK client."""
        store = MemoryMediaStore()
        source = store.put_bytes(b"pdf", mime_type="application/pdf", filename="report.pdf")
        inline = await AssetMediaResolver(store)(source)
        self.assertEqual(inline.data, "cGRm")
        files, captured, _ = self.files()
        uploaded = await AssetMediaResolver(store, files=files)(source)
        self.assertEqual(uploaded.provider_id, "test")
        self.assertEqual(len(captured), 1)
        self.assertIn(b'name="purpose"', captured[0].content)
        self.assertIn(b"user_data", captured[0].content)
        self.assertIn(b'filename="report.pdf"', captured[0].content)
        self.assertIn(b"Content-Type: application/pdf", captured[0].content)

    async def test_asset_upload_flows_into_actual_responses_sdk_request(self):
        """The Provider resolves an AssetSource into the uploaded file ID on its own loop."""
        import json
        from autoagent.agent import OpenAIResponsesProvider
        files, captured, sdk = self.files()
        store = MemoryMediaStore()
        source = store.put_bytes(b"pdf", mime_type="application/pdf", filename="report.pdf")
        provider = OpenAIResponsesProvider(OpenAIProviderConfig(provider_id="test"), client=sdk,
                                           media_resolver=AssetMediaResolver(store, files=files))
        self.addAsyncCleanup(provider.aclose)
        response = await provider.ainvoke(provider_tests.request(media_content(source, filename="report.pdf")))
        self.assertEqual(response.text, "Answer")
        self.assertEqual(len(captured), 2)
        part = json.loads(captured[1].content)["input"][0]["content"][0]
        self.assertEqual(part["file_id"], "file-1")
        self.assertEqual(part["filename"], "report.pdf")
        await files.acleanup()

    async def test_concurrent_uploads_deduplicate_and_deletion_invalidates_cache(self):
        """Concurrent equivalent uploads share one file until its explicit deletion."""
        files, captured, _ = self.files()
        args = {"filename":"report.pdf", "mime_type":"application/pdf"}
        first, second = await asyncio.gather(files.aupload(b"pdf", **args), files.aupload(b"pdf", **args))
        self.assertEqual(first, second)
        self.assertEqual(len(captured), 1)
        self.assertEqual((await files.aretrieve(first))["id"], first.file_id)
        await files.adelete(first)
        self.assertNotEqual(first, await files.aupload(b"pdf", **args))
        await files.acleanup()
        self.assertFalse(files._created)

    async def test_closing_file_client_preserves_uploaded_files_and_borrowed_sdk(self):
        """Client shutdown does not delete replayable files or close borrowed transports."""
        files, captured, sdk = self.files()
        await files.aupload(b"pdf", filename="report.pdf", mime_type="application/pdf")
        await files.aclose()
        self.assertEqual(len(captured), 1)
        self.assertFalse(sdk.is_closed())

    async def test_foreign_references_are_rejected_before_network_calls(self):
        """Files from another configured provider cannot be inspected or deleted."""
        files, captured, _ = self.files()
        source = ProviderSource(provider_id="other", file_id="file-other")
        for action in (files.aretrieve, files.adelete):
            with self.assertRaises(MediaError):
                await action(source)
        self.assertEqual(captured, [])

    async def test_cancelled_upload_releases_admission_and_keeps_cache_empty(self):
        """Cancellation does not record an unknown file or allow premature client shutdown."""
        started = asyncio.Event()
        files, _, _ = self.files(blocked=started)
        task = asyncio.create_task(files.aupload(b"pdf", filename="report.pdf", mime_type="application/pdf"))
        await started.wait()
        with self.assertRaises(RuntimeError):
            await files.aclose()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(files._active, 0)
        self.assertFalse(files._uploads)

    async def test_cleanup_excludes_uploads_and_keeps_client_admission_until_settled(self):
        """Cleanup blocks new uploads and shutdown while its deletion is in flight."""
        from unittest.mock import patch
        files, _, sdk = self.files()
        await files.aupload(b"pdf", filename="report.pdf", mime_type="application/pdf")
        started = asyncio.Event()
        release = asyncio.Event()
        original = sdk.files.delete
        async def deleting(file_id):
            started.set()
            await release.wait()
            return await original(file_id)
        with patch.object(sdk.files, "delete", side_effect=deleting):
            task = asyncio.create_task(files.acleanup())
            await started.wait()
            try:
                with self.assertRaises(RuntimeError):
                    await files.aupload(b"pdf", filename="report.pdf", mime_type="application/pdf")
                with self.assertRaises(RuntimeError):
                    await files.aclose()
            finally:
                release.set()
                await task
        self.assertEqual(files._active, 0)
        self.assertFalse(files._created)

    async def test_failed_cleanup_retains_uploads_without_hidden_retries(self):
        """Cleanup failures retain ownership so the application can retry deliberately."""
        files, captured, _ = self.files(delete_failure=True)
        source = await files.aupload(b"pdf", filename="report.pdf", mime_type="application/pdf")
        with self.assertRaises(ExceptionGroup):
            await files.acleanup()
        self.assertIn(source.file_id, files._created)
        self.assertEqual(len(captured), 2)
