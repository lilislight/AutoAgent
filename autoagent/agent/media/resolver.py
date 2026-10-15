"""Explicit inline or upload resolution of application-owned media assets."""

import asyncio


class AssetMediaResolver:
    """Pass directly as a Provider's media_resolver; resources remain caller-owned."""

    def __init__(self, store, *, files=None):
        self.store = store
        self.files = files

    async def __call__(self, source):
        asset = await asyncio.to_thread(self.store.get, source)
        if self.files is None:
            return asset.inline()
        return await self.files.aupload(asset.data, filename=asset.filename, mime_type=asset.source.mime_type)


__all__ = ["AssetMediaResolver"]
