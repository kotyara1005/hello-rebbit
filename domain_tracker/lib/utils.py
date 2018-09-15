class Closing:
    def __init__(self, coroutine):
        self._coroutine = coroutine

    async def __aenter__(self):
        self._obj = await self._coroutine
        return self._obj

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._obj.close()
