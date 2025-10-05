import httpx
from app.config import NOTION_TOKEN, NOTION_VERSION

BASE_URL = "https://api.notion.com/v1"

class _NotionSection:
    def __init__(self, client):
        self._client = client

class Pages(_NotionSection):
    async def retrieve(self, page_id: str):
        return await self._client._request("GET", f"/pages/{page_id}")

    async def update(self, page_id: str, properties: dict):
        body = {"properties": properties}
        return await self._client._request("PATCH", f"/pages/{page_id}", json=body)

    async def create(self, parent: dict, properties: dict):
        body = {"parent": parent, "properties": properties}
        return await self._client._request("POST", "/pages", json=body)

class Databases(_NotionSection):
    async def create(self, parent: dict, title: list, properties: dict):
        body = {"parent": parent, "title": title, "properties": properties}
        return await self._client._request("POST", "/databases", json=body)

class NotionClient:
    def __init__(self, token: str, version: str):
        self._token = token
        self._version = version
        self.pages = Pages(self)
        self.databases = Databases(self)

    async def _request(self, method: str, path: str, json: dict | None = None, params: dict | None = None):
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Notion-Version": self._version,
            "Content-Type": "application/json",
        }
        async with httpx.AsyncClient(timeout=40) as client:
            r = await client.request(method, BASE_URL + path, headers=headers, json=json, params=params)
            r.raise_for_status()
            if r.content and r.headers.get("content-type", "").startswith("application/json"):
                return r.json()
            return {}

notion = NotionClient(NOTION_TOKEN, NOTION_VERSION)
