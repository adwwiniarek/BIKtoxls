# app/notion_client.py
from __future__ import annotations
import httpx
from typing import Any, Dict, Optional, List
from .config import settings

NOTION_API = "https://api.notion.com/v1"

class NotionError(RuntimeError):
    pass

class NotionClient:
    def __init__(self, token: str | None = None, notion_version: str | None = None):
        self.token = token or settings.notion_token
        self.notion_version = notion_version or settings.notion_version
        if not self.token:
            raise NotionError("Brak NOTION_TOKEN")
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
            "Notion-Version": self.notion_version,
            "Content-Type": "application/json",
        }
        self.http = httpx.Client(timeout=30.0, headers=self.headers)

    def _handle(self, r: httpx.Response) -> Any:
        if r.status_code >= 400:
            raise NotionError(f"{r.status_code}: {r.text}")
        return r.json()

    # --- Discovery: lista źródeł dla danej bazy ---
    def list_data_sources_for_database(self, database_id: str) -> List[dict]:
        r = self.http.get(f"{NOTION_API}/databases/{database_id}")
        data = self._handle(r)
        return data.get("data_sources", []) or []

    # --- Heurystyka wyboru data source ---
    def choose_data_source(self, database_id: Optional[str], explicit_data_source_id: Optional[str]) -> str:
        if explicit_data_source_id:
            return explicit_data_source_id
        if not database_id:
            raise NotionError("Brak data_source_id oraz database_id – nie można dobrać źródła.")
        ds_list = self.list_data_sources_for_database(database_id)
        if not ds_list:
            raise NotionError(f"Baza {database_id} nie ma żadnych data_sources w tej wersji API.")
        if len(ds_list) == 1:
            return ds_list[0]["id"]
        for ds in ds_list:
            if ds.get("name", "").lower() == "primary":
                return ds["id"]
        return ds_list[0]["id"]

    # --- Pages: create (parent = data_source_id) ---
    def create_page(self, data_source_id: str, properties: Dict[str, Any], children: Optional[list]=None, icon: Optional[dict]=None, cover: Optional[dict]=None) -> Any:
        payload = {
            "parent": {"type": "data_source_id", "data_source_id": data_source_id},
            "properties": properties
        }
        if children:
            payload["children"] = children
        if icon:
            payload["icon"] = icon
        if cover:
            payload["cover"] = cover
        r = self.http.post(f"{NOTION_API}/pages", json=payload)
        return self._handle(r)

    # --- Data Sources: query ---
    def data_source_query(self, data_source_id: str, filter: Optional[dict]=None, sorts: Optional[list]=None, page_size: int=50, start_cursor: Optional[str]=None) -> Any:
        payload: Dict[str, Any] = {}
        if filter:
            payload["filter"] = filter
        if sorts:
            payload["sorts"] = sorts
        if page_size:
            payload["page_size"] = page_size
        if start_cursor:
            payload["start_cursor"] = start_cursor
        r = self.http.post(f"{NOTION_API}/data_sources/{data_source_id}/query", json=payload)
        return self._handle(r)

    # --- Search: tylko data_source ---
    def search_data_sources(self, query: str, limit: int=10) -> List[dict]:
        payload = {
            "query": query,
            "page_size": limit,
            "filter": {"value": "data_source", "property": "object"},
        }
        r = self.http.post(f"{NOTION_API}/search", json=payload)
        data = self._handle(r)
        return data.get("results", [])

    # --- Pages: update properties ---
    def update_page_properties(self, page_id: str, properties: Dict[str, Any]) -> Any:
        r = self.http.patch(f"{NOTION_API}/pages/{page_id}", json={"properties": properties})
        return self._handle(r)

    # --- Pages: get one (używane w /notion/poll-one) ---
    def get_page(self, page_id: str):
        r = self.http.get(f"{NOTION_API}/pages/{page_id}")
        return self._handle(r)
