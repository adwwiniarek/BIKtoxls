from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .config import settings
from .routes import health, bik, notion_webhook

app = FastAPI(title="Restrukturyzacja – Notion (data_sources)", version="1.0.0")

origins = [o.strip() for o in settings.allow_origins.split(",")]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(bik.router)
app.include_router(notion_webhook.router)
