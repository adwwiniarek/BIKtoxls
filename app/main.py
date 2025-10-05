# app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import health, notion_webhook, bik_pdf  # ⟵ UWAGA: bez "bik"

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(health.router)
app.include_router(notion_webhook.router)
app.include_router(bik_pdf.router)  # ⟵ nasze PDF→XLS i kompat /notion/poll-one
