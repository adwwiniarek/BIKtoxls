# app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ⬇️ Importujemy moduły routerów z KATALOGU routes/ (ten obok app/)
import routes.health as health
import routes.notion_webhook as notion_webhook
import routes.bik_pdf as bik_pdf
import routes.notion_compat as notion_compat  # <- tu jest /notion/poll-one

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rejestrujemy routy
app.include_router(health.router)
app.include_router(notion_webhook.router)
app.include_router(bik_pdf.router)
app.include_router(notion_compat.router)  # <- kluczowe, bo daje /notion/poll-one
