from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import ALLOW_ORIGINS
from .routes import health, notion_compat
from .routes import bik_pdf  # jeśli używasz /bik/pdf-to-xls

app = FastAPI(title="BIK → XLS Notion Bridge", version="1.0.0")

# CORS
origins = [o.strip() for o in (ALLOW_ORIGINS or "*").split(",")]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins if origins != ["*"] else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routery
app.include_router(health.router)
app.include_router(notion_compat.router)
app.include_router(bik_pdf.router)  # jeśli endpoint jest w użyciu
