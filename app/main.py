from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# importy z pakietu app.routes (bo routes jest TERAZ w środku app/)
from app.routes import health, notion_webhook, notion_compat, bik_pdf

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# rejestracja tras
app.include_router(health.router)
app.include_router(notion_webhook.router)
app.include_router(notion_compat.router)  # GET /notion/poll-one
app.include_router(bik_pdf.router)        # POST /bik/pdf-to-xls
