#!/usr/bin/env python3

import logging
import os
from typing import Optional

import uvicorn
from fastapi import Depends, FastAPI, Form, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from .db import init_db, get_user

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

app = FastAPI()
app.add_middleware(
    SessionMiddleware,
    secret_key=os.environ.get("WEBAPP_SECRET") or os.urandom(32).hex(),
    same_site="lax",
)

if os.path.isdir(os.path.join(TEMPLATES_DIR, "..", "static")):
    from fastapi.staticfiles import StaticFiles

    app.mount(
        "/static",
        StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")),
        name="static",
    )


def current_user(request: Request) -> Optional[dict]:
    return request.session.get("auth_user")


@app.on_event("startup")
async def startup_event():
    init_db()
    os.makedirs(TEMPLATES_DIR, exist_ok=True)


@app.get("/", response_class=HTMLResponse)
async def login_page(request: Request):
    if current_user(request):
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_302_FOUND)
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/", response_class=HTMLResponse)
async def login_action(request: Request, user_id: int = Form(...)):
    user = get_user(user_id)
    if not user:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "User not found."},
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    if not user["ativado"]:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Account is disabled."},
            status_code=status.HTTP_403_FORBIDDEN,
        )

    request.session["auth_user"] = user
    logger.info("User %s (%s) signed in.", user["cod_usuario"], user["usuario"])
    return RedirectResponse(url="/dashboard", status_code=status.HTTP_302_FOUND)


def require_auth(request: Request):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)
    return user


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, user: dict = Depends(require_auth)):
    if isinstance(user, RedirectResponse):
        return user
    return templates.TemplateResponse("dashboard.html", {"request": request, "user": user})


@app.get("/logout")
async def logout(request: Request):
    request.session.pop("auth_user", None)
    return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)


def main():
    uvicorn.run(
        "src.webapp.app:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8000)),
        reload=os.environ.get("WEBAPP_RELOAD") == "1",
    )


if __name__ == "__main__":
    main()
