#!/usr/bin/env python3

import logging
import os
from typing import Optional

import uvicorn
from fastapi import FastAPI, Form, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from .db import (
    authenticate,
    get_user,
    get_user_by_username,
    init_db,
    list_users,
    update_password,
)

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


ROLE_LABELS = {
    "viewer": "Viewer (Read-only)",
    "employee": "Employee (Add records)",
    "supervisor": "Supervisor (Remove records)",
    "admin": "Administrator (Manage credentials)",
}


def current_user(request: Request) -> Optional[dict]:
    data = request.session.get("auth_user")
    if not data:
        return None
    data["is_active"] = bool(data.get("is_active", True))
    return data


def set_flash(request: Request, message: str, category: str = "success") -> None:
    request.session["_flash"] = {"message": message, "category": category}


def consume_flash(request: Request) -> Optional[dict]:
    return request.session.pop("_flash", None)


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
async def login_action(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    user = authenticate(username, password)
    if not user:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Invalid credentials or inactive account."},
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    request.session["auth_user"] = {
        "id": user["id"],
        "username": user["username"],
        "full_name": user["full_name"],
        "role": user["role"],
        "is_active": bool(user["is_active"]),
    }
    logger.info("User %s signed in.", user["username"])
    return RedirectResponse(url="/dashboard", status_code=status.HTTP_302_FOUND)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)
    ctx = {
        "request": request,
        "user": user,
        "role_label": ROLE_LABELS.get(user["role"], user["role"]),
    }
    flash = consume_flash(request)
    if flash:
        ctx["flash"] = flash
    return templates.TemplateResponse("dashboard.html", ctx)


@app.get("/admin/users", response_class=HTMLResponse)
async def admin_users(request: Request):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)
    if user["role"] != "admin":
        set_flash(request, "Administrator access required.", "error")
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_302_FOUND)

    ctx = {
        "request": request,
        "user": user,
        "users": list(list_users()),
        "flash": consume_flash(request),
    }
    return templates.TemplateResponse("admin_users.html", ctx)


@app.post("/admin/users/reset", response_class=HTMLResponse)
async def admin_reset_password(
    request: Request,
    target_username: str = Form(...),
    new_password: str = Form(...),
    confirm_password: str = Form(...),
):
    user = current_user(request)
    if not user:
        return RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)
    if user["role"] != "admin":
        set_flash(request, "Administrator access required.", "error")
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_302_FOUND)

    target = get_user_by_username(target_username)
    if not target:
        set_flash(request, "User not found.", "error")
        return RedirectResponse(url="/admin/users", status_code=status.HTTP_302_FOUND)

    if len(new_password) < 8:
        set_flash(request, "Password must be at least 8 characters long.", "error")
        return RedirectResponse(url="/admin/users", status_code=status.HTTP_302_FOUND)

    if new_password != confirm_password:
        set_flash(request, "Passwords do not match.", "error")
        return RedirectResponse(url="/admin/users", status_code=status.HTTP_302_FOUND)

    update_password(target["id"], new_password)
    logger.info("Admin %s reset password for user %s", user["username"], target["username"])
    set_flash(request, f"Password updated for {target['username']}.", "success")
    return RedirectResponse(url="/admin/users", status_code=status.HTTP_302_FOUND)


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
