#!/usr/bin/env python3

import logging
import os
import sqlite3
from typing import Iterable, Optional

import uvicorn
from fastapi import FastAPI, Form, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from .db import (
    ALLOWED_ROLES,
    authenticate,
    create_user,
    get_user,
    get_user_by_username,
    init_db,
    list_users,
    update_password,
    update_user_role,
    update_user_status,
)
from . import iassets

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


def ensure_access(request: Request, allowed_roles: Optional[Iterable[str]] = None):
    user = current_user(request)
    if not user:
        return None, RedirectResponse(url="/", status_code=status.HTTP_302_FOUND)
    if allowed_roles and user["role"] not in allowed_roles:
        set_flash(request, "You do not have permission to view that page.", "error")
        return user, RedirectResponse(url="/dashboard", status_code=status.HTTP_302_FOUND)
    return user, None


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
    return RedirectResponse(url="/pickups", status_code=status.HTTP_302_FOUND)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    user, redirect_resp = ensure_access(
        request,
        allowed_roles=("viewer", "employee", "supervisor", "admin"),
    )
    if redirect_resp:
        return redirect_resp
    ctx = {
        "request": request,
        "user": user,
        "role_label": ROLE_LABELS.get(user["role"], user["role"]),
    }
    flash = consume_flash(request)
    if flash:
        ctx["flash"] = flash
    return templates.TemplateResponse("dashboard.html", ctx)


@app.get("/pickups", response_class=HTMLResponse)
async def pickups_overview(request: Request):
    allowed = ("viewer", "employee", "supervisor", "admin")
    user, redirect_resp = ensure_access(request, allowed_roles=allowed)
    if redirect_resp:
        return redirect_resp

    pickups = iassets.list_recent_pickups()
    flash = consume_flash(request)
    ctx = {
        "request": request,
        "user": user,
        "pickups": pickups,
        "flash": flash,
    }
    return templates.TemplateResponse("pickups.html", ctx)


@app.get("/pickups/{pickup_number}", response_class=HTMLResponse)
async def pickup_detail(request: Request, pickup_number: int):
    allowed = ("viewer", "employee", "supervisor", "admin")
    user, redirect_resp = ensure_access(request, allowed_roles=allowed)
    if redirect_resp:
        return redirect_resp

    items = iassets.fetch_pickup_items(pickup_number)
    flash = consume_flash(request)
    ctx = {
        "request": request,
        "user": user,
        "pickup_number": pickup_number,
        "items": items,
        "flash": flash,
    }
    return templates.TemplateResponse("pickup_detail.html", ctx)


@app.get("/admin/users", response_class=HTMLResponse)
async def admin_users(request: Request):
    user, redirect_resp = ensure_access(request, allowed_roles=("admin",))
    if redirect_resp:
        return redirect_resp

    ctx = {
        "request": request,
        "user": user,
        "users": list(list_users()),
        "roles": ALLOWED_ROLES,
        "flash": consume_flash(request),
    }
    return templates.TemplateResponse("admin_users.html", ctx)


def _require_admin(request: Request) -> tuple[Optional[dict], Optional[RedirectResponse]]:
    user, redirect_resp = ensure_access(request, allowed_roles=("admin",))
    if redirect_resp:
        return None, redirect_resp
    return user, None


@app.post("/admin/users/create", response_class=HTMLResponse)
async def admin_create_user(
    request: Request,
    new_username: str = Form(...),
    full_name: str = Form(...),
    role: str = Form(...),
    account_status: str = Form(...),
    temp_password: str = Form(...),
):
    admin_user, redirect_resp = _require_admin(request)
    if redirect_resp:
        return redirect_resp

    username = new_username.strip().lower()
    full_name = full_name.strip()
    is_active = account_status == "active"

    if len(username) < 3:
        set_flash(request, "Username must be at least 3 characters.", "error")
        return RedirectResponse(url="/admin/users", status_code=status.HTTP_302_FOUND)
    if len(full_name) < 3:
        set_flash(request, "Full name must be at least 3 characters.", "error")
        return RedirectResponse(url="/admin/users", status_code=status.HTTP_302_FOUND)
    if role not in ALLOWED_ROLES:
        set_flash(request, "Invalid role.", "error")
        return RedirectResponse(url="/admin/users", status_code=status.HTTP_302_FOUND)
    if len(temp_password) < 8:
        set_flash(request, "Password must be at least 8 characters.", "error")
        return RedirectResponse(url="/admin/users", status_code=status.HTTP_302_FOUND)
    if get_user_by_username(username):
        set_flash(request, "Username already exists.", "error")
        return RedirectResponse(url="/admin/users", status_code=status.HTTP_302_FOUND)

    try:
        create_user(
            username=username,
            full_name=full_name,
            password=temp_password,
            role=role,
            is_active=is_active,
        )
    except sqlite3.IntegrityError:
        set_flash(request, "Username already exists.", "error")
    except ValueError as exc:
        set_flash(request, str(exc), "error")
    else:
        if admin_user:
            logger.info("Admin %s created user %s (%s)", admin_user["username"], username, role)
        else:
            logger.info("Admin created user %s (%s)", username, role)
        set_flash(request, f"User {username} created.", "success")
    return RedirectResponse(url="/admin/users", status_code=status.HTTP_302_FOUND)


@app.post("/admin/users/status", response_class=HTMLResponse)
async def admin_change_status(
    request: Request,
    target_user: str = Form(...),
    new_status: int = Form(...),
):
    admin_user, redirect_resp = _require_admin(request)
    if redirect_resp:
        return redirect_resp

    target = get_user_by_username(target_user)
    if not target:
        set_flash(request, "User not found.", "error")
        return RedirectResponse(url="/admin/users", status_code=status.HTTP_302_FOUND)

    update_user_status(target["id"], is_active=bool(int(new_status)))
    action = "activated" if int(new_status) else "deactivated"
    set_flash(request, f"User {target['username']} {action}.", "success")
    if admin_user:
        logger.info("Admin %s changed status for %s to %s", admin_user["username"], target["username"], action)
    return RedirectResponse(url="/admin/users", status_code=status.HTTP_302_FOUND)


@app.post("/admin/users/role", response_class=HTMLResponse)
async def admin_change_role(
    request: Request,
    role_username: str = Form(...),
    new_role: str = Form(...),
):
    admin_user, redirect_resp = _require_admin(request)
    if redirect_resp:
        return redirect_resp

    target = get_user_by_username(role_username)
    if not target:
        set_flash(request, "User not found.", "error")
        return RedirectResponse(url="/admin/users", status_code=status.HTTP_302_FOUND)

    if new_role not in ALLOWED_ROLES:
        set_flash(request, "Invalid role selected.", "error")
        return RedirectResponse(url="/admin/users", status_code=status.HTTP_302_FOUND)

    update_user_role(target["id"], role=new_role)
    set_flash(request, f"Role updated to {new_role} for {target['username']}.", "success")
    if admin_user:
        logger.info("Admin %s changed role for %s to %s", admin_user["username"], target["username"], new_role)
    return RedirectResponse(url="/admin/users", status_code=status.HTTP_302_FOUND)


@app.post("/admin/users/reset", response_class=HTMLResponse)
async def admin_reset_password(
    request: Request,
    target_username: str = Form(...),
    new_password: str = Form(...),
    confirm_password: str = Form(...),
):
    admin_user, redirect_resp = _require_admin(request)
    if redirect_resp:
        return redirect_resp

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
    actor = admin_user or current_user(request)
    if actor:
        logger.info("Admin %s reset password for user %s", actor["username"], target["username"])
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
