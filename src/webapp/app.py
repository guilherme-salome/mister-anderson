#!/usr/bin/env python3

import json
import logging
import os
import shutil
import sqlite3
from pathlib import Path
from typing import Iterable, Optional, List
from uuid import uuid4

from pydantic import BaseModel

import uvicorn
from fastapi import FastAPI, Form, Request, status, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
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
from .iassets import (
    DATA_DIR,
    update_local_product_photos,
    delete_local_product,
    update_local_product_field,
    sync_local_products_to_iassets,
)
from ..product import Product
from ..llm import process_product_folder

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

PRODUCT_UPLOAD_DIR = Path(DATA_DIR) / "product_uploads"
PRODUCT_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


class ProductFieldUpdate(BaseModel):
    field: str
    value: str = ""

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
    iassets.ensure_support_tables()
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
async def pickups_overview(request: Request, page: int = 1, q: Optional[int] = None):
    allowed = ("viewer", "employee", "supervisor", "admin")
    user, redirect_resp = ensure_access(request, allowed_roles=allowed)
    if redirect_resp:
        return redirect_resp

    page = max(page, 1)
    page_size = 25
    pickups, total = iassets.list_pickups(page=page, page_size=page_size, pickup_query=q)
    total_pages = max((total + page_size - 1) // page_size, 1)

    flash = consume_flash(request)
    ctx = {
        "request": request,
        "user": user,
        "pickups": pickups,
        "flash": flash,
        "page": page,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
        "query": q,
    }
    return templates.TemplateResponse("pickups.html", ctx)


@app.post("/pickups/create", response_class=HTMLResponse)
async def create_pickup_route(request: Request, pickup_number: int = Form(...)):
    allowed = ("employee", "supervisor", "admin")
    user, redirect_resp = ensure_access(request, allowed_roles=allowed)
    if redirect_resp:
        return redirect_resp

    try:
        iassets.create_pickup(pickup_number, created_by=user["username"])
    except ValueError as exc:
        set_flash(request, str(exc), "error")
        return RedirectResponse(url="/pickups", status_code=status.HTTP_302_FOUND)

    set_flash(request, f"Pickup {pickup_number} created.", "success")
    return RedirectResponse(
        url=f"/pickups/{pickup_number}",
        status_code=status.HTTP_302_FOUND,
    )


@app.get("/pickups/{pickup_number}", response_class=HTMLResponse)
async def pickup_detail(request: Request, pickup_number: int):
    allowed = ("viewer", "employee", "supervisor", "admin")
    user, redirect_resp = ensure_access(request, allowed_roles=allowed)
    if redirect_resp:
        return redirect_resp

    pallets = iassets.list_pallets(pickup_number)
    flash = consume_flash(request)
    ctx = {
        "request": request,
        "user": user,
        "pickup_number": pickup_number,
        "pallets": pallets,
        "flash": flash,
        "can_edit": user["role"] in ("employee", "supervisor", "admin"),
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


@app.post("/pickups/{pickup_number}/pallets/create", response_class=HTMLResponse)
async def create_pallet_route(
    request: Request,
    pickup_number: int,
    pallet_number: int = Form(...),
):
    user, redirect_resp = ensure_access(request, allowed_roles=("employee", "supervisor", "admin"))
    if redirect_resp:
        return redirect_resp

    try:
        iassets.create_pallet(pickup_number, pallet_number, created_by=user["username"])
    except ValueError as exc:
        set_flash(request, str(exc), "error")
    else:
        set_flash(request, f"Pallet {pallet_number} added to pickup {pickup_number}.", "success")

    return RedirectResponse(
        url=f"/pickups/{pickup_number}",
        status_code=status.HTTP_302_FOUND,
    )


@app.post("/pickups/{pickup_number}/pallets/{pallet_number}/products", response_class=HTMLResponse)
async def create_product_route(
    request: Request,
    pickup_number: int,
    pallet_number: int,
    quantity: int = Form(...),
    photos: Optional[List[UploadFile]] = File(None),
):
    user, redirect_resp = ensure_access(request, allowed_roles=("employee", "supervisor", "admin"))
    if redirect_resp:
        return redirect_resp

    if quantity <= 0:
        set_flash(request, "Quantity must be greater than zero.", "error")
        return RedirectResponse(
            url=f"/pickups/{pickup_number}/pallets/{pallet_number}",
            status_code=status.HTTP_302_FOUND,
        )

    valid_files = [upload for upload in (photos or []) if upload and upload.filename]
    has_photos = bool(valid_files)

    product = Product(created_by=user["username"])
    product.quantity = quantity
    product.pickup = str(pickup_number)

    base_dir = PRODUCT_UPLOAD_DIR / f"pickup_{pickup_number}" / f"pallet_{pallet_number}"
    staging_dir = base_dir / f"pending_{uuid4().hex}"
    if has_photos:
        staging_dir.mkdir(parents=True, exist_ok=True)

    saved_files: List[str] = []
    product_id = None
    try:
        if has_photos:
            for upload in valid_files:
                contents = await upload.read()
                suffix = Path(upload.filename).suffix or ".jpg"
                filename = f"{uuid4().hex}{suffix}"
                temp_path = Path(product.tempdir) / filename
                with open(temp_path, "wb") as temp_file:
                    temp_file.write(contents)

                dest_path = staging_dir / filename
                with open(dest_path, "wb") as dest_file:
                    dest_file.write(contents)
                saved_files.append(filename)

            if not saved_files:
                set_flash(request, "Images could not be processed. Please retry.", "error")
                return RedirectResponse(
                    url=f"/pickups/{pickup_number}/pallets/{pallet_number}",
                    status_code=status.HTTP_302_FOUND,
                )

            try:
                await process_product_folder(product)
            except Exception as exc:
                logger.exception("Failed to process product images with LLM: %s", exc)
                set_flash(
                    request,
                    "Images uploaded, but automatic description failed. Please edit manually when available.",
                    "error",
                )
        else:
            product.description_json = {}
            product.description_raw = ""

        data = product.description_json or {}
        serial_number = data.get("serial_number", "")
        short_description = data.get("short_description", "")
        commodity = data.get("commodity", "")
        destination = data.get("destination", "")
        description_raw = product.description_raw or ""

        description_raw = product.description_raw or ""

        product_id = iassets.create_local_product(
            pickup_number=pickup_number,
            pallet_number=pallet_number,
            quantity=quantity,
            serial_number=serial_number,
            short_description=short_description,
            commodity=commodity,
            destination=destination,
            description_raw=description_raw,
            photos=[],
            created_by=user["username"],
        )

        final_dir = base_dir / f"product_{product_id}"
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        if final_dir.exists():
            shutil.rmtree(final_dir)

        if has_photos:
            staging_dir.rename(final_dir)
            final_photo_paths: List[str] = []
            for filename in saved_files:
                photo_path = final_dir / filename
                if photo_path.exists():
                    final_photo_paths.append(str(photo_path.relative_to(DATA_DIR)))
        else:
            final_dir.mkdir(parents=True, exist_ok=True)
            final_photo_paths = []

        update_local_product_photos(product_id, final_photo_paths)

        metadata = {
            "product_id": product_id,
            "pickup_number": pickup_number,
            "pallet_number": pallet_number,
            "quantity": quantity,
            "serial_number": serial_number,
            "short_description": short_description,
            "commodity": commodity,
            "destination": destination,
            "description_raw": description_raw,
            "photos": final_photo_paths,
            "created_by": user["username"],
        }

        try:
            with open(final_dir / "metadata.json", "w", encoding="utf-8") as meta_file:
                json.dump(metadata, meta_file, indent=2)
        except Exception:
            logger.warning("Failed to write metadata for product %s", product_id, exc_info=True)

        message = f"Product captured (#{product_id})"
        if short_description:
            message += f": {short_description}"
        elif has_photos:
            message += ". AI description pending."
        set_flash(request, message, "success")
    except Exception as exc:
        logger.exception("Failed to capture product for pickup %s pallet %s", pickup_number, pallet_number)
        if product_id is not None:
            delete_local_product(product_id)
        set_flash(request, "Could not capture product. Please try again.", "error")
    finally:
        try:
            product.clean_tempdir()
        except Exception:
            pass
        if has_photos and staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)
        if product_id is None and base_dir.exists() and not any(base_dir.iterdir()):
            shutil.rmtree(base_dir, ignore_errors=True)

    return RedirectResponse(
        url=f"/pickups/{pickup_number}/pallets/{pallet_number}",
        status_code=status.HTTP_302_FOUND,
    )


@app.post(
    "/pickups/{pickup_number}/pallets/{pallet_number}/products/{product_id}/update",
    response_class=JSONResponse,
)
async def update_product_field_route(
    request: Request,
    pickup_number: int,
    pallet_number: int,
    product_id: int,
    update: ProductFieldUpdate,
):
    user, redirect_resp = ensure_access(request, allowed_roles=("employee", "supervisor", "admin"))
    if redirect_resp:
        return JSONResponse({"status": "error", "message": "Unauthorized."}, status_code=403)

    try:
        new_value = update_local_product_field(
            product_id=product_id,
            pickup_number=pickup_number,
            pallet_number=pallet_number,
            field=update.field,
            raw_value=update.value,
        )
    except ValueError as exc:
        logger.warning(
            "Failed to update product %s (pickup %s pallet %s field %s): %s",
            product_id,
            pickup_number,
            pallet_number,
            update.field,
            exc,
        )
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=400)
    except Exception:
        logger.exception(
            "Unexpected error updating product %s (pickup %s pallet %s field %s)",
            product_id,
            pickup_number,
            pallet_number,
            update.field,
        )
        return JSONResponse({"status": "error", "message": "Server error."}, status_code=500)

    return JSONResponse({"status": "ok", "value": new_value})


@app.post(
    "/pickups/{pickup_number}/pallets/{pallet_number}/sync",
    response_class=JSONResponse,
)
async def sync_products_route(
    request: Request,
    pickup_number: int,
    pallet_number: int,
):
    user, redirect_resp = ensure_access(request, allowed_roles=("employee", "supervisor", "admin"))
    if redirect_resp:
        return JSONResponse({"status": "error", "message": "Unauthorized."}, status_code=403)

    try:
        synced = sync_local_products_to_iassets(pickup_number, pallet_number)
    except Exception:
        logger.exception(
            "Failed to sync local products for pickup %s pallet %s", pickup_number, pallet_number
        )
        return JSONResponse({"status": "error", "message": "Sync failed."}, status_code=500)

    return JSONResponse({"status": "ok", "synced": synced})


@app.get("/pickups/{pickup_number}/pallets/{pallet_number}", response_class=HTMLResponse)
async def pallet_detail(
    request: Request,
    pickup_number: int,
    pallet_number: int,
):
    allowed = ("viewer", "employee", "supervisor", "admin")
    user, redirect_resp = ensure_access(request, allowed_roles=allowed)
    if redirect_resp:
        return redirect_resp

    items = iassets.fetch_pallet_items(pickup_number, pallet_number)
    pending = iassets.list_local_products(pickup_number, pallet_number)
    flash = consume_flash(request)
    ctx = {
        "request": request,
        "user": user,
        "pickup_number": pickup_number,
        "pallet_number": pallet_number,
        "items": items,
        "pending": pending,
        "flash": flash,
        "can_edit": user["role"] in ("employee", "supervisor", "admin"),
    }
    return templates.TemplateResponse("pallet_detail.html", ctx)


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
