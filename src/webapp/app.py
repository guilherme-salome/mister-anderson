#!/usr/bin/env python3

import json
import logging
import os
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional, List, Tuple
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
    PRODUCT_UPLOAD_DIR,
    create_product_entry,
    delete_product_entry,
    update_iassets_field,
    warm_access_connection,
)
from ..product import Product
from ..llm import process_product_folder
from .uploads import (
    ANALYSIS_FILENAME,
    AnalysisPayload,
    begin_session,
    cleanup_session,
    is_valid_session_id,
    iter_session_files,
    load_analysis,
    persist_bytes,
    session_dir_for,
    validate_uploads,
    write_analysis,
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
    warm_access_connection()
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
        "active_page": "dashboard",
    }
    flash = consume_flash(request)
    if flash:
        ctx["flash"] = flash
    return templates.TemplateResponse("dashboard.html", ctx)


@app.get("/pickups", response_class=HTMLResponse)
async def pickups_overview(request: Request, q: Optional[str] = None):
    allowed = ("viewer", "employee", "supervisor", "admin")
    user, redirect_resp = ensure_access(request, allowed_roles=allowed)
    if redirect_resp:
        return redirect_resp

    flash = consume_flash(request)
    query_value = ""

    if q is not None:
        query_value = q.strip()
        if query_value:
            try:
                pickup_number = int(query_value)
                if pickup_number <= 0:
                    raise ValueError
            except ValueError:
                flash = {"message": "Enter a valid pickup number.", "category": "error"}
            else:
                if iassets.pickup_exists(pickup_number):
                    return RedirectResponse(
                        url=f"/pickups/{pickup_number}",
                        status_code=status.HTTP_302_FOUND,
                    )
                flash = {
                    "message": f"Pickup {pickup_number} not found.",
                    "category": "error",
                }
        else:
            flash = flash or {"message": "Enter a pickup number to search.", "category": "error"}

    ctx = {
        "request": request,
        "user": user,
        "flash": flash,
        "query": query_value,
        "active_page": "pickups",
    }
    return templates.TemplateResponse("pickups.html", ctx)


@app.post("/pickups/create", response_class=HTMLResponse)
async def create_pickup_route(request: Request, pickup_number: int = Form(...)):
    allowed = ("employee", "supervisor", "admin")
    user, redirect_resp = ensure_access(request, allowed_roles=allowed)
    if redirect_resp:
        return redirect_resp

    set_flash(
        request,
        "Creating pickups from the web app is disabled. Please use the external workflow.",
        "error",
    )
    return RedirectResponse(url="/pickups", status_code=status.HTTP_302_FOUND)


@app.get("/pickups/{pickup_number}", response_class=HTMLResponse)
async def pickup_detail(request: Request, pickup_number: int):
    allowed = ("viewer", "employee", "supervisor", "admin")
    user, redirect_resp = ensure_access(request, allowed_roles=allowed)
    if redirect_resp:
        return redirect_resp

    client_info = iassets.get_pickup_client(pickup_number)
    pallets = iassets.list_pallets(pickup_number)
    for pallet in pallets:
        dt_text = pallet.get("DT_UPDATE")
        if isinstance(dt_text, str) and dt_text:
            pallet["DT_UPDATE_DATE"] = dt_text.split(" ", 1)[0]
        else:
            pallet["DT_UPDATE_DATE"] = None
    flash = consume_flash(request)
    ctx = {
        "request": request,
        "user": user,
        "pickup_number": pickup_number,
        "pallets": pallets,
        "flash": flash,
        "client_id": client_info.get("client_id"),
        "client_name": client_info.get("client_name"),
        "can_edit": user["role"] in ("employee", "supervisor", "admin"),
        "active_page": "pickups",
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
        "active_page": "admin",
    }
    return templates.TemplateResponse("admin_users.html", ctx)


@app.post("/pickups/{pickup_number}/pallets/create", response_class=HTMLResponse)
async def create_pallet_route(
    request: Request,
    pickup_number: int,
    cod_assets: int = Form(...),
):
    user, redirect_resp = ensure_access(request, allowed_roles=("employee", "supervisor", "admin"))
    if redirect_resp:
        return redirect_resp

    set_flash(
        request,
        "Pallet creation from the web app is disabled. Please use the external workflow.",
        "error",
    )

    return RedirectResponse(
        url=f"/pickups/{pickup_number}",
        status_code=status.HTTP_302_FOUND,
    )



@app.post("/pickups/{pickup_number}/pallets/{cod_assets}/products", response_class=HTMLResponse)
async def create_product_route(
    request: Request,
    pickup_number: int,
    cod_assets: int,
    quantity: int = Form(...),
    sn: str = Form(""),
    asset_tag: str = Form(""),
    description: str = Form(""),
    subcategory: str = Form(""),
    cod_destiny: str = Form(""),
    upload_session_id: str = Form(""),
    photos: Optional[List[UploadFile]] = File(None),
):
    user, redirect_resp = ensure_access(request, allowed_roles=("employee", "supervisor", "admin"))
    if redirect_resp:
        return redirect_resp

    redirect_url = f"/pickups/{pickup_number}/pallets/{cod_assets}"
    client_info = iassets.get_pickup_client(pickup_number)
    subcategory_options = iassets.get_subcategory_suggestions(client_info['client_id'])
    destiny_options = iassets.get_destiny_options()
    destiny_lookup: dict[int, str] = {}
    for option in destiny_options:
        if not isinstance(option, dict):
            continue
        code_raw = option.get("code")
        label_raw = option.get("label")
        try:
            code_int = int(code_raw) if code_raw is not None else None
        except (TypeError, ValueError):
            code_int = None
        label = (label_raw or "") if label_raw is not None else ""
        if code_int is not None and label:
            destiny_lookup[code_int] = label

    if quantity <= 0:
        set_flash(request, "Quantity must be greater than zero.", "error")
        return RedirectResponse(url=redirect_url, status_code=status.HTTP_302_FOUND)

    sn_value = (sn or "").strip()
    asset_tag_value = (asset_tag or "").strip()
    description_value = (description or "").strip()
    subcategory_input = (subcategory or "").strip()
    cod_destiny_input = (cod_destiny or "").strip()

    valid_files = [upload for upload in (photos or []) if upload and upload.filename]
    session_id_raw = (upload_session_id or "").strip()

    base_dir = PRODUCT_UPLOAD_DIR / f"pickup_{pickup_number}" / f"pallet_{cod_assets}"
    session_dir = None
    analysis_payload: Optional[AnalysisPayload] = None
    using_session = False
    session_files: List[Path] = []

    if session_id_raw:
        if not is_valid_session_id(session_id_raw):
            set_flash(request, "Photo session token was invalid. Please re-upload your images.", "error")
            return RedirectResponse(url=redirect_url, status_code=status.HTTP_302_FOUND)
        candidate_dir = session_dir_for(base_dir, session_id_raw)
        if not candidate_dir.exists():
            set_flash(request, "Photo session expired. Please re-upload your images.", "error")
            return RedirectResponse(url=redirect_url, status_code=status.HTTP_302_FOUND)
        session_files = iter_session_files(candidate_dir)
        if not session_files:
            set_flash(request, "Photo session expired. Please re-upload your images.", "error")
            return RedirectResponse(url=redirect_url, status_code=status.HTTP_302_FOUND)
        analysis_payload = load_analysis(candidate_dir)
        session_dir = candidate_dir
        using_session = True

    if using_session and valid_files:
        set_flash(
            request,
            "Photos were already analyzed. Refresh the page or clear the session before attaching new images.",
            "error",
        )
        return RedirectResponse(url=redirect_url, status_code=status.HTTP_302_FOUND)

    has_photos = bool(valid_files) or using_session

    product = Product(created_by=user["username"])
    product.quantity = quantity
    product.pickup = str(pickup_number)

    staging_dir = session_dir if using_session else base_dir / f"pending_{uuid4().hex}"
    if has_photos and not using_session:
        staging_dir.mkdir(parents=True, exist_ok=True)

    saved_files: List[str] = []
    product_id = None
    final_photo_paths: List[str] = []
    try:
        if using_session:
            saved_files = [path.name for path in session_files]
            if analysis_payload:
                product.description_json = analysis_payload.description_json
                product.description_raw = analysis_payload.description_raw
            else:
                product.description_json = {}
                product.description_raw = ""
        elif has_photos:
            file_payloads: List[Tuple[str, bytes]] = []
            for upload in valid_files:
                contents = await upload.read()
                file_payloads.append((upload.filename or "", contents))

            try:
                validate_uploads(file_payloads)
            except ValueError as exc:
                set_flash(request, str(exc), "error")
                return RedirectResponse(url=redirect_url, status_code=status.HTTP_302_FOUND)

            saved_files = persist_bytes(
                staging_dir,
                Path(product.tempdir),
                file_payloads,
            )

            if not saved_files:
                set_flash(request, "Images could not be processed. Please retry.", "error")
                return RedirectResponse(url=redirect_url, status_code=status.HTTP_302_FOUND)

            try:
                await process_product_folder(
                    product,
                    subcategory_options=subcategory_options,
                    destiny_options=destiny_options,
                )
            except Exception as exc:
                logger.exception("Failed to process product images with LLM: %s", exc)
                set_flash(
                    request,
                    "Images uploaded, but automatic description failed. Please review the form manually.",
                    "error",
                )
        else:
            product.description_json = {}
            product.description_raw = ""

        data = product.description_json or {}
        if isinstance(data, dict) and "commodity" in data and "subcategory" not in data:
            legacy_value = data.pop("commodity")
            if legacy_value:
                data["subcategory"] = legacy_value
        llm_serial = (data.get("serial_number") or "").strip()
        llm_short = (data.get("short_description") or "").strip()
        llm_asset_tag = (data.get("asset_tag") or "").strip()
        llm_subcategory = (data.get("subcategory") or "").strip()
        llm_destination_label = (data.get("destination_label") or data.get("destination") or "").strip()
        llm_destination_code = data.get("cod_destiny")
        llm_reason = (data.get("destination_reason") or data.get("reason") or "").strip()
        description_raw = (product.description_raw or "").strip()

        if not sn_value:
            sn_value = llm_serial
        if not description_value:
            description_value = llm_short or description_raw or ""
        if not asset_tag_value:
            asset_tag_value = llm_asset_tag or product.asset_tag

        subcategory_value = iassets.canonicalize_subcategory(
            subcategory_input,
            suggestions=subcategory_options,
        )
        if not subcategory_value:
            subcategory_value = iassets.canonicalize_subcategory(
                llm_subcategory,
                suggestions=subcategory_options,
            )
        if not subcategory_value and description_value:
            subcategory_value = iassets.canonicalize_subcategory(
                description_value,
                suggestions=subcategory_options,
            )

        cod_destiny_value, cod_destiny_label = iassets.resolve_cod_destiny(
            cod_destiny_input or None,
            destiny_options=destiny_options,
        )
        if cod_destiny_value is None:
            cod_destiny_value, cod_destiny_label = iassets.resolve_cod_destiny(
                llm_destination_code,
                destiny_options=destiny_options,
                label_hint=llm_destination_label,
            )
        if cod_destiny_value is None:
            cod_destiny_value, cod_destiny_label = iassets.resolve_cod_destiny(
                llm_destination_label,
                destiny_options=destiny_options,
            )
        if cod_destiny_value is not None and not cod_destiny_label:
            try:
                cod_destiny_label = destiny_lookup.get(int(cod_destiny_value))
            except (TypeError, ValueError):
                cod_destiny_label = destiny_lookup.get(cod_destiny_value)

        reason_value = llm_reason

        if not subcategory_value:
            set_flash(request, "Subcategory is required. Please choose or enter a value.", "error")
            return RedirectResponse(url=redirect_url, status_code=status.HTTP_302_FOUND)

        subcategory_code_value, subcategory_label = iassets.resolve_subcategory_code(subcategory_value)
        if subcategory_code_value is None:
            set_flash(request, "Subcategory selection is invalid. Please pick an option from the list.", "error")
            return RedirectResponse(url=redirect_url, status_code=status.HTTP_302_FOUND)
        if subcategory_label:
            subcategory_value = subcategory_label

        if cod_destiny_value is None:
            set_flash(request, "Destination is required. Please select a destination.", "error")
            return RedirectResponse(url=redirect_url, status_code=status.HTTP_302_FOUND)

        product_id = create_product_entry(
            pickup_number=pickup_number,
            cod_assets=cod_assets,
            quantity=quantity,
            serial_number=sn_value,
            short_description=description_value,
            description_raw=description_raw,
            created_by=user["username"],
            asset_tag=asset_tag_value,
            subcategory=subcategory_value,
            subcategory_code=subcategory_code_value,
            cod_destiny=cod_destiny_value,
            destination_label=cod_destiny_label,
            reason=reason_value,
        )

        final_dir = base_dir / f"product_{product_id}"
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        if final_dir.exists():
            shutil.rmtree(final_dir)

        if has_photos:
            staging_dir.rename(final_dir)
            for filename in saved_files:
                photo_path = final_dir / filename
                if photo_path.exists():
                    final_photo_paths.append(str(photo_path.relative_to(DATA_DIR)))
            analysis_file = final_dir / ANALYSIS_FILENAME
            if analysis_file.exists():
                analysis_file.unlink(missing_ok=True)
        else:
            final_dir.mkdir(parents=True, exist_ok=True)

        metadata = {
            "product_id": product_id,
            "pickup_number": pickup_number,
            "quantity": quantity,
            "cod_assets": cod_assets,
            "cod_assets_sqlite": product_id,
            "sn": sn_value,
            "asset_tag": asset_tag_value,
            "description": description_value,
            "description_raw": description_raw,
            "subcategory": subcategory_value,
            "subcategory_code": subcategory_code_value,
            "cod_destiny": cod_destiny_value,
            "destination_label": cod_destiny_label,
            "destination_reason": reason_value,
            "photos": final_photo_paths,
            "created_by": user["username"],
            "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
            "ai_payload": data,
        }

        try:
            with open(final_dir / "metadata.json", "w", encoding="utf-8") as meta_file:
                json.dump(metadata, meta_file, indent=2)
        except Exception:  # pragma: no cover - best effort
            logger.warning("Failed to write metadata for product %s", product_id, exc_info=True)

        summary_parts: List[str] = []
        if description_value:
            summary_parts.append(description_value[:72])
        elif sn_value:
            summary_parts.append(sn_value)
        elif asset_tag_value:
            summary_parts.append(asset_tag_value)
        if subcategory_value:
            summary_parts.append(f"Subcategory: {subcategory_value}")
        if cod_destiny_label:
            summary_parts.append(f"Destination: {cod_destiny_label}")
        elif cod_destiny_value is not None:
            summary_parts.append(f"Destination: #{cod_destiny_value}")

        if summary_parts:
            message = f"Product #{product_id} stored — {' · '.join(summary_parts[:3])}"
        else:
            message = f"Product #{product_id} stored."
        set_flash(request, message, "success")
    except Exception:
        logger.exception("Failed to capture product for pickup %s COD_ASSETS %s", pickup_number, cod_assets)
        if product_id is not None:
            try:
                delete_product_entry(product_id)
            except Exception:
                logger.warning("Failed to delete IASSETS entry %s after error", product_id, exc_info=True)
        set_flash(request, "Could not capture product. Please try again.", "error")
    finally:
        try:
            product.clean_tempdir()
        except Exception:
            pass
        if has_photos and not using_session and staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)
        if (
            product_id is None
            and not using_session
            and base_dir.exists()
            and not any(base_dir.iterdir())
        ):
            shutil.rmtree(base_dir, ignore_errors=True)

    return RedirectResponse(url=redirect_url, status_code=status.HTTP_302_FOUND)


@app.post(
    "/pickups/{pickup_number}/pallets/{cod_assets}/products/analyze",
    response_class=JSONResponse,
)
async def analyze_product_photos(
    request: Request,
    pickup_number: int,
    cod_assets: int,
    photos: Optional[List[UploadFile]] = File(None),
):
    user, redirect_resp = ensure_access(
        request, allowed_roles=("employee", "supervisor", "admin")
    )
    if redirect_resp:
        return JSONResponse({"status": "error", "message": "Unauthorized."}, status_code=403)

    uploads = [upload for upload in (photos or []) if upload and upload.filename]
    if not uploads:
        return JSONResponse({"status": "error", "message": "Please add at least one image."}, status_code=400)

    file_payloads: List[Tuple[str, bytes]] = []
    for upload in uploads:
        contents = await upload.read()
        file_payloads.append((upload.filename or "", contents))

    try:
        validate_uploads(file_payloads)
    except ValueError as exc:
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=400)

    subcategory_options = iassets.get_subcategory_suggestions()
    destiny_options = iassets.get_destiny_options()

    session_id, session_dir = begin_session(pickup_number, cod_assets)
    product = Product(created_by=user["username"])
    product.pickup = str(pickup_number)

    try:
        persist_bytes(session_dir, Path(product.tempdir), file_payloads)

        await process_product_folder(
            product,
            subcategory_options=subcategory_options,
            destiny_options=destiny_options,
        )

        data = product.description_json or {}
        if isinstance(data, dict) and "commodity" in data and "subcategory" not in data:
            legacy_value = data.pop("commodity")
            if legacy_value:
                data["subcategory"] = legacy_value

        write_analysis(
            session_dir,
            description_json=data,
            description_raw=product.description_raw or "",
        )

        suggestion_serial = (data.get("serial_number") or "").strip()
        suggestion_asset_tag = (data.get("asset_tag") or "").strip()
        suggestion_description = (data.get("short_description") or "").strip()
        suggestion_subcategory = (data.get("subcategory") or "").strip()
        suggestion_destiny = data.get("cod_destiny")
        suggestion_destiny_label = (data.get("destination_label") or data.get("destination") or "").strip()
        suggestion_reason = (data.get("destination_reason") or data.get("reason") or "").strip()

        response_payload = {
            "status": "ok",
            "session": session_id,
            "suggestions": {
                "serial_number": suggestion_serial,
                "asset_tag": suggestion_asset_tag,
                "short_description": suggestion_description,
                "subcategory": suggestion_subcategory,
                "cod_destiny": suggestion_destiny,
                "destination_label": suggestion_destiny_label,
                "destination_reason": suggestion_reason,
                "description_raw": product.description_raw or "",
            },
        }
        return JSONResponse(response_payload)
    except Exception:
        logger.exception(
            "Failed to analyze product images for pickup %s COD_ASSETS %s",
            pickup_number,
            cod_assets,
        )
        cleanup_session(session_dir)
        return JSONResponse(
            {"status": "error", "message": "Unable to analyze photos right now. Please retry."},
            status_code=500,
        )
    finally:
        try:
            product.clean_tempdir()
        except Exception:
            pass

@app.post(
    "/pickups/{pickup_number}/pallets/{cod_assets}/iassets/{row_id}/update",
    response_class=JSONResponse,
)
async def update_iassets_field_route(
    request: Request,
    pickup_number: int,
    cod_assets: int,
    row_id: int,
    update: ProductFieldUpdate,
):
    user, redirect_resp = ensure_access(request, allowed_roles=("employee", "supervisor", "admin"))
    if redirect_resp:
        return JSONResponse({"status": "error", "message": "Unauthorized."}, status_code=403)

    try:
        new_value = update_iassets_field(
            row_id=row_id,
            pickup_number=pickup_number,
            cod_assets=cod_assets,
            field=update.field,
            raw_value=update.value,
        )
    except ValueError as exc:
        logger.warning(
            "Failed to update IASSETS row %s (pickup %s COD_ASSETS %s field %s): %s",
            row_id,
            pickup_number,
            cod_assets,
            update.field,
            exc,
        )
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=400)
    except Exception:
        logger.exception(
            "Unexpected error updating IASSETS row %s (pickup %s COD_ASSETS %s field %s)",
            row_id,
            pickup_number,
            cod_assets,
            update.field,
        )
        return JSONResponse({"status": "error", "message": "Server error."}, status_code=500)

    return JSONResponse({"status": "ok", "value": new_value})
@app.get("/pickups/{pickup_number}/pallets/{cod_assets}", response_class=HTMLResponse)
async def pallet_detail(
    request: Request,
    pickup_number: int,
    cod_assets: int,
):
    allowed = ("viewer", "employee", "supervisor", "admin")
    user, redirect_resp = ensure_access(request, allowed_roles=allowed)
    if redirect_resp:
        return redirect_resp

    items = iassets.fetch_pallet_items(pickup_number, cod_assets)
    subcategory_options = iassets.get_subcategory_suggestions()
    destiny_options = iassets.get_destiny_options()
    destiny_lookup = {}
    for option in destiny_options:
        code = option.get("code")
        label = option.get("label")
        try:
            key = int(code) if code is not None else None
        except (TypeError, ValueError):
            key = None
        if key is not None and label:
            destiny_lookup[key] = label

    for item in items:
        code_value = item.get("COD_DESTINY")
        label_value = ""
        try:
            code_int = int(code_value) if code_value is not None else None
        except (TypeError, ValueError):
            code_int = None
        if code_int is not None and code_int in destiny_lookup:
            label_value = destiny_lookup[code_int]
        item["COD_DESTINY_LABEL"] = label_value

    warehouse_pallet_number = iassets.get_warehouse_pallet_number(pickup_number, cod_assets)
    flash = consume_flash(request)
    display_number: object
    if warehouse_pallet_number is not None:
        display_number = warehouse_pallet_number
    elif cod_assets not in (None, ""):
        display_number = cod_assets
    else:
        display_number = "—"

    ctx = {
        "request": request,
        "user": user,
        "pickup_number": pickup_number,
        "cod_assets": cod_assets,
        "pallet_number": cod_assets,
        "pallet_display_number": display_number,
        "warehouse_pallet_number": warehouse_pallet_number,
        "items": items,
        "flash": flash,
        "can_edit": user["role"] in ("employee", "supervisor", "admin"),
        "active_page": "pickups",
        "subcategory_options": subcategory_options,
        "destiny_options": destiny_options,
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
