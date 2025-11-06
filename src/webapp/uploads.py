import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence, Tuple
from uuid import uuid4
import re

from .iassets import PRODUCT_UPLOAD_DIR


logger = logging.getLogger(__name__)


UPLOAD_ROOT = PRODUCT_UPLOAD_DIR
SESSION_PREFIX = "session_"
ANALYSIS_FILENAME = "analysis.json"
ALLOWED_SUFFIXES = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".webp"}
MAX_FILES = 10
MAX_TOTAL_BYTES = 25 * 1024 * 1024  # 25 MiB
SESSION_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")


@dataclass(frozen=True)
class AnalysisPayload:
    session_id: str
    description_json: dict
    description_raw: str


def ensure_upload_root() -> None:
    UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)


def base_dir_for(pickup_number: int, cod_assets: int) -> Path:
    ensure_upload_root()
    return UPLOAD_ROOT / f"pickup_{pickup_number}" / f"pallet_{cod_assets}"


def begin_session(pickup_number: int, cod_assets: int) -> Tuple[str, Path]:
    session_id = uuid4().hex
    base_dir = base_dir_for(pickup_number, cod_assets)
    session_dir = base_dir / f"{SESSION_PREFIX}{session_id}"
    session_dir.mkdir(parents=True, exist_ok=True)
    return session_id, session_dir


def session_dir_for(base_dir: Path, session_id: str) -> Path:
    return base_dir / f"{SESSION_PREFIX}{session_id}"


def is_valid_session_id(session_id: str) -> bool:
    return bool(SESSION_ID_PATTERN.fullmatch(session_id))


def normalise_suffix(filename: Optional[str]) -> str:
    suffix = Path(filename or "").suffix.lower()
    return suffix or ".jpg"


def validate_uploads(uploads: Sequence[Tuple[str, bytes]]) -> None:
    if not uploads:
        raise ValueError("No images were provided.")
    if len(uploads) > MAX_FILES:
        raise ValueError(f"No more than {MAX_FILES} images allowed per analysis.")
    total = sum(len(data) for _, data in uploads)
    if total > MAX_TOTAL_BYTES:
        raise ValueError("Image batch exceeds 25 MB limit.")
    for filename, _ in uploads:
        suffix = normalise_suffix(filename)
        if suffix not in ALLOWED_SUFFIXES:
            raise ValueError(f"Unsupported image type: {suffix}")


def persist_bytes(session_dir: Path, product_tempdir: Path, uploads: Sequence[Tuple[str, bytes]]) -> List[str]:
    filenames: List[str] = []
    for original_name, data in uploads:
        suffix = normalise_suffix(original_name)
        filename = f"{uuid4().hex}{suffix}"
        (product_tempdir / filename).write_bytes(data)
        (session_dir / filename).write_bytes(data)
        filenames.append(filename)
    return filenames


def write_analysis(session_dir: Path, *, description_json: dict, description_raw: str) -> None:
    payload = {
        "description_json": description_json,
        "description_raw": description_raw,
    }
    (session_dir / ANALYSIS_FILENAME).write_text(json.dumps(payload), encoding="utf-8")


def load_analysis(session_dir: Path) -> Optional[AnalysisPayload]:
    analysis_path = session_dir / ANALYSIS_FILENAME
    if not analysis_path.exists():
        return None
    try:
        data = json.loads(analysis_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        logger.warning("Failed to parse analysis payload at %s", analysis_path)
        return None
    session_id = session_dir.name
    if session_id.startswith(SESSION_PREFIX):
        session_id = session_id[len(SESSION_PREFIX) :]
    return AnalysisPayload(
        session_id=session_id,
        description_json=data.get("description_json") or {},
        description_raw=data.get("description_raw") or "",
    )


def iter_session_files(session_dir: Path) -> List[Path]:
    if not session_dir.exists():
        return []
    return [
        path
        for path in session_dir.iterdir()
        if path.is_file() and path.name != ANALYSIS_FILENAME
    ]


def cleanup_session(session_dir: Path) -> None:
    if not session_dir.exists():
        return
    for path in session_dir.iterdir():
        if path.is_file():
            path.unlink(missing_ok=True)
    try:
        session_dir.rmdir()
    except OSError:
        logger.debug("Session directory %s not empty during cleanup.", session_dir)
