from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import html
import json
import logging
import os
import re
import secrets
import shutil
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit, urlunsplit

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse
from fastapi.responses import HTMLResponse
from fastapi.responses import JSONResponse
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field


APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = (APP_DIR / "static").resolve()
_DATA_PATH_RAW = os.getenv("APP_DATA_PATH", "./data/chat.json").strip() or "./data/chat.json"
if Path(_DATA_PATH_RAW).is_absolute():
    DATA_PATH = Path(_DATA_PATH_RAW).resolve()
else:
    DATA_PATH = (APP_DIR / _DATA_PATH_RAW).resolve()
LEGACY_DATA_PATH = (APP_DIR / "data" / "store.json").resolve()
ATTACHMENT_FILE_DIR = (DATA_PATH.parent / "file").resolve()
ATTACHMENT_FILE_URL_PREFIX = "/data/file/"
ATTACHMENT_FILE_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_LOG_PATH = (DATA_PATH.parent / "audit.log").resolve()

SESSION_COOKIE = "pychat_session"
SESSION_TTL_SECONDS = int(os.getenv("APP_SESSION_TTL_SECONDS", "604800"))
SESSION_COOKIE_SAMESITE = os.getenv("APP_SESSION_COOKIE_SAMESITE", "lax").lower()
SESSION_COOKIE_SECURE = os.getenv("APP_SESSION_COOKIE_SECURE", "false").lower() in {
    "1",
    "true",
    "yes",
}

AUTH_FREE_API_PATHS = {
    "/api/auth/status",
    "/api/auth/login",
    "/api/auth/logout",
    "/api/setup/init",
}

DEFAULT_APP_TITLE = "My Chat"
DEFAULT_APP_SUBTITLE = "Build your own AI assistant"
DEFAULT_MODEL_NAME = os.getenv("DEFAULT_MODEL_NAME", "gpt-5.5").strip() or "gpt-5.5"
DEFAULT_BASE_URL = os.getenv("DEFAULT_BASE_URL", "").strip()
DEFAULT_API_KEY = os.getenv("DEFAULT_API_KEY", "").strip()
MAX_RETENTION_HOURS = 24 * 365 * 5

BUILTIN_OPS_AGENT_NAME = "运维专家"
BUILTIN_OPS_AGENT_PROMPT = """你是一名资深运维/运维开发工程师助手。

## 风格
- 默认中文。先给结论，再给最多3条要点。
- 除非用户要求，不提供背景扩展或免责声明。
- 能一句话说清就只说一句。
- 能用命令解决的不说废话，能用代码解决的不写文档
- 回答用散文+代码块，不滥用列表和标题
- 不说"好的""当然""希望对你有帮助"

## 能力范围
Linux 系统、Shell、Python、go、lua、容器、监控告警、公有/私有云平台、分布式/集中式块存储、文件存储、网络排障、数据库运维、中间件运维、k8s/docker

## 回答原则
- 给可以直接用的命令/脚本，附必要参数说明
- 有多种方案时，直接推荐最优的，其他的一句话带过
- 不确定的直接说，不编
- 排障类问题：定位命令 → 关键字段 → 结论，没有第四段
- 涉及危险操作主动提示风险，一句话够了

## 禁止
- 不开头重复用户的问题
- 不结尾加任何引导句，不说"把xxx贴出来""如有问题""可以继续问"
- 不把一句话能说清的东西拆成列表
- 同一个意思只说一次，不在结尾复述
- 列表项超过5条时，合并同类、删掉次要的
- 命令注释只写必要的，不写废话注释

## 回答前强制自检
1. 第一句是废话？→ 删
2. 能更短？→ 删多余的
3. 有用户没问的东西？→ 删
4. 列表能改一句话？→ 改
5. 结尾有引导句？→ 删。"""

BUILTIN_GENERAL_AGENT_NAME = "默认助手"
BUILTIN_GENERAL_AGENT_PROMPT = """你是一名通用中文助手。

## 风格
- 默认中文。先给结论，再给最多3条要点。
- 除非用户要求，不提供背景扩展或免责声明。
- 能一句话说清就只说一句。
- 不说"好的""当然""希望对你有帮助"

## 回答原则
- 有多种方案时，直接推荐最优的，其他的一句话带过
- 不确定的直接说，不编

## 禁止
- 不开头重复用户的问题
- 不结尾加任何引导句，不说"把xxx贴出来""如有问题""可以继续问"
- 不把一句话能说清的东西拆成列表
- 同一个意思只说一次，不在结尾复述
- 列表项超过5条时，合并同类、删掉次要的

## 回答前强制自检
1. 第一句是废话？→ 删
2. 能更短？→ 删多余的
3. 有用户没问的东西？→ 删
4. 列表能改一句话？→ 改
5. 结尾有引导句？→ 删。"""


# Fresh deployment default builtin agent.
# Supported keys are the `key` fields in BUILTIN_AGENTS below.
DEFAULT_BUILTIN_AGENT_KEY = "general"

BUILTIN_AGENTS = [
    {
        "key": "ops",
        "name": BUILTIN_OPS_AGENT_NAME,
        "prompt": BUILTIN_OPS_AGENT_PROMPT,
    },
    {
        "key": "general",
        "name": BUILTIN_GENERAL_AGENT_NAME,
        "prompt": BUILTIN_GENERAL_AGENT_PROMPT,
    },
]
BUILTIN_AGENT_BY_KEY = {item["key"]: item for item in BUILTIN_AGENTS}
BUILTIN_AGENT_BY_NAME = {item["name"]: item for item in BUILTIN_AGENTS}
BUILTIN_AGENT_NAME_SET = set(BUILTIN_AGENT_BY_NAME.keys())


UPSTREAM_RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
UPSTREAM_RETRY_DELAYS_SECONDS = [0.8, 1.6]

STORAGE_LOCK = threading.Lock()
AUDIT_LOG_LOCK = threading.Lock()
LOGGER = logging.getLogger("py_lite_chat")


def now_ts() -> int:
    return int(time.time())


def new_id() -> str:
    return uuid.uuid4().hex


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_chat_title_from_message(message: str, max_len: int = 28) -> str:
    text = " ".join((message or "").strip().splitlines()).strip()
    if not text:
        return "新对话"
    if len(text) <= max_len:
        return text
    return f"{text[:max_len].rstrip()}..."


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _normalize_retention_hours(value: Any, default: int = 0) -> int:
    ttl = _safe_int(value, default)
    if ttl < 0:
        ttl = 0
    if ttl > MAX_RETENTION_HOURS:
        ttl = MAX_RETENTION_HOURS
    return ttl


def _normalize_role(value: Any) -> str:
    role = str(value or "user").strip().lower()
    if role not in {"admin", "user"}:
        return "user"
    return role


def _normalize_username(value: Any) -> str:
    return str(value or "").strip()


def _normalize_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return default


def _normalize_reasoning_effort(value: Any) -> str | None:
    effort = str(value or "").strip().lower()
    if effort in {"low", "medium", "high", "xhigh"}:
        return effort
    return None


def _get_builtin_agent_definition_by_name(name: Any) -> dict[str, Any] | None:
    return BUILTIN_AGENT_BY_NAME.get(str(name or "").strip())


def _get_default_builtin_agent_definition() -> dict[str, Any]:
    configured = BUILTIN_AGENT_BY_KEY.get(DEFAULT_BUILTIN_AGENT_KEY)
    if configured:
        return configured
    return BUILTIN_AGENTS[0]


def _mask_secret_for_audit(value: Any) -> str:
    raw = str(value or "")
    n = len(raw)
    if n <= 0:
        return ""
    if n <= 6:
        return "*" * n
    return f"{raw[:2]}{'*' * (n - 4)}{raw[-2:]}"


def _truncate_for_audit_text(value: Any, max_len: int = 240) -> str:
    text = str(value or "")
    if len(text) <= max_len:
        return text
    return f"{text[:max_len]}...(总{len(text)}字符)"


def _sanitize_audit_value(value: Any, key_name: str = "", depth: int = 0) -> Any:
    if depth > 6:
        return "[已省略嵌套对象]"

    key = str(key_name or "").lower()
    if key in {"message", "messages", "prompt", "input"} or key.endswith("_message") or key.endswith("_content"):
        return "[已省略消息内容]"
    if key in {"attachments", "files", "images"}:
        if isinstance(value, list):
            return f"[已省略附件，共{len(value)}项]"
        return "[已省略附件]"
    if any(token in key for token in ("password", "api_key", "authorization", "token", "cookie", "secret")):
        return _mask_secret_for_audit(value)

    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        items = list(value.items())
        for idx, (k, v) in enumerate(items):
            if idx >= 80:
                sanitized["__剩余字段__"] = f"已省略{len(items) - idx}个字段"
                break
            sanitized[str(k)] = _sanitize_audit_value(v, key_name=str(k), depth=depth + 1)
        return sanitized

    if isinstance(value, list):
        sanitized_items = []
        for idx, item in enumerate(value):
            if idx >= 30:
                sanitized_items.append(f"[已省略剩余{len(value) - idx}项]")
                break
            sanitized_items.append(_sanitize_audit_value(item, key_name=key_name, depth=depth + 1))
        return sanitized_items

    if isinstance(value, (bytes, bytearray)):
        return f"[二进制{len(value)}字节]"

    if isinstance(value, str):
        return _truncate_for_audit_text(value)

    if value is None or isinstance(value, (int, float, bool)):
        return value

    return _truncate_for_audit_text(repr(value))


async def _extract_request_params_for_audit(request: Request) -> dict[str, Any]:
    params: dict[str, Any] = {}

    query_map: dict[str, Any] = {}
    for key in request.query_params.keys():
        values = request.query_params.getlist(key)
        query_map[key] = values[0] if len(values) == 1 else values
    if query_map:
        params["query"] = _sanitize_audit_value(query_map)

    if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
        return params

    content_type = str(request.headers.get("content-type") or "").lower()
    if "application/json" not in content_type:
        if content_type:
            params["body"] = f"[{content_type} 请求体未解析]"
        return params

    body_bytes = await request.body()
    if not body_bytes:
        return params

    if request.url.path in {"/api/chat", "/api/chat/stream"}:
        try:
            payload = json.loads(body_bytes.decode("utf-8", errors="ignore"))
        except Exception:
            params["body"] = "[聊天请求参数解析失败]"
            return params
        if isinstance(payload, dict):
            summary = {
                "chat_id": payload.get("chat_id"),
                "model_id": payload.get("model_id"),
                "model_name": payload.get("model_name"),
                "mask_id": payload.get("mask_id"),
                "reasoning_effort": payload.get("reasoning_effort"),
                "temperature": payload.get("temperature"),
                "attachments_count": len(payload.get("attachments", []))
                if isinstance(payload.get("attachments"), list)
                else 0,
                "message": "[已省略消息内容]",
                "_has_model_id": "model_id" in payload,
                "_has_mask_id": "mask_id" in payload,
                "_has_reasoning_effort": "reasoning_effort" in payload,
            }
            params["body"] = summary
            return params
        params["body"] = "[聊天请求体格式异常]"
        return params

    try:
        payload = json.loads(body_bytes.decode("utf-8", errors="ignore"))
        params["body"] = _sanitize_audit_value(payload)
    except Exception:
        params["body"] = _truncate_for_audit_text(body_bytes.decode("utf-8", errors="ignore"), max_len=400)
    return params


def _client_ip_for_audit(request: Request) -> str:
    xff = str(request.headers.get("x-forwarded-for") or "").strip()
    if xff:
        return xff.split(",")[0].strip()
    client = request.client
    return client.host if client else ""


def _describe_api_action(method: str, path: str) -> str:
    method_u = str(method or "").upper()
    if path == "/api/auth/login":
        return "用户登录"
    if path == "/api/auth/logout":
        return "用户登出"
    if path == "/api/auth/status":
        return "查询登录状态"
    if path == "/api/setup/init":
        return "初始化应用"
    if path == "/api/state":
        return "读取应用状态"
    if path == "/api/models":
        return "读取模型列表" if method_u == "GET" else "新增或更新模型"
    if method_u == "POST" and path.endswith("/streaming") and path.startswith("/api/models/"):
        return "修改模型流传输设置"
    if method_u == "DELETE" and path.startswith("/api/models/"):
        return "删除模型"
    if path == "/api/masks":
        return "读取Agent列表" if method_u == "GET" else "新增或编辑Agent"
    if method_u == "POST" and path.endswith("/public") and path.startswith("/api/masks/"):
        return "设置Agent公开状态"
    if method_u == "DELETE" and path.startswith("/api/masks/"):
        return "删除Agent"
    if path == "/api/admin/app-settings":
        return "修改应用设置"
    if path == "/api/user/retention-settings":
        return "修改个人消息保留策略"
    if path == "/api/default-model":
        return "设置默认模型"
    if path == "/api/default-agent":
        return "设置默认Agent"
    if path == "/api/chats":
        return "读取会话列表" if method_u == "GET" else "创建会话"
    if path == "/api/chats/clear":
        return "清空个人会话数据"
    if method_u == "POST" and path.startswith("/api/chats/") and path.endswith("/config"):
        return "修改会话参数"
    if method_u == "DELETE" and path.startswith("/api/chats/"):
        return "删除会话"
    if path == "/api/chat":
        return "发送消息"
    if path == "/api/chat/stream":
        return "流式发送消息"
    if path == "/api/admin/users":
        return "读取用户列表" if method_u == "GET" else "创建用户"
    if method_u == "POST" and path.startswith("/api/admin/users/") and path.endswith("/password"):
        return "重置用户密码"
    if method_u == "DELETE" and path.startswith("/api/admin/users/"):
        return "删除用户"
    if path == "/api/admin/chats/clear":
        return "清空所有用户会话数据"
    return "API访问"


def _reasoning_effort_label_for_audit(value: Any) -> str:
    effort = _normalize_reasoning_effort(value)
    if effort == "low":
        return "低"
    if effort == "medium":
        return "中"
    if effort == "high":
        return "高"
    if effort == "xhigh":
        return "超高"
    return "无"


def _enrich_request_params_for_audit(
    request: Request,
    request_params: dict[str, Any] | None,
    store: dict[str, Any] | None = None,
    user: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if not isinstance(request_params, dict):
        return request_params
    if request.url.path not in {"/api/chat", "/api/chat/stream"}:
        return request_params
    body = request_params.get("body")
    if not isinstance(body, dict):
        return request_params
    if not isinstance(store, dict):
        return request_params

    chat_id = str(body.get("chat_id") or "").strip()
    user_id = str((user or {}).get("id") or "").strip()
    chat = find_chat_for_user(store, chat_id, user_id) if chat_id and user_id else None

    use_payload_model = bool(body.get("_has_model_id"))
    use_payload_mask = bool(body.get("_has_mask_id"))
    use_payload_reasoning = bool(body.get("_has_reasoning_effort"))

    payload_model_id = str(body.get("model_id") or "").strip()
    payload_model_name = str(body.get("model_name") or "").strip()
    payload_mask_id_raw = body.get("mask_id")
    payload_mask_id = str(payload_mask_id_raw or "").strip() if payload_mask_id_raw is not None else None

    target_model_id = payload_model_id if use_payload_model else str((chat or {}).get("model_id") or "").strip()
    if not target_model_id:
        target_model_id = str((user or {}).get("default_model_id") or "").strip()

    model = _resolve_visible_model(store, user or {}, target_model_id) if target_model_id else None
    if not model and payload_model_name:
        named_model = find_model(store, None, payload_model_name)
        if named_model and ((user or {}).get("role") == "admin" or named_model.get("enabled", True)):
            model = named_model
    if not model:
        model = _pick_first_enabled_model(store)
    model_name = str((model or {}).get("name") or payload_model_name or "(未知模型)")

    target_mask_id = payload_mask_id if use_payload_mask else (chat or {}).get("mask_id")
    mask = _resolve_visible_mask(store, user or {}, target_mask_id) if target_mask_id else None
    mask_name = str((mask or {}).get("name") or "不使用Agent")

    reasoning_source = (
        body.get("reasoning_effort")
        if use_payload_reasoning
        else (chat or {}).get("reasoning_effort")
    )

    enriched_body = {
        "chat_id": chat_id or None,
        "model": model_name,
        "agent": mask_name,
        "推理等级": _reasoning_effort_label_for_audit(reasoning_source),
        "temperature": body.get("temperature"),
        "attachments_count": body.get("attachments_count", 0),
        "message": "[已省略消息内容]",
    }
    request_params["body"] = _sanitize_audit_value(enriched_body)
    return request_params


def _write_audit_log(record: dict[str, Any]) -> None:
    try:
        AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        with AUDIT_LOG_LOCK:
            with AUDIT_LOG_PATH.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
    except Exception as exc:
        LOGGER.warning("write audit log failed: %s", exc)


def _audit_log_api_request(
    request: Request,
    status_code: int,
    user: dict[str, Any] | None,
    request_params: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    user_name = ""
    user_id = ""
    role = ""
    if isinstance(user, dict):
        user_name = str(user.get("username") or "")
        user_id = str(user.get("id") or "")
        role = str(user.get("role") or "")

    record: dict[str, Any] = {
        "时间": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "事件": _describe_api_action(request.method, request.url.path),
        "方法": request.method,
        "路径": request.url.path,
        "状态码": int(status_code),
        "用户": user_name,
        "角色": role,
        "用户ID": user_id,
        "来源IP": _client_ip_for_audit(request),
    }
    if request_params:
        record["请求参数"] = _sanitize_audit_value(request_params)
    if extra:
        record["附加信息"] = _sanitize_audit_value(extra)
    record["结果"] = "成功" if int(status_code) < 400 else "失败"
    _write_audit_log(record)


def _extract_error_detail_from_body(body_bytes: bytes | bytearray | None, content_type: str) -> str | None:
    if not body_bytes or not isinstance(body_bytes, (bytes, bytearray)):
        return None
    content_type = str(content_type or "").lower()
    if "application/json" in content_type:
        try:
            payload = json.loads(body_bytes.decode("utf-8", errors="ignore"))
        except Exception:
            payload = None
        if isinstance(payload, dict):
            detail = payload.get("detail")
            if isinstance(detail, str) and detail.strip():
                return detail.strip()[:500]
            error = payload.get("error")
            if isinstance(error, dict):
                message = error.get("message")
                if isinstance(message, str) and message.strip():
                    return message.strip()[:500]
            message = payload.get("message")
            if isinstance(message, str) and message.strip():
                return message.strip()[:500]
        return None

    if "text/plain" in content_type:
        text = body_bytes.decode("utf-8", errors="ignore").strip()
        if text:
            return text[:500]
    return None


async def _extract_error_detail_from_response(response: Response) -> tuple[Response, str | None]:
    try:
        status_code = int(getattr(response, "status_code", 200))
    except Exception:
        status_code = 200
    if status_code < 400:
        return response, None

    content_type = str(getattr(response, "headers", {}).get("content-type") or "").lower()

    body_bytes = getattr(response, "body", None)
    if isinstance(body_bytes, (bytes, bytearray)):
        return response, _extract_error_detail_from_body(body_bytes, content_type)

    if "text/event-stream" in content_type:
        return response, None

    body_iterator = getattr(response, "body_iterator", None)
    if body_iterator is None:
        return response, None

    chunks: list[bytes] = []
    async for chunk in body_iterator:
        if isinstance(chunk, bytes):
            chunks.append(chunk)
        else:
            chunks.append(str(chunk).encode("utf-8"))
    body_bytes = b"".join(chunks)

    cloned_response = Response(
        content=body_bytes,
        status_code=response.status_code,
        headers=dict(response.headers),
        media_type=getattr(response, "media_type", None),
        background=getattr(response, "background", None),
    )
    return cloned_response, _extract_error_detail_from_body(body_bytes, content_type)


def _mask_api_key_for_display(api_key: Any) -> str:
    raw = str(api_key or "").strip()
    n = len(raw)
    if n == 0:
        return ""
    if n <= 12:
        return "*" * n
    if n <= 17:
        keep = 2
    elif n <= 24:
        keep = 4
    else:
        keep = 6
    hidden = max(0, n - 2 * keep)
    return f"{raw[:keep]}{'*' * hidden}{raw[n - keep:]}"


def _payload_has_field(payload: Any, field_name: str) -> bool:
    fields_set = getattr(payload, "model_fields_set", None)
    if fields_set is None:
        fields_set = getattr(payload, "__fields_set__", set())
    return field_name in fields_set


MAX_ATTACHMENTS_PER_MESSAGE = 8
MAX_ATTACHMENT_NAME_LEN = 255
MAX_ATTACHMENT_MIME_LEN = 128
MAX_ATTACHMENT_TEXT_CHARS = 20000
MAX_ATTACHMENT_DATA_URL_CHARS = 2_500_000
MAX_APP_ICON_DATA_URL_CHARS = 750_000
MAX_APP_ICON_BYTES = 512 * 1024


def _truncate_text(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[:max_len]


def _decode_app_icon_data_url(value: Any) -> tuple[bytes, str] | None:
    raw = "" if value is None else str(value).strip()
    if not raw:
        return None
    if len(raw) > MAX_APP_ICON_DATA_URL_CHARS:
        return None
    lower = raw.lower()
    mime_to_ext = {
        "data:image/png;base64,": "png",
        "data:image/jpeg;base64,": "jpg",
        "data:image/jpg;base64,": "jpg",
        "data:image/webp;base64,": "webp",
        "data:image/gif;base64,": "gif",
        "data:image/svg+xml;base64,": "svg",
        "data:image/x-icon;base64,": "ico",
        "data:image/vnd.microsoft.icon;base64,": "ico",
    }
    ext = ""
    for prefix, candidate_ext in mime_to_ext.items():
        if lower.startswith(prefix):
            ext = candidate_ext
            break
    if not ext:
        return None
    try:
        encoded = raw.split(",", 1)[1]
        binary = base64.b64decode(_clean_base64_text(encoded) + "===", validate=False)
    except Exception:
        return None
    if not binary or len(binary) > MAX_APP_ICON_BYTES:
        return None
    return binary, ext


def _normalize_app_icon_filename(value: Any) -> str:
    raw = Path(str(value or "").strip()).name
    if not raw:
        return ""
    if not re.fullmatch(r"app-icon\.(png|jpg|webp|gif|svg|ico)", raw, flags=re.I):
        return ""
    path = (STATIC_DIR / raw).resolve()
    try:
        path.relative_to(STATIC_DIR)
    except ValueError:
        return ""
    return raw if path.exists() else ""


def _remove_app_icon_files(keep_filename: str = "") -> None:
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    keep = Path(str(keep_filename or "")).name
    for path in STATIC_DIR.glob("app-icon.*"):
        if path.name == keep:
            continue
        if not path.is_file():
            continue
        try:
            path.unlink()
        except Exception:
            pass


def _store_app_icon_data_url(value: Any) -> str:
    decoded = _decode_app_icon_data_url(value)
    if decoded is None:
        return ""
    binary, ext = decoded
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"app-icon.{ext}"
    path = (STATIC_DIR / filename).resolve()
    try:
        path.relative_to(STATIC_DIR)
    except ValueError:
        return ""
    try:
        path.write_bytes(binary)
    except Exception:
        return ""
    _remove_app_icon_files(filename)
    return filename


def _app_icon_url_for_meta(app_meta: dict[str, Any] | None) -> str:
    meta = app_meta or {}
    filename = _normalize_app_icon_filename(meta.get("icon_filename"))
    if not filename:
        return ""
    version = _safe_int(meta.get("updated_at"), 0)
    if version <= 0:
        try:
            version = int((STATIC_DIR / filename).stat().st_mtime)
        except Exception:
            version = int(time.time())
    return f"/static/{filename}?v={version}"


def _serialize_app_meta_for_client(app_meta: dict[str, Any] | None) -> dict[str, Any]:
    meta = app_meta or {}
    legacy_ttl_hours = _safe_int(meta.get("message_ttl_hours"), 0)
    text_ttl_hours = _safe_int(meta.get("message_text_ttl_hours"), legacy_ttl_hours)
    media_ttl_hours = _safe_int(meta.get("message_media_ttl_hours"), legacy_ttl_hours)
    return {
        "title": meta.get("title") or DEFAULT_APP_TITLE,
        "subtitle": meta.get("subtitle") or DEFAULT_APP_SUBTITLE,
        "icon_url": _app_icon_url_for_meta(meta),
        "icon_data_url": "",
        "message_ttl_hours": legacy_ttl_hours,
        "message_text_ttl_hours": text_ttl_hours,
        "message_media_ttl_hours": media_ttl_hours,
    }


def _safe_local_attachment_path_from_url(data_url: str) -> Path | None:
    raw = str(data_url or "").strip()
    if not raw.startswith(ATTACHMENT_FILE_URL_PREFIX):
        return None
    suffix = raw[len(ATTACHMENT_FILE_URL_PREFIX) :].lstrip("/")
    if not suffix:
        return None
    candidate = (ATTACHMENT_FILE_DIR / suffix).resolve()
    try:
        candidate.relative_to(ATTACHMENT_FILE_DIR)
    except ValueError:
        return None
    return candidate


def _store_data_image_to_local_file(data_url: str, mime_hint: str = "") -> tuple[str | None, int | None]:
    raw = str(data_url or "").strip()
    if not raw.startswith("data:image/") or ";base64," not in raw:
        return None, None
    _, encoded = raw.split(",", 1)
    cleaned = _clean_base64_text(encoded)
    if not cleaned:
        return None, None
    try:
        binary = base64.b64decode(cleaned + "===", validate=False)
    except Exception:
        return None, None
    if not binary:
        return None, None

    mime_type = _infer_image_mime_from_data_url(raw)
    if not mime_type.startswith("image/"):
        hint = str(mime_hint or "").strip().lower()
        mime_type = hint if hint.startswith("image/") else "image/png"

    digest = hashlib.sha256(binary).hexdigest()
    ext = _image_extension_from_mime(mime_type)
    filename = f"{digest}.{ext}"
    ATTACHMENT_FILE_DIR.mkdir(parents=True, exist_ok=True)
    file_path = ATTACHMENT_FILE_DIR / filename
    if not file_path.exists():
        try:
            with file_path.open("wb") as f:
                f.write(binary)
        except Exception:
            return None, None
    return f"{ATTACHMENT_FILE_URL_PREFIX}{filename}", len(binary)


def _materialize_attachment_data_url(
    kind: str,
    mime_type: str,
    raw_data_url: Any,
    current_size: int,
) -> tuple[str | None, int]:
    data_url = None if raw_data_url is None else str(raw_data_url).strip()
    size = max(0, _safe_int(current_size, 0))
    if not data_url:
        return None, size

    if kind == "image" and data_url.startswith("data:image/"):
        local_url, local_size = _store_data_image_to_local_file(data_url, mime_type)
        if local_url:
            data_url = local_url
            if local_size is not None and local_size > 0:
                size = local_size

    if len(data_url) > MAX_ATTACHMENT_DATA_URL_CHARS:
        data_url = data_url[:MAX_ATTACHMENT_DATA_URL_CHARS]

    if not (
        data_url.startswith("data:")
        or data_url.startswith("http://")
        or data_url.startswith("https://")
        or data_url.startswith(ATTACHMENT_FILE_URL_PREFIX)
    ):
        return None, size

    local_path = _safe_local_attachment_path_from_url(data_url)
    if local_path is not None and size <= 0:
        try:
            size = max(0, local_path.stat().st_size)
        except FileNotFoundError:
            size = 0
        except Exception:
            pass

    return data_url, size


def _normalize_attachment_kind(kind: str, mime_type: str) -> str:
    raw = (kind or "").strip().lower()
    if raw in {"image", "text", "file"}:
        return raw
    if mime_type.startswith("image/"):
        return "image"
    if mime_type.startswith("text/"):
        return "text"
    return "file"


def _normalize_attachment(att: Any) -> dict[str, Any] | None:
    if not isinstance(att, dict):
        return None
    name = str(att.get("name") or "attachment").strip() or "attachment"
    mime_type = str(att.get("mime_type") or "").strip().lower()
    if not mime_type:
        mime_type = "application/octet-stream"
    kind = _normalize_attachment_kind(str(att.get("kind") or ""), mime_type)
    size = _safe_int(att.get("size"), 0)
    if size < 0:
        size = 0
    data_url, size = _materialize_attachment_data_url(kind, mime_type, att.get("data_url"), size)

    text_content = att.get("text_content")
    if text_content is not None:
        text_content = _truncate_text(str(text_content), MAX_ATTACHMENT_TEXT_CHARS)

    normalized = {
        "name": _truncate_text(name, MAX_ATTACHMENT_NAME_LEN),
        "mime_type": _truncate_text(mime_type, MAX_ATTACHMENT_MIME_LEN),
        "size": size,
        "kind": kind,
    }
    if data_url:
        normalized["data_url"] = data_url
    if text_content:
        normalized["text_content"] = text_content
    return normalized


def _normalize_attachments(attachments: Any) -> list[dict[str, Any]]:
    if not isinstance(attachments, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in attachments[:MAX_ATTACHMENTS_PER_MESSAGE]:
        att = _normalize_attachment(item)
        if att is not None:
            normalized.append(att)
    return normalized


def _clean_base64_text(value: str) -> str:
    return "".join(ch for ch in str(value or "") if ch not in {"\n", "\r", "\t", " "})


def _is_probable_base64_blob(value: str) -> bool:
    cleaned = _clean_base64_text(value)
    if len(cleaned) < 128:
        return False
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=")
    return all(ch in allowed for ch in cleaned)


def _infer_image_mime_from_data_url(data_url: str) -> str:
    raw = str(data_url or "")
    if raw.startswith("data:image/"):
        prefix = raw[5:].split(",", 1)[0]
        mime = prefix.split(";", 1)[0]
        if mime.startswith("image/"):
            return mime
    return "image/png"


def _infer_image_mime_from_url(url: str) -> str:
    lower = str(url or "").lower()
    if ".png" in lower:
        return "image/png"
    if ".jpg" in lower or ".jpeg" in lower:
        return "image/jpeg"
    if ".webp" in lower:
        return "image/webp"
    if ".gif" in lower:
        return "image/gif"
    return "image/png"


def _image_extension_from_mime(mime_type: str) -> str:
    lower = str(mime_type or "").lower()
    if lower == "image/jpeg":
        return "jpg"
    if lower.startswith("image/"):
        return lower.split("/", 1)[1]
    return "png"


def _build_image_attachment_from_data_url(data_url: str, index: int) -> dict[str, Any] | None:
    url = str(data_url or "").strip()
    if not url:
        return None
    if not (
        url.startswith("data:image/")
        or url.startswith("http://")
        or url.startswith("https://")
    ):
        return None

    mime_type = _infer_image_mime_from_data_url(url) if url.startswith("data:") else _infer_image_mime_from_url(url)
    size = 0
    if url.startswith("data:") and ";base64," in url:
        b64 = _clean_base64_text(url.split(",", 1)[1])
        try:
            size = len(base64.b64decode(b64 + "===", validate=False))
        except Exception:
            size = max(0, (len(b64) * 3) // 4)
    return {
        "name": f"generated-image-{index}.{_image_extension_from_mime(mime_type)}",
        "mime_type": mime_type,
        "size": size,
        "kind": "image",
        "data_url": url,
    }


def _build_image_attachment_from_base64(base64_text: str, index: int, mime_hint: str = "image/png") -> dict[str, Any] | None:
    cleaned = _clean_base64_text(base64_text)
    if not _is_probable_base64_blob(cleaned):
        return None
    mime_type = str(mime_hint or "").strip().lower()
    if not mime_type.startswith("image/"):
        mime_type = "image/png"
    data_url = f"data:{mime_type};base64,{cleaned}"
    try:
        size = len(base64.b64decode(cleaned + "===", validate=False))
    except Exception:
        size = max(0, (len(cleaned) * 3) // 4)
    return {
        "name": f"generated-image-{index}.{_image_extension_from_mime(mime_type)}",
        "mime_type": mime_type,
        "size": size,
        "kind": "image",
        "data_url": data_url,
    }


def _extract_first_svg_xml_block(value: Any) -> str | None:
    text = str(value or "")
    if not text:
        return None
    match = re.search(r"<svg\b[\s\S]*?</svg>", text, flags=re.IGNORECASE)
    if not match:
        return None
    block = str(match.group(0) or "").strip()
    return block or None


def _sanitize_svg_xml_text(svg_text: Any) -> str | None:
    candidate = _extract_first_svg_xml_block(svg_text)
    if not candidate:
        return None

    lowered = candidate.lower()
    if "<script" in lowered:
        return None
    if len(candidate) > 2_000_000:
        return None

    try:
        root = ET.fromstring(candidate)
    except Exception:
        return None

    tag = str(root.tag or "")
    namespace = ""
    local_name = tag
    if tag.startswith("{") and "}" in tag:
        namespace, local_name = tag[1:].split("}", 1)

    if local_name.lower() != "svg":
        return None
    if namespace and namespace.strip().lower() != "http://www.w3.org/2000/svg":
        return None

    return candidate


def _store_svg_text_to_local_file(svg_text: str) -> tuple[str | None, int]:
    text = _sanitize_svg_xml_text(svg_text)
    if not text:
        return None, 0

    binary = text.encode("utf-8", errors="ignore")
    if not binary:
        return None, 0

    digest = hashlib.sha256(binary).hexdigest()
    filename = f"{digest}.svg"
    ATTACHMENT_FILE_DIR.mkdir(parents=True, exist_ok=True)
    file_path = ATTACHMENT_FILE_DIR / filename
    if not file_path.exists():
        try:
            with file_path.open("wb") as f:
                f.write(binary)
        except Exception:
            return None, 0
    return f"{ATTACHMENT_FILE_URL_PREFIX}{filename}", len(binary)


def _extract_svg_attachments_from_text(text: str, start_index: int = 1) -> list[dict[str, Any]]:
    source = str(text or "")
    if "<svg" not in source.lower():
        return []

    # Scan all SVG XML fragments directly from raw text, independent of markdown
    # fence formatting, so multiple SVG blocks in one message are handled robustly.
    inline_pattern = re.compile(r"<svg\b[\s\S]*?</svg>", re.IGNORECASE)
    svg_chunks: list[str] = []
    for match in inline_pattern.finditer(source):
        chunk = str(match.group(0) or "").strip()
        if chunk:
            svg_chunks.append(chunk)

    raw_attachments: list[dict[str, Any]] = []
    index = max(1, _safe_int(start_index, 1))
    for chunk in svg_chunks[:MAX_ATTACHMENTS_PER_MESSAGE]:
        local_url, local_size = _store_svg_text_to_local_file(chunk)
        if not local_url:
            continue
        raw_attachments.append(
            {
                "name": f"generated-image-{index}.svg",
                "mime_type": "image/svg+xml",
                "size": local_size,
                "kind": "image",
                "data_url": local_url,
            }
        )
        index += 1

    normalized = _normalize_attachments(raw_attachments)
    unique: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for att in normalized:
        data_url = str(att.get("data_url") or "")
        if not data_url or data_url in seen_urls:
            continue
        seen_urls.add(data_url)
        unique.append(att)
    return unique


def _extract_generated_image_attachments(payload: Any) -> list[dict[str, Any]]:
    found_data_urls: list[str] = []
    found_base64: list[tuple[str, str]] = []

    def _maybe_add_url(value: Any, *, loose_http: bool = False) -> None:
        if not isinstance(value, str):
            return
        url = value.strip()
        if not url:
            return
        lower = url.lower()
        if lower.startswith("data:image/"):
            found_data_urls.append(url)
            return
        if lower.startswith("http://") or lower.startswith("https://"):
            if loose_http or any(ext in lower for ext in (".png", ".jpg", ".jpeg", ".webp", ".gif")):
                found_data_urls.append(url)

    def _maybe_add_b64(value: Any, mime_hint: str = "image/png") -> None:
        if not isinstance(value, str):
            return
        text = value.strip()
        if not text:
            return
        if text.startswith("data:image/"):
            found_data_urls.append(text)
            return
        if _is_probable_base64_blob(text):
            found_base64.append((text, mime_hint))

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            mime_hint = str(node.get("mime_type") or node.get("mime") or "image/png")

            image_url = node.get("image_url")
            if isinstance(image_url, dict):
                _maybe_add_url(image_url.get("url"), loose_http=True)
                _maybe_add_b64(image_url.get("b64_json"), mime_hint)
            else:
                _maybe_add_url(image_url, loose_http=True)

            _maybe_add_url(node.get("url"))
            _maybe_add_b64(node.get("b64_json"), mime_hint)
            _maybe_add_b64(node.get("image_base64"), mime_hint)
            _maybe_add_b64(node.get("base64"), mime_hint)
            _maybe_add_b64(node.get("image_b64"), mime_hint)

            for key, value in node.items():
                if key in {"image_url", "url", "b64_json", "image_base64", "base64", "image_b64"}:
                    continue
                if key == "result":
                    _maybe_add_b64(value, mime_hint)
                    _maybe_add_url(value, loose_http=True)
                _walk(value)
            return
        if isinstance(node, list):
            for item in node:
                _walk(item)

    roots: list[Any] = []
    if isinstance(payload, dict):
        for key in ("output", "choices", "data", "response", "result", "images"):
            if key in payload:
                roots.append(payload.get(key))
        if not roots:
            roots.append(payload)
    else:
        roots.append(payload)

    for root in roots:
        _walk(root)

    raw_attachments: list[dict[str, Any]] = []
    index = 1
    for data_url in found_data_urls:
        att = _build_image_attachment_from_data_url(data_url, index)
        if att:
            raw_attachments.append(att)
            index += 1
    for base64_text, mime_hint in found_base64:
        att = _build_image_attachment_from_base64(base64_text, index, mime_hint=mime_hint)
        if att:
            raw_attachments.append(att)
            index += 1

    normalized = _normalize_attachments(raw_attachments)
    unique: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for att in normalized:
        data_url = str(att.get("data_url") or "")
        if not data_url or data_url in seen_urls:
            continue
        seen_urls.add(data_url)
        unique.append(att)
    return unique


def _merge_attachments_unique(base: list[dict[str, Any]], extra: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not extra:
        return base
    seen_urls = {str(item.get("data_url") or "") for item in base if str(item.get("data_url") or "")}
    for item in extra:
        data_url = str(item.get("data_url") or "")
        if not data_url or data_url in seen_urls:
            continue
        seen_urls.add(data_url)
        base.append(item)
    return base


def _dedupe_stream_piece(piece: str, emitted_text: str) -> str:
    raw = str(piece or "")
    if not raw:
        return ""
    if not emitted_text:
        return raw
    if raw.startswith(emitted_text):
        return raw[len(emitted_text) :]
    return raw


def _looks_like_svg_text_request(message_text: str) -> bool:
    text = str(message_text or "").strip().lower()
    if not text:
        return False
    keywords = (
        "svg",
        "<svg",
        "```svg",
        ".svg",
        "svg图",
        "svg图片",
        "svg代码",
        "输出svg",
        "返回svg",
        "生成svg",
        "写svg",
        "保存为svg",
        "保存为.svg",
        "矢量",
        "矢量图",
        "vector",
        "xml svg",
        "logo代码",
        "图标代码",
        "icon代码",
    )
    return any(word in text for word in keywords)


def _looks_like_image_generation_request(message_text: str, model_name: str) -> bool:
    text = str(message_text or "").strip().lower()
    if not text:
        return False
    if _looks_like_svg_text_request(text):
        return False
    model_lower = str(model_name or "").strip().lower()
    if "image" in model_lower:
        return True
    keywords = (
        "生成图片",
        "生成图像",
        "画一张",
        "画个",
        "画图",
        "出图",
        "做图",
        "海报",
        "插画",
        "logo",
        "poster",
        "draw",
        "generate image",
        "create image",
    )
    return any(word in text for word in keywords)


def _is_image_generation_model(model_name: str) -> bool:
    model_lower = str(model_name or "").strip().lower()
    if not model_lower:
        return False
    return any(token in model_lower for token in ("gpt-image", "image-gen", "image"))


def _build_images_generations_url(base_url: str) -> str:
    trimmed = str(base_url or "").strip().rstrip("/")
    if not trimmed:
        raise HTTPException(status_code=400, detail="模型未配置 base_url")
    if not trimmed.startswith("http"):
        trimmed = f"https://{trimmed}"

    parsed = urlsplit(trimmed)
    path = parsed.path.rstrip("/")
    lower_path = path.lower()

    if lower_path.endswith("/images/generations"):
        target_path = path
    elif lower_path.endswith("/v1/responses"):
        target_path = f"{path[:-len('/responses')]}/images/generations"
    elif lower_path.endswith("/responses"):
        target_path = f"{path[:-len('/responses')]}/images/generations"
    elif lower_path.endswith("/chat/completions"):
        target_path = f"{path[:-len('/chat/completions')]}/images/generations"
    elif lower_path.endswith("/v1"):
        target_path = f"{path}/images/generations"
    else:
        target_path = f"{path}/v1/images/generations"

    if not target_path.startswith("/"):
        target_path = f"/{target_path}"
    return urlunsplit((parsed.scheme, parsed.netloc, target_path, "", ""))


def _resolve_upstream_target(base_url: str, model_name: str, message_text: str) -> tuple[str, str]:
    url, protocol = build_upstream_url(base_url)
    if protocol != "images_generations":
        if _is_image_generation_model(model_name) and _looks_like_image_generation_request(message_text, model_name):
            return _build_images_generations_url(base_url), "images_generations"
    return url, protocol


def default_store() -> dict[str, Any]:
    ts = now_ts()
    return {
        "version": 2,
        "updated_at": ts,
        "app": {
            "title": DEFAULT_APP_TITLE,
            "subtitle": DEFAULT_APP_SUBTITLE,
            "icon_filename": "",
            "message_text_ttl_hours": 0,
            "message_media_ttl_hours": 0,
            "message_ttl_hours": 0,
            "session_secret": secrets.token_hex(32),
            "created_at": ts,
            "updated_at": ts,
        },
        "users": [],
        "models": [],
        "masks": [],
        "chats": [],
    }


def _normalize_model(model: Any) -> dict[str, Any] | None:
    if not isinstance(model, dict):
        return None
    ts = now_ts()
    model_id = str(model.get("id") or "").strip() or new_id()
    return {
        "id": model_id,
        "name": str(model.get("name") or DEFAULT_MODEL_NAME).strip() or DEFAULT_MODEL_NAME,
        "provider": str(model.get("provider") or "openai-compatible").strip() or "openai-compatible",
        "base_url": str(model.get("base_url") or "").strip(),
        "api_key": str(model.get("api_key") or "").strip(),
        "enabled": _normalize_bool(model.get("enabled"), True),
        "use_streaming": _normalize_bool(model.get("use_streaming"), True),
        "created_at": _safe_int(model.get("created_at"), ts),
        "updated_at": _safe_int(model.get("updated_at"), ts),
    }


def _normalize_message(msg: Any) -> dict[str, Any] | None:
    if not isinstance(msg, dict):
        return None
    role = str(msg.get("role") or "").strip().lower()
    if role not in {"user", "assistant", "system"}:
        return None
    content = str(msg.get("content") or "")
    attachments = _normalize_attachments(msg.get("attachments"))
    if not content and role != "assistant" and not attachments:
        return None
    normalized = {
        "id": str(msg.get("id") or "").strip() or new_id(),
        "role": role,
        "content": content,
        "created_at": _safe_int(msg.get("created_at"), now_ts()),
    }
    mask_id = msg.get("mask_id")
    if mask_id is not None:
        normalized["mask_id"] = str(mask_id)
    agent_name = str(msg.get("agent_name") or "").strip()
    if agent_name:
        normalized["agent_name"] = agent_name
    if attachments:
        normalized["attachments"] = attachments
    return normalized


def _normalize_chat(chat: Any) -> dict[str, Any] | None:
    if not isinstance(chat, dict):
        return None
    ts = now_ts()
    messages = []
    for m in chat.get("messages", []):
        normalized = _normalize_message(m)
        if normalized is not None:
            messages.append(normalized)
    return {
        "id": str(chat.get("id") or "").strip() or new_id(),
        "user_id": chat.get("user_id"),
        "title": str(chat.get("title") or "").strip() or "新对话",
        "model_id": chat.get("model_id"),
        "mask_id": chat.get("mask_id"),
        "reasoning_effort": _normalize_reasoning_effort(chat.get("reasoning_effort")),
        "created_at": _safe_int(chat.get("created_at"), ts),
        "updated_at": _safe_int(chat.get("updated_at"), ts),
        "messages": messages,
    }


def _normalize_mask(mask: Any) -> dict[str, Any] | None:
    if not isinstance(mask, dict):
        return None
    ts = now_ts()
    name = str(mask.get("name") or "").strip() or "新Agent"
    prompt = str(mask.get("prompt") or "")
    builtin_def = _get_builtin_agent_definition_by_name(name)
    is_builtin = _normalize_bool(mask.get("is_builtin"), False)
    is_public = _normalize_bool(mask.get("is_public"), False)
    if builtin_def:
        is_public = True
        is_builtin = True
        if not prompt.strip():
            prompt = str(builtin_def.get("prompt") or "")
    return {
        "id": str(mask.get("id") or "").strip() or new_id(),
        "name": name,
        "prompt": prompt,
        "owner_user_id": mask.get("owner_user_id"),
        "is_public": is_public,
        "is_builtin": is_builtin,
        "created_at": _safe_int(mask.get("created_at"), ts),
        "updated_at": _safe_int(mask.get("updated_at"), ts),
    }


def _normalize_user(
    user: Any,
    default_text_ttl_hours: int = 0,
    default_media_ttl_hours: int = 0,
) -> dict[str, Any] | None:
    if not isinstance(user, dict):
        return None
    ts = now_ts()
    username = _normalize_username(user.get("username"))
    if not username:
        return None
    password_hash = str(user.get("password_sha256") or "").strip().lower()
    return {
        "id": str(user.get("id") or "").strip() or new_id(),
        "username": username,
        "password_sha256": password_hash,
        "role": _normalize_role(user.get("role")),
        "enabled": _normalize_bool(user.get("enabled"), True),
        "default_model_id": user.get("default_model_id"),
        "default_mask_id": user.get("default_mask_id"),
        "message_text_ttl_hours": _normalize_retention_hours(
            user.get("message_text_ttl_hours"),
            default_text_ttl_hours,
        ),
        "message_media_ttl_hours": _normalize_retention_hours(
            user.get("message_media_ttl_hours"),
            default_media_ttl_hours,
        ),
        "created_at": _safe_int(user.get("created_at"), ts),
        "updated_at": _safe_int(user.get("updated_at"), ts),
    }


def _normalize_v2_store(data: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    changed = False
    ts = now_ts()

    if not isinstance(data.get("app"), dict):
        data["app"] = {}
        changed = True
    app_meta = data["app"]
    if not isinstance(app_meta.get("title"), str) or not app_meta.get("title").strip():
        app_meta["title"] = DEFAULT_APP_TITLE
        changed = True
    if not isinstance(app_meta.get("subtitle"), str) or not app_meta.get("subtitle").strip():
        app_meta["subtitle"] = DEFAULT_APP_SUBTITLE
        changed = True
    legacy_icon_data_url = str(app_meta.get("icon_data_url") or "").strip()
    if legacy_icon_data_url:
        icon_filename = _store_app_icon_data_url(legacy_icon_data_url)
        app_meta["icon_filename"] = icon_filename
        app_meta.pop("icon_data_url", None)
        changed = True
    else:
        icon_filename = _normalize_app_icon_filename(app_meta.get("icon_filename"))
        if app_meta.get("icon_filename") != icon_filename:
            app_meta["icon_filename"] = icon_filename
            changed = True
    legacy_ttl = _normalize_retention_hours(app_meta.get("message_ttl_hours"), 0)
    text_ttl = _normalize_retention_hours(app_meta.get("message_text_ttl_hours"), legacy_ttl)
    media_ttl = _normalize_retention_hours(app_meta.get("message_media_ttl_hours"), legacy_ttl)

    if app_meta.get("message_text_ttl_hours") != text_ttl:
        app_meta["message_text_ttl_hours"] = text_ttl
        changed = True
    if app_meta.get("message_media_ttl_hours") != media_ttl:
        app_meta["message_media_ttl_hours"] = media_ttl
        changed = True

    compatibility_ttl = (
        text_ttl
        if text_ttl == media_ttl
        else min((ttl for ttl in (text_ttl, media_ttl) if ttl > 0), default=0)
    )
    if app_meta.get("message_ttl_hours") != compatibility_ttl:
        app_meta["message_ttl_hours"] = compatibility_ttl
        changed = True
    secret = str(app_meta.get("session_secret") or "").strip()
    if not secret:
        app_meta["session_secret"] = secrets.token_hex(32)
        changed = True
    if not isinstance(app_meta.get("created_at"), int):
        app_meta["created_at"] = ts
        changed = True
    if not isinstance(app_meta.get("updated_at"), int):
        app_meta["updated_at"] = ts
        changed = True

    raw_users = data.get("users")
    if not isinstance(raw_users, list):
        raw_users = []
        changed = True
    normalized_users: list[dict[str, Any]] = []
    seen_user_ids: set[str] = set()
    seen_usernames: set[str] = set()
    for user in raw_users:
        normalized = _normalize_user(
            user,
            default_text_ttl_hours=text_ttl,
            default_media_ttl_hours=media_ttl,
        )
        if normalized is None:
            changed = True
            continue
        uid = normalized["id"]
        uname_key = normalized["username"].lower()
        if uid in seen_user_ids or uname_key in seen_usernames:
            changed = True
            continue
        seen_user_ids.add(uid)
        seen_usernames.add(uname_key)
        normalized_users.append(normalized)
    if normalized_users != raw_users:
        changed = True
    data["users"] = normalized_users

    raw_models = data.get("models")
    if not isinstance(raw_models, list):
        raw_models = []
        changed = True
    normalized_models: list[dict[str, Any]] = []
    seen_model_ids: set[str] = set()
    for model in raw_models:
        normalized = _normalize_model(model)
        if normalized is None:
            changed = True
            continue
        mid = normalized["id"]
        if mid in seen_model_ids:
            changed = True
            continue
        seen_model_ids.add(mid)
        normalized_models.append(normalized)
    if normalized_models != raw_models:
        changed = True
    data["models"] = normalized_models

    raw_masks = data.get("masks")
    if not isinstance(raw_masks, list):
        raw_masks = []
        changed = True
    normalized_masks: list[dict[str, Any]] = []
    seen_mask_ids: set[str] = set()
    for mask in raw_masks:
        normalized = _normalize_mask(mask)
        if normalized is None:
            changed = True
            continue
        mid = normalized["id"]
        if mid in seen_mask_ids:
            changed = True
            continue
        seen_mask_ids.add(mid)
        normalized_masks.append(normalized)
    if normalized_masks != raw_masks:
        changed = True
    data["masks"] = normalized_masks

    raw_chats = data.get("chats")
    if not isinstance(raw_chats, list):
        raw_chats = []
        changed = True
    normalized_chats: list[dict[str, Any]] = []
    seen_chat_ids: set[str] = set()
    for chat in raw_chats:
        normalized = _normalize_chat(chat)
        if normalized is None:
            changed = True
            continue
        cid = normalized["id"]
        if cid in seen_chat_ids:
            changed = True
            continue
        seen_chat_ids.add(cid)
        normalized_chats.append(normalized)
    if normalized_chats != raw_chats:
        changed = True
    data["chats"] = normalized_chats

    if not isinstance(data.get("updated_at"), int):
        data["updated_at"] = ts
        changed = True

    return data, changed


def _migrate_v1_to_v2(data: dict[str, Any]) -> dict[str, Any]:
    ts = now_ts()
    migrated = default_store()
    migrated["updated_at"] = _safe_int(data.get("updated_at"), ts)
    migrated["app"]["legacy_default_mask_id"] = data.get("default_mask_id")

    models: list[dict[str, Any]] = []
    for model in data.get("models", []):
        normalized = _normalize_model(model)
        if normalized is not None:
            models.append(normalized)
    migrated["models"] = models

    masks: list[dict[str, Any]] = []
    for mask in data.get("masks", []):
        normalized = _normalize_mask(mask)
        if normalized is None:
            continue
        normalized["owner_user_id"] = None
        if normalized["name"] in BUILTIN_AGENT_NAME_SET:
            normalized["is_public"] = True
            normalized["is_builtin"] = True
        masks.append(normalized)
    migrated["masks"] = masks

    chats: list[dict[str, Any]] = []
    for chat in data.get("chats", []):
        normalized = _normalize_chat(chat)
        if normalized is None:
            continue
        normalized["user_id"] = None
        chats.append(normalized)
    migrated["chats"] = chats

    return migrated


def _normalize_store(data: Any) -> tuple[dict[str, Any], bool]:
    if not isinstance(data, dict):
        return default_store(), True

    version = _safe_int(data.get("version"), 1)
    if version < 2:
        migrated = _migrate_v1_to_v2(data)
        normalized, _ = _normalize_v2_store(migrated)
        normalized["version"] = 2
        return normalized, True

    data["version"] = 2
    normalized, changed = _normalize_v2_store(data)
    return normalized, changed


def ensure_data_file() -> None:
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DATA_PATH.exists():
        if LEGACY_DATA_PATH.exists() and LEGACY_DATA_PATH != DATA_PATH:
            try:
                with DATA_PATH.open("r", encoding="utf-8") as f:
                    current = json.load(f)
                with LEGACY_DATA_PATH.open("r", encoding="utf-8") as f:
                    legacy = json.load(f)
                current_has_data = bool(current.get("users") or current.get("models") or current.get("masks") or current.get("chats"))
                legacy_has_data = bool(legacy.get("models") or legacy.get("masks") or legacy.get("chats"))
                if not current_has_data and legacy_has_data:
                    shutil.copyfile(LEGACY_DATA_PATH, DATA_PATH)
            except Exception:
                pass
        return

    if LEGACY_DATA_PATH.exists() and LEGACY_DATA_PATH != DATA_PATH:
        shutil.copyfile(LEGACY_DATA_PATH, DATA_PATH)
        return
    with DATA_PATH.open("w", encoding="utf-8") as f:
        json.dump(default_store(), f, ensure_ascii=False, indent=2)


def _save_store_unlocked(data: dict[str, Any]) -> None:
    data["updated_at"] = now_ts()
    if isinstance(data.get("app"), dict):
        data["app"]["updated_at"] = now_ts()
    tmp_file = DATA_PATH.with_suffix(".tmp")
    with tmp_file.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp_file.replace(DATA_PATH)


def _message_uses_media_retention(message: dict[str, Any]) -> bool:
    attachments = message.get("attachments")
    if not isinstance(attachments, list):
        return False
    for item in attachments:
        if not isinstance(item, dict):
            continue
        kind = _normalize_attachment_kind(str(item.get("kind") or ""), str(item.get("mime_type") or ""))
        if kind in {"image", "file"}:
            return True
    return False


def _iter_message_local_attachment_paths(message: dict[str, Any]) -> list[Path]:
    attachments = message.get("attachments")
    if not isinstance(attachments, list):
        return []
    paths: list[Path] = []
    for item in attachments:
        if not isinstance(item, dict):
            continue
        local_path = _safe_local_attachment_path_from_url(str(item.get("data_url") or ""))
        if local_path is not None:
            paths.append(local_path)
    return paths


def _normalize_attachment_url_for_compare(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw.startswith(ATTACHMENT_FILE_URL_PREFIX):
        return ""
    suffix = raw[len(ATTACHMENT_FILE_URL_PREFIX) :].lstrip("/")
    if not suffix:
        return ""
    return f"{ATTACHMENT_FILE_URL_PREFIX}{suffix}"


def _user_can_access_attachment_url(store: dict[str, Any], user: dict[str, Any], attachment_url: str) -> bool:
    normalized_url = _normalize_attachment_url_for_compare(attachment_url)
    if not normalized_url:
        return False
    if str(user.get("role") or "").lower() == "admin":
        return True

    user_id = str(user.get("id") or "").strip()
    if not user_id:
        return False

    for chat in store.get("chats", []):
        if str(chat.get("user_id") or "") != user_id:
            continue
        messages = chat.get("messages")
        if not isinstance(messages, list):
            continue
        for message in messages:
            if not isinstance(message, dict):
                continue
            attachments = message.get("attachments")
            if not isinstance(attachments, list):
                continue
            for item in attachments:
                if not isinstance(item, dict):
                    continue
                candidate = _normalize_attachment_url_for_compare(item.get("data_url"))
                if candidate and candidate == normalized_url:
                    return True
    return False


def _iter_chat_local_attachment_paths(chat: dict[str, Any]) -> list[Path]:
    messages = chat.get("messages")
    if not isinstance(messages, list):
        return []
    paths: list[Path] = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        paths.extend(_iter_message_local_attachment_paths(message))
    return paths


def _collect_store_local_attachment_paths(store: dict[str, Any]) -> set[Path]:
    used: set[Path] = set()
    for chat in store.get("chats", []):
        if not isinstance(chat, dict):
            continue
        for path in _iter_chat_local_attachment_paths(chat):
            used.add(path)
    return used


def _delete_local_attachment_files(
    candidates: set[Path],
    kept_paths: set[Path] | None = None,
) -> bool:
    if not candidates:
        return False
    changed = False
    kept = kept_paths or set()
    for path in sorted(candidates):
        if path in kept:
            continue
        try:
            path.relative_to(ATTACHMENT_FILE_DIR)
        except ValueError:
            continue
        try:
            if path.exists():
                path.unlink()
                changed = True
        except Exception as exc:
            LOGGER.warning("delete attachment file failed: path=%s err=%s", path, exc)
    return changed


def _retention_settings_for_user(user: dict[str, Any] | None, app_meta: dict[str, Any] | None = None) -> tuple[int, int]:
    legacy_ttl = _normalize_retention_hours((app_meta or {}).get("message_ttl_hours"), 0)
    text_ttl_hours = _normalize_retention_hours(
        (user or {}).get("message_text_ttl_hours"),
        legacy_ttl,
    )
    media_ttl_hours = _normalize_retention_hours(
        (user or {}).get("message_media_ttl_hours"),
        legacy_ttl,
    )
    return text_ttl_hours, media_ttl_hours


def _apply_message_retention(store: dict[str, Any]) -> bool:
    app_meta = store.get("app") or {}
    current_ts = now_ts()
    empty_chat_grace_seconds = 180
    changed = False
    kept_chats: list[dict[str, Any]] = []
    candidate_delete_files: set[Path] = set()
    kept_files: set[Path] = set()
    users_by_id = {
        str(user.get("id") or ""): user
        for user in store.get("users", [])
        if isinstance(user, dict)
    }

    for chat in store.get("chats", []):
        owner_user = users_by_id.get(str(chat.get("user_id") or ""))
        text_ttl_hours, media_ttl_hours = _retention_settings_for_user(owner_user, app_meta)
        text_cutoff = current_ts - text_ttl_hours * 3600 if text_ttl_hours > 0 else None
        media_cutoff = current_ts - media_ttl_hours * 3600 if media_ttl_hours > 0 else None
        active_cutoffs = [c for c in (text_cutoff, media_cutoff) if c is not None]
        empty_chat_cutoff = min(active_cutoffs) if active_cutoffs else None
        messages = chat.get("messages", [])
        kept_messages: list[dict[str, Any]] = []
        for msg in messages:
            if not isinstance(msg, dict):
                changed = True
                continue
            msg_local_files = set(_iter_message_local_attachment_paths(msg))
            cutoff = media_cutoff if _message_uses_media_retention(msg) else text_cutoff
            created_at = _safe_int(msg.get("created_at"), current_ts)
            if cutoff is not None and created_at < cutoff:
                candidate_delete_files.update(msg_local_files)
                changed = True
                continue
            kept_files.update(msg_local_files)
            kept_messages.append(msg)
        if len(kept_messages) != len(messages):
            changed = True
        if not kept_messages:
            if messages:
                # All existing messages in this chat were removed by retention,
                # so remove the chat itself.
                changed = True
                continue
            # Empty chats can still exist right after "new chat". Keep a short grace
            # period to avoid deleting them immediately, then clean stale empty chats.
            chat_ts = _safe_int(chat.get("updated_at"), _safe_int(chat.get("created_at"), current_ts))
            if current_ts - chat_ts <= empty_chat_grace_seconds:
                kept_chats.append(chat)
                continue
            if empty_chat_cutoff is not None and chat_ts < empty_chat_cutoff:
                changed = True
                continue
            kept_chats.append(chat)
            continue
        if kept_messages != messages:
            chat["messages"] = kept_messages
            changed = True
        latest_msg_ts = max(_safe_int(m.get("created_at"), chat.get("updated_at", now_ts())) for m in kept_messages)
        if _safe_int(chat.get("updated_at"), 0) < latest_msg_ts:
            chat["updated_at"] = latest_msg_ts
            changed = True
        kept_chats.append(chat)

    if len(kept_chats) != len(store.get("chats", [])):
        changed = True
        store["chats"] = kept_chats

    if _delete_local_attachment_files(candidate_delete_files, kept_paths=kept_files):
        changed = True

    return changed


def _clear_chats_in_store(store: dict[str, Any], user_id: str | None = None) -> int:
    removed_files: set[Path] = set()
    kept_chats: list[dict[str, Any]] = []
    removed_count = 0

    for chat in store.get("chats", []):
        if not isinstance(chat, dict):
            continue
        chat_user_id = str(chat.get("user_id") or "")
        should_remove = user_id is None or chat_user_id == str(user_id or "")
        if should_remove:
            removed_count += 1
            removed_files.update(_iter_chat_local_attachment_paths(chat))
            continue
        kept_chats.append(chat)

    if removed_count <= 0:
        return 0

    store["chats"] = kept_chats
    kept_files = _collect_store_local_attachment_paths(store)
    _delete_local_attachment_files(removed_files, kept_paths=kept_files)
    return removed_count


def load_store() -> dict[str, Any]:
    ensure_data_file()
    with STORAGE_LOCK:
        with DATA_PATH.open("r", encoding="utf-8") as f:
            raw_data = json.load(f)
        store, normalized_changed = _normalize_store(raw_data)
        builtin_changed = _ensure_store_builtin_agents(store)
        retention_changed = _apply_message_retention(store)
        if normalized_changed or builtin_changed or retention_changed:
            _save_store_unlocked(store)
        return store


def save_store(data: dict[str, Any]) -> None:
    with STORAGE_LOCK:
        normalized, _ = _normalize_store(data)
        _ensure_store_builtin_agents(normalized)
        _apply_message_retention(normalized)
        _save_store_unlocked(normalized)


def is_initialized(store: dict[str, Any]) -> bool:
    users = store.get("users") or []
    return isinstance(users, list) and len(users) > 0


def find_user_by_id(store: dict[str, Any], user_id: str | None) -> dict[str, Any] | None:
    if not user_id:
        return None
    for user in store.get("users", []):
        if user.get("id") == user_id:
            return user
    return None


def find_user_by_username(store: dict[str, Any], username: str) -> dict[str, Any] | None:
    target = _normalize_username(username).lower()
    if not target:
        return None
    for user in store.get("users", []):
        if _normalize_username(user.get("username")).lower() == target:
            return user
    return None


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64url_decode(text: str) -> bytes:
    padded = text + ("=" * ((4 - len(text) % 4) % 4))
    return base64.urlsafe_b64decode(padded.encode("ascii"))


def create_session_token(store: dict[str, Any], user_id: str) -> str:
    app_meta = store.get("app") or {}
    secret = str(app_meta.get("session_secret") or "").strip()
    if not secret:
        secret = secrets.token_hex(32)
        app_meta["session_secret"] = secret
        store["app"] = app_meta
        save_store(store)

    payload = {
        "uid": user_id,
        "exp": now_ts() + SESSION_TTL_SECONDS,
    }
    payload_text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    payload_part = _b64url_encode(payload_text.encode("utf-8"))
    sig = hmac.new(secret.encode("utf-8"), payload_part.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{payload_part}.{sig}"


def verify_session_token(store: dict[str, Any], token: str | None) -> str | None:
    if not token or "." not in token:
        return None
    payload_part, got_sig = token.split(".", 1)
    app_meta = store.get("app") or {}
    secret = str(app_meta.get("session_secret") or "").strip()
    if not secret:
        return None
    expected_sig = hmac.new(secret.encode("utf-8"), payload_part.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected_sig, got_sig):
        return None
    try:
        payload = json.loads(_b64url_decode(payload_part).decode("utf-8"))
    except Exception:
        return None
    exp = _safe_int(payload.get("exp"), 0)
    if exp < now_ts():
        return None
    uid = str(payload.get("uid") or "").strip()
    return uid or None


def get_current_user_from_request(request: Request) -> dict[str, Any]:
    cached = getattr(request.state, "current_user", None)
    if isinstance(cached, dict):
        return cached

    store = load_store()
    if not is_initialized(store):
        raise HTTPException(status_code=409, detail="应用尚未初始化，请先完成首次设置")

    user_id = verify_session_token(store, request.cookies.get(SESSION_COOKIE))
    user = find_user_by_id(store, user_id)
    if not user or not user.get("enabled", True):
        raise HTTPException(status_code=401, detail="未登录或会话无效")
    request.state.current_user = user
    return user


def require_auth(request: Request) -> dict[str, Any]:
    return get_current_user_from_request(request)


def require_admin(user: dict[str, Any] = Depends(require_auth)) -> dict[str, Any]:
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="仅管理员可执行此操作")
    return user


def sanitize_user_for_client(user: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": user.get("id"),
        "username": user.get("username"),
        "role": user.get("role"),
        "enabled": bool(user.get("enabled", True)),
        "default_model_id": user.get("default_model_id"),
        "default_mask_id": user.get("default_mask_id"),
        "message_text_ttl_hours": _normalize_retention_hours(user.get("message_text_ttl_hours"), 0),
        "message_media_ttl_hours": _normalize_retention_hours(user.get("message_media_ttl_hours"), 0),
        "created_at": user.get("created_at"),
        "updated_at": user.get("updated_at"),
    }


def find_chat_for_user(store: dict[str, Any], chat_id: str, user_id: str) -> dict[str, Any] | None:
    for chat in store.get("chats", []):
        if chat.get("id") == chat_id and chat.get("user_id") == user_id:
            return chat
    return None


def find_model(store: dict[str, Any], model_id: str | None, model_name: str | None = None):
    if model_id:
        for model in store.get("models", []):
            if model.get("id") == model_id:
                return model
    if model_name:
        for model in store.get("models", []):
            if model.get("name") == model_name:
                return model
    return None


def _resolve_visible_model(store: dict[str, Any], user: dict[str, Any], model_id: str | None):
    model = find_model(store, model_id)
    if not model:
        return None
    if user.get("role") == "admin":
        return model
    if not model.get("enabled", True):
        return None
    return model


def find_mask_by_id(store: dict[str, Any], mask_id: str | None) -> dict[str, Any] | None:
    if not mask_id:
        return None
    for mask in store.get("masks", []):
        if mask.get("id") == mask_id:
            return mask
    return None


def can_view_mask(mask: dict[str, Any], user: dict[str, Any]) -> bool:
    if mask.get("is_public"):
        return True
    return mask.get("owner_user_id") == user.get("id")


def can_edit_mask(mask: dict[str, Any], user: dict[str, Any]) -> bool:
    return mask.get("owner_user_id") == user.get("id")


def visible_masks_for_user(store: dict[str, Any], user: dict[str, Any]) -> list[dict[str, Any]]:
    visible = [m for m in store.get("masks", []) if can_view_mask(m, user)]
    visible.sort(key=lambda m: (_normalize_bool(m.get("is_builtin"), False), _safe_int(m.get("updated_at"), 0)), reverse=True)
    return visible


def serialize_mask_for_user(mask: dict[str, Any], user: dict[str, Any], store: dict[str, Any]) -> dict[str, Any]:
    owner = find_user_by_id(store, mask.get("owner_user_id"))
    return {
        "id": mask.get("id"),
        "name": mask.get("name"),
        "prompt": mask.get("prompt"),
        "is_public": bool(mask.get("is_public", False)),
        "is_builtin": bool(mask.get("is_builtin", False)),
        "owner_user_id": mask.get("owner_user_id"),
        "owner_username": owner.get("username") if owner else None,
        "created_at": mask.get("created_at"),
        "updated_at": mask.get("updated_at"),
        "can_edit": can_edit_mask(mask, user),
    }


def _configured_default_builtin_mask(store: dict[str, Any]) -> dict[str, Any] | None:
    default_def = _get_default_builtin_agent_definition()
    preferred_name = str(default_def.get("name") or "")
    for mask in store.get("masks", []):
        if (
            mask.get("is_builtin")
            and mask.get("is_public")
            and str(mask.get("name") or "") == preferred_name
        ):
            return mask
    for mask in store.get("masks", []):
        if mask.get("is_builtin") and mask.get("is_public"):
            return mask
    return None


def ensure_builtin_agents(store: dict[str, Any], owner_user_id: str) -> dict[str, dict[str, Any]]:
    ts = now_ts()
    builtins_by_key: dict[str, dict[str, Any]] = {}
    masks = store.setdefault("masks", [])
    for builtin_def in BUILTIN_AGENTS:
        builtin_name = str(builtin_def.get("name") or "")
        mask = None
        for current in masks:
            if str(current.get("name") or "") == builtin_name:
                mask = current
                break
        if mask is None:
            mask = {
                "id": new_id(),
                "name": builtin_name,
                "prompt": str(builtin_def.get("prompt") or ""),
                "owner_user_id": owner_user_id,
                "is_public": True,
                "is_builtin": True,
                "created_at": ts,
                "updated_at": ts,
            }
            masks.append(mask)
        else:
            mask_changed = False
            if mask.get("owner_user_id") is None:
                mask["owner_user_id"] = owner_user_id
                mask_changed = True
            if not mask.get("is_public"):
                mask["is_public"] = True
                mask_changed = True
            if not mask.get("is_builtin"):
                mask["is_builtin"] = True
                mask_changed = True
            if not str(mask.get("prompt") or "").strip():
                mask["prompt"] = str(builtin_def.get("prompt") or "")
                mask_changed = True
            if mask_changed:
                mask["updated_at"] = ts
        builtins_by_key[str(builtin_def.get("key") or "")] = mask
    return builtins_by_key


def _ensure_store_builtin_agents(store: dict[str, Any]) -> bool:
    users = store.get("users", [])
    if not isinstance(users, list) or not users:
        return False
    owner = next((u for u in users if str(u.get("role") or "").lower() == "admin"), None)
    if owner is None:
        owner = users[0]
    owner_user_id = str(owner.get("id") or "").strip()
    if not owner_user_id:
        return False

    before = json.dumps(store.get("masks", []), ensure_ascii=False, sort_keys=True)
    ensure_builtin_agents(store, owner_user_id)
    after = json.dumps(store.get("masks", []), ensure_ascii=False, sort_keys=True)
    return before != after


def _append_user_message(
    chat: dict[str, Any],
    message: str,
    attachments: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    existing_user_count = sum(1 for m in chat.get("messages", []) if m.get("role") == "user")
    user_msg = {
        "id": new_id(),
        "role": "user",
        "content": message,
        "created_at": now_ts(),
    }
    normalized_attachments = _normalize_attachments(attachments or [])
    if normalized_attachments:
        user_msg["attachments"] = normalized_attachments
    chat.setdefault("messages", []).append(user_msg)
    current_title = (chat.get("title") or "").strip()
    if existing_user_count == 0 and (not current_title or current_title == "新对话"):
        chat["title"] = build_chat_title_from_message(message)
    return user_msg


def _attachment_text_block(att: dict[str, Any]) -> str:
    name = str(att.get("name") or "attachment")
    mime_type = str(att.get("mime_type") or "application/octet-stream")
    kind = str(att.get("kind") or "file")
    size = _safe_int(att.get("size"), 0)
    text_content = str(att.get("text_content") or "").strip()
    if text_content:
        return f"[闄勪欢:{name} ({mime_type})]\\n{text_content}"
    return f"[附件:{name}] 类型:{mime_type} 种类:{kind} 大小:{size}字节（未提取文本）"


def _attachment_image_data_for_upstream(att: dict[str, Any]) -> str | None:
    data_url = str(att.get("data_url") or "").strip()
    if not data_url:
        return None
    if data_url.startswith("data:") or data_url.startswith("http://") or data_url.startswith("https://"):
        return data_url

    local_path = _safe_local_attachment_path_from_url(data_url)
    if local_path is None or not local_path.exists():
        return None

    mime_type = str(att.get("mime_type") or "").strip().lower()
    if not mime_type.startswith("image/"):
        mime_type = _infer_image_mime_from_url(local_path.name)
    if not mime_type.startswith("image/"):
        mime_type = "image/png"

    try:
        binary = local_path.read_bytes()
    except Exception:
        return None
    if not binary:
        return None
    encoded = base64.b64encode(binary).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _build_user_message_for_upstream(msg: dict[str, Any], protocol: str) -> dict[str, Any]:
    base_text = str(msg.get("content") or "").strip()
    attachments = _normalize_attachments(msg.get("attachments") or [])
    image_data_urls: list[str] = []
    attachment_texts: list[str] = []
    for att in attachments:
        if att.get("kind") == "image":
            image_data = _attachment_image_data_for_upstream(att)
            if image_data:
                image_data_urls.append(image_data)
                continue
            attachment_texts.append(_attachment_text_block(att))
        else:
            attachment_texts.append(_attachment_text_block(att))

    merged_text = base_text
    if attachment_texts:
        merged = "\n\n".join(attachment_texts)
        merged_text = f"{base_text}\n\n{merged}".strip() if base_text else merged

    if not image_data_urls:
        return {"role": "user", "content": merged_text}

    if protocol == "responses":
        parts: list[dict[str, Any]] = []
        if merged_text:
            parts.append({"type": "input_text", "text": merged_text})
        for data_url in image_data_urls:
            parts.append({"type": "input_image", "image_url": data_url})
        if not parts:
            parts.append({"type": "input_text", "text": ""})
        return {"role": "user", "content": parts}

    parts = []
    if merged_text:
        parts.append({"type": "text", "text": merged_text})
    for data_url in image_data_urls:
        parts.append({"type": "image_url", "image_url": {"url": data_url}})
    if not parts:
        return {"role": "user", "content": ""}
    return {"role": "user", "content": parts}


def _build_request_messages(chat: dict[str, Any], mask: dict[str, Any] | None, protocol: str) -> list[dict[str, Any]]:
    req_messages: list[dict[str, Any]] = []
    if mask and str(mask.get("prompt") or "").strip():
        req_messages.append({"role": "system", "content": mask["prompt"]})
    for msg in chat.get("messages", []):
        role = str(msg.get("role") or "").strip().lower()
        if role == "user":
            req_messages.append(_build_user_message_for_upstream(msg, protocol=protocol))
        elif role in {"assistant", "system"}:
            req_messages.append({"role": role, "content": str(msg.get("content") or "")})
    return req_messages


def build_upstream_url(base_url: str) -> tuple[str, str]:
    trimmed = base_url.strip().rstrip("/")
    if not trimmed:
        raise HTTPException(status_code=400, detail="模型未配置 base_url")
    if not trimmed.startswith("http"):
        trimmed = f"https://{trimmed}"
    if trimmed.endswith("/v1/images/generations") or trimmed.endswith("/images/generations"):
        return trimmed, "images_generations"
    if trimmed.endswith("/v1/responses") or trimmed.endswith("/responses"):
        return trimmed, "responses"
    if trimmed.endswith("/chat/completions"):
        return trimmed, "chat_completions"
    if trimmed.endswith("/v1"):
        return f"{trimmed}/chat/completions", "chat_completions"
    return f"{trimmed}/v1/chat/completions", "chat_completions"


def _extract_upstream_error_text(res: httpx.Response) -> str:
    body = (res.text or "").strip()
    if not body:
        return "上游服务返回空错误信息"
    try:
        payload = res.json()
    except ValueError:
        return body[:500]
    if isinstance(payload, dict):
        detail = payload.get("detail")
        if isinstance(detail, str) and detail.strip():
            return detail.strip()[:500]
        error = payload.get("error")
        if isinstance(error, dict):
            message = error.get("message")
            if isinstance(message, str) and message.strip():
                return message.strip()[:500]
        message = payload.get("message")
        if isinstance(message, str) and message.strip():
            return message.strip()[:500]
    return body[:500]


def _extract_assistant_text(data: dict[str, Any], protocol: str) -> str:
    if protocol == "images_generations":
        revised_prompts: list[str] = []
        items = data.get("data")
        if isinstance(items, list):
            for item in items:
                if not isinstance(item, dict):
                    continue
                revised = item.get("revised_prompt")
                if isinstance(revised, str) and revised.strip():
                    revised_prompts.append(revised.strip())
        if revised_prompts:
            return "\n".join(revised_prompts).strip()

    if protocol == "responses":
        output_text = data.get("output_text")
        if isinstance(output_text, str) and output_text.strip():
            return output_text.strip()

        fragments: list[str] = []
        output = data.get("output")
        if isinstance(output, list):
            for item in output:
                if not isinstance(item, dict):
                    continue
                content = item.get("content")
                if not isinstance(content, list):
                    continue
                for part in content:
                    if not isinstance(part, dict):
                        continue
                    text = part.get("text")
                    if isinstance(text, str) and text.strip():
                        fragments.append(text.strip())
        if fragments:
            return "\n".join(fragments).strip()

    message_content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
    if isinstance(message_content, str):
        return message_content.strip()
    if isinstance(message_content, list):
        fragments: list[str] = []
        for part in message_content:
            if not isinstance(part, dict):
                continue
            text = part.get("text")
            if isinstance(text, str) and text.strip():
                fragments.append(text.strip())
        if fragments:
            return "\n".join(fragments).strip()
    return ""


def _extract_stream_delta_text(payload: dict[str, Any], protocol: str) -> str:
    if protocol == "responses":
        event_type = payload.get("type")
        if isinstance(event_type, str):
            if event_type.endswith(".delta"):
                delta = payload.get("delta")
                if isinstance(delta, str):
                    return delta
            return ""

        return ""

    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    first = choices[0] if isinstance(choices[0], dict) else {}
    delta = first.get("delta") if isinstance(first, dict) else None
    if isinstance(delta, dict):
        content = delta.get("content")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            fragments: list[str] = []
            for part in content:
                if not isinstance(part, dict):
                    continue
                text = part.get("text")
                if isinstance(text, str):
                    fragments.append(text)
            if fragments:
                return "".join(fragments)
    return ""


def _extract_stream_final_text(payload: dict[str, Any], protocol: str) -> str:
    if protocol != "responses":
        return ""

    event_type = payload.get("type")
    if isinstance(event_type, str):
        if event_type.endswith(".output_text.done"):
            text = payload.get("text")
            if isinstance(text, str):
                return text
        response = payload.get("response")
        if isinstance(response, dict):
            return _extract_assistant_text(response, protocol)
        return ""

    return _extract_assistant_text(payload, protocol)


def _build_upstream_body(
    protocol: str,
    model_name: str,
    req_messages: list[dict[str, Any]],
    temperature: float,
    reasoning_effort: str | None = None,
    *,
    stream: bool = False,
    enable_image_generation: bool = False,
    image_prompt: str = "",
) -> dict[str, Any]:
    if protocol == "images_generations":
        prompt = str(image_prompt or "").strip()
        if not prompt:
            raise HTTPException(status_code=400, detail="图片生成提示词不能为空")
        return {
            "model": model_name,
            "prompt": prompt,
            "n": 1,
            "size": "1024x1024",
        }

    if protocol == "responses":
        body: dict[str, Any] = {
            "model": model_name,
            "input": req_messages,
            "temperature": temperature,
        }
    else:
        body = {
            "model": model_name,
            "messages": req_messages,
            "temperature": temperature,
        }
    if reasoning_effort:
        if protocol == "responses":
            body["reasoning"] = {"effort": reasoning_effort}
        else:
            body["reasoning_effort"] = reasoning_effort
    if protocol == "responses" and enable_image_generation:
        body["tools"] = [{"type": "image_generation"}]
        body["modalities"] = ["text", "image"]
    if stream:
        body["stream"] = True
    return body


async def _call_upstream_chat_completion(
    url: str,
    headers: dict[str, str],
    body: dict[str, Any],
) -> dict[str, Any]:
    max_attempts = len(UPSTREAM_RETRY_DELAYS_SECONDS) + 1
    async with httpx.AsyncClient(timeout=120) as client:
        for attempt in range(max_attempts):
            try:
                res = await client.post(url, headers=headers, json=body)
            except httpx.HTTPError as e:
                if attempt < max_attempts - 1:
                    LOGGER.warning(
                        "上游请求异常，准备重试(%s/%s): %s",
                        attempt + 1,
                        max_attempts,
                        e,
                    )
                    await asyncio.sleep(UPSTREAM_RETRY_DELAYS_SECONDS[attempt])
                    continue
                LOGGER.error("上游请求失败，已完成重试: %s", e)
                raise HTTPException(status_code=502, detail=f"请求模型失败: {e}") from e

            if res.status_code >= 400:
                error_text = _extract_upstream_error_text(res)
                if res.status_code in UPSTREAM_RETRY_STATUS_CODES and attempt < max_attempts - 1:
                    LOGGER.warning(
                        "上游返回错误，准备重试(%s/%s): status=%s detail=%s",
                        attempt + 1,
                        max_attempts,
                        res.status_code,
                        error_text,
                    )
                    await asyncio.sleep(UPSTREAM_RETRY_DELAYS_SECONDS[attempt])
                    continue
                LOGGER.error(
                    "上游返回错误，重试结束: status=%s detail=%s",
                    res.status_code,
                    error_text,
                )
                raise HTTPException(
                    status_code=502,
                    detail=f"上游错误({res.status_code}): {error_text}",
                )
            try:
                return res.json()
            except ValueError as e:
                raise HTTPException(status_code=502, detail="上游返回了无法解析的 JSON 响应") from e
    raise HTTPException(status_code=502, detail="请求模型失败: 未知错误")


def _pick_first_enabled_model(store: dict[str, Any]) -> dict[str, Any] | None:
    for model in store.get("models", []):
        if model.get("enabled", True):
            return model
    return store.get("models", [None])[0]


def _default_model_payload(name: str | None, base_url: str | None, api_key: str | None) -> dict[str, Any]:
    ts = now_ts()
    return {
        "id": new_id(),
        "name": (name or DEFAULT_MODEL_NAME).strip() or DEFAULT_MODEL_NAME,
        "provider": "openai-compatible",
        "base_url": (base_url or DEFAULT_BASE_URL).strip(),
        "api_key": (api_key or DEFAULT_API_KEY).strip(),
        "enabled": True,
        "use_streaming": True,
        "created_at": ts,
        "updated_at": ts,
    }


def _resolve_visible_mask(store: dict[str, Any], user: dict[str, Any], mask_id: str | None):
    mask = find_mask_by_id(store, mask_id)
    if mask and can_view_mask(mask, user):
        return mask
    return None


def _visible_models_for_user(store: dict[str, Any], user: dict[str, Any]) -> list[dict[str, Any]]:
    if user.get("role") == "admin":
        return list(store.get("models", []))
    return [m for m in store.get("models", []) if m.get("enabled", True)]


def _ensure_user_default_model(store: dict[str, Any], user: dict[str, Any]) -> bool:
    visible_models = _visible_models_for_user(store, user)
    visible_ids = {m.get("id") for m in visible_models}
    current_default = user.get("default_model_id")
    if current_default and current_default in visible_ids:
        return False
    if current_default is None:
        return False

    user["default_model_id"] = None
    user["updated_at"] = now_ts()
    return True


def _default_model_id_for_new_user(store: dict[str, Any], admin_user: dict[str, Any] | None) -> str | None:
    if not isinstance(admin_user, dict):
        return None

    admin_default_model_id = str(admin_user.get("default_model_id") or "").strip()
    if not admin_default_model_id:
        return None

    model = find_model(store, admin_default_model_id)
    if not model or not model.get("enabled", True):
        return None
    return model.get("id")


def _ensure_user_default_mask(store: dict[str, Any], user: dict[str, Any]) -> bool:
    changed = False
    visible_masks = visible_masks_for_user(store, user)
    visible_ids = {m.get("id") for m in visible_masks}
    current_default = user.get("default_mask_id")
    if current_default is None:
        # User explicitly chose "no default agent". Keep it as-is.
        return False
    if current_default and current_default in visible_ids:
        return False

    fallback = None
    preferred_default = _configured_default_builtin_mask(store)
    preferred_default_id = preferred_default.get("id") if preferred_default else None
    if preferred_default_id in visible_ids:
        fallback = preferred_default_id
    if not fallback:
        for mask in visible_masks:
            if mask.get("is_builtin"):
                fallback = mask.get("id")
                break
    if not fallback:
        for mask in visible_masks:
            fallback = mask.get("id")
            break

    if user.get("default_mask_id") != fallback:
        user["default_mask_id"] = fallback
        user["updated_at"] = now_ts()
        changed = True
    return changed


def _serialize_model_for_user(model: dict[str, Any], is_admin: bool) -> dict[str, Any]:
    base = {
        "id": model.get("id"),
        "name": model.get("name"),
        "provider": model.get("provider"),
        "enabled": bool(model.get("enabled", True)),
        "use_streaming": bool(model.get("use_streaming", True)),
        "created_at": model.get("created_at"),
        "updated_at": model.get("updated_at"),
    }
    if is_admin:
        base["base_url"] = model.get("base_url", "")
        base["api_key_masked"] = _mask_api_key_for_display(model.get("api_key", ""))
    return base


def _admin_users_for_state(store: dict[str, Any]) -> list[dict[str, Any]]:
    users = [sanitize_user_for_client(u) for u in store.get("users", [])]
    users.sort(key=lambda u: (u.get("role") != "admin", str(u.get("username") or "").lower()))
    return users


def _claim_orphan_data_for_admin(store: dict[str, Any], admin_user_id: str) -> bool:
    changed = False
    for mask in store.get("masks", []):
        if not mask.get("owner_user_id"):
            mask["owner_user_id"] = admin_user_id
            mask["updated_at"] = now_ts()
            changed = True
    for chat in store.get("chats", []):
        if not chat.get("user_id"):
            chat["user_id"] = admin_user_id
            chat["updated_at"] = now_ts()
            changed = True
    return changed


class SetupInitInput(BaseModel):
    app_title: str | None = None
    app_subtitle: str | None = None
    app_icon_data_url: str | None = None
    admin_password: str = Field(min_length=1)
    default_model_name: str | None = None
    default_base_url: str | None = None
    default_api_key: str | None = None


class LoginInput(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


class ModelInput(BaseModel):
    id: str | None = None
    name: str = Field(min_length=1)
    provider: str = "openai-compatible"
    base_url: str = ""
    api_key: str = ""
    enabled: bool = True
    use_streaming: bool = True


class ModelStreamingInput(BaseModel):
    use_streaming: bool = True


class MaskInput(BaseModel):
    id: str | None = None
    name: str = Field(min_length=1)
    prompt: str = ""
    is_public: bool | None = None


class MaskPublicInput(BaseModel):
    is_public: bool


class DefaultAgentInput(BaseModel):
    agent_id: str | None = None


class DefaultModelInput(BaseModel):
    model_id: str | None = None


class ChatCreateInput(BaseModel):
    title: str = "新对话"
    model_id: str | None = None
    mask_id: str | None = None
    reasoning_effort: Literal["low", "medium", "high", "xhigh"] | None = None


class ChatConfigInput(BaseModel):
    model_id: str | None = None
    mask_id: str | None = None
    reasoning_effort: Literal["low", "medium", "high", "xhigh"] | None = None


class AttachmentInput(BaseModel):
    name: str = ""
    mime_type: str = ""
    size: int = 0
    kind: str = "file"
    data_url: str | None = None
    text_content: str | None = None


class SendMessageInput(BaseModel):
    chat_id: str
    message: str = ""
    model_id: str | None = None
    model_name: str | None = None
    mask_id: str | None = None
    reasoning_effort: Literal["low", "medium", "high", "xhigh"] | None = None
    temperature: float = 0.7
    attachments: list[AttachmentInput] = Field(default_factory=list)


class AdminUserCreateInput(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)
    role: str = "user"


class AdminUserPasswordInput(BaseModel):
    password: str = Field(min_length=1)


class UserRetentionSettingsInput(BaseModel):
    message_text_ttl_hours: int | None = Field(default=None, ge=0, le=MAX_RETENTION_HOURS)
    message_media_ttl_hours: int | None = Field(default=None, ge=0, le=MAX_RETENTION_HOURS)


class AdminAppSettingsInput(BaseModel):
    app_title: str | None = None
    app_subtitle: str | None = None
    app_icon_data_url: str | None = None


app = FastAPI(title="My Chat")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.mount("/data/file", StaticFiles(directory=str(ATTACHMENT_FILE_DIR)), name="attachment_files")


@app.middleware("http")
async def enforce_api_auth(request: Request, call_next):
    path = request.url.path
    is_api = path.startswith("/api")
    is_attachment_file = path.startswith(ATTACHMENT_FILE_URL_PREFIX)
    request_params: dict[str, Any] | None = None
    current_user_for_audit: dict[str, Any] | None = None
    audit_store: dict[str, Any] | None = None

    if is_attachment_file:
        store = load_store()
        user_id = verify_session_token(store, request.cookies.get(SESSION_COOKIE))
        user = find_user_by_id(store, user_id)
        if not user or not user.get("enabled", True):
            return JSONResponse(status_code=401, content={"detail": "未登录或会话无效"})

        normalized_url = _normalize_attachment_url_for_compare(path)
        local_path = _safe_local_attachment_path_from_url(normalized_url)
        if local_path is None or not local_path.exists() or not local_path.is_file():
            return JSONResponse(status_code=404, content={"detail": "文件不存在"})

        if not _user_can_access_attachment_url(store, user, normalized_url):
            return JSONResponse(status_code=403, content={"detail": "无权访问该文件"})

        request.state.current_user = user
        return await call_next(request)

    if is_api:
        try:
            request_params = await _extract_request_params_for_audit(request)
        except Exception:
            request_params = {"body": "[请求参数解析失败]"}

    if is_api and path not in AUTH_FREE_API_PATHS:
        store = load_store()
        audit_store = store
        if not is_initialized(store):
            audit_params = _enrich_request_params_for_audit(
                request=request,
                request_params=request_params,
                store=audit_store,
                user=None,
            )
            response = JSONResponse(status_code=409, content={"detail": "应用尚未初始化，请先完成首次设置"})
            _audit_log_api_request(
                request=request,
                status_code=response.status_code,
                user=None,
                request_params=audit_params,
                extra={"鉴权": "应用未初始化"},
            )
            return response
        user_id = verify_session_token(store, request.cookies.get(SESSION_COOKIE))
        user = find_user_by_id(store, user_id)
        if not user or not user.get("enabled", True):
            audit_params = _enrich_request_params_for_audit(
                request=request,
                request_params=request_params,
                store=audit_store,
                user=None,
            )
            response = JSONResponse(status_code=401, content={"detail": "未登录或会话无效"})
            _audit_log_api_request(
                request=request,
                status_code=response.status_code,
                user=None,
                request_params=audit_params,
                extra={"鉴权": "未登录或会话无效"},
            )
            return response
        request.state.current_user = user
        current_user_for_audit = user
    elif is_api:
        try:
            store = load_store()
            audit_store = store
            if is_initialized(store):
                user_id = verify_session_token(store, request.cookies.get(SESSION_COOKIE))
                user = find_user_by_id(store, user_id)
                if user and user.get("enabled", True):
                    current_user_for_audit = user
        except Exception:
            current_user_for_audit = None

    try:
        response = await call_next(request)
    except Exception as exc:
        if is_api:
            final_user = getattr(request.state, "current_user", None) or current_user_for_audit
            audit_params = _enrich_request_params_for_audit(
                request=request,
                request_params=request_params,
                store=audit_store,
                user=final_user,
            )
            _audit_log_api_request(
                request=request,
                status_code=500,
                user=final_user,
                request_params=audit_params,
                extra={"异常": f"{exc.__class__.__name__}: {str(exc)[:200]}"},
            )
        raise

    if is_api and not getattr(request.state, "skip_auto_audit", False):
        final_user = getattr(request.state, "current_user", None) or current_user_for_audit
        audit_params = _enrich_request_params_for_audit(
            request=request,
            request_params=request_params,
            store=audit_store,
            user=final_user,
        )
        audit_extra = None
        response, error_detail = await _extract_error_detail_from_response(response)
        if error_detail:
            audit_extra = {"错误": error_detail}
        _audit_log_api_request(
            request=request,
            status_code=response.status_code,
            user=final_user,
            request_params=audit_params,
            extra=audit_extra,
        )
    return response


@app.get("/")
def index() -> HTMLResponse:
    store = load_store()
    app_meta = _serialize_app_meta_for_client(store.get("app") or {})
    html_text = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    title = html.escape(str(app_meta.get("title") or DEFAULT_APP_TITLE), quote=False)
    icon_url = str(app_meta.get("icon_url") or "")
    icon_link = (
        f'<link id="appIconLink" rel="icon" href="{html.escape(icon_url, quote=True)}" />'
        if icon_url
        else '<link id="appIconLink" rel="icon" href="data:," />'
    )
    html_text = re.sub(r"<title>.*?</title>", f"<title>{title}</title>\n    {icon_link}", html_text, count=1, flags=re.I | re.S)
    return HTMLResponse(content=html_text)


@app.get("/api/auth/status")
def auth_status(request: Request):
    store = load_store()
    initialized = is_initialized(store)
    user = None
    if initialized:
        user_id = verify_session_token(store, request.cookies.get(SESSION_COOKIE))
        user = find_user_by_id(store, user_id)
        if user and not user.get("enabled", True):
            user = None

    app_meta = store.get("app") or {}
    return {
        "need_setup": not initialized,
        "authorized": bool(user),
        "user": sanitize_user_for_client(user) if user else None,
        "app": _serialize_app_meta_for_client(app_meta),
    }


@app.post("/api/setup/init")
def setup_init(payload: SetupInitInput, response: Response):
    store = load_store()
    if is_initialized(store):
        raise HTTPException(status_code=409, detail="应用已经初始化，无需重复设置")

    ts = now_ts()
    admin_user = {
        "id": new_id(),
        "username": "admin",
        "password_sha256": sha256_hex(payload.admin_password),
        "role": "admin",
        "enabled": True,
        "default_model_id": None,
        "default_mask_id": None,
        "message_text_ttl_hours": 0,
        "message_media_ttl_hours": 0,
        "created_at": ts,
        "updated_at": ts,
    }

    if not store.get("models"):
        store["models"] = [
            _default_model_payload(
                payload.default_model_name,
                payload.default_base_url,
                payload.default_api_key,
            )
        ]

    default_admin_model = _pick_first_enabled_model(store)
    admin_user["default_model_id"] = default_admin_model.get("id") if default_admin_model else None
    store.setdefault("users", []).append(admin_user)
    _claim_orphan_data_for_admin(store, admin_user["id"])

    ensure_builtin_agents(store, admin_user["id"])
    configured_builtin_mask = _configured_default_builtin_mask(store)
    legacy_default = (store.get("app") or {}).pop("legacy_default_mask_id", None)

    valid_mask_ids = {m.get("id") for m in store.get("masks", []) if m.get("owner_user_id") == admin_user["id"] or m.get("is_public")}
    if legacy_default in valid_mask_ids:
        admin_user["default_mask_id"] = legacy_default
    else:
        admin_user["default_mask_id"] = configured_builtin_mask.get("id") if configured_builtin_mask else None
    _ensure_user_default_model(store, admin_user)

    app_meta = store.get("app") or {}
    title = (payload.app_title or "").strip()
    subtitle = (payload.app_subtitle or "").strip()
    if title:
        app_meta["title"] = title
    if subtitle:
        app_meta["subtitle"] = subtitle
    icon_filename = _store_app_icon_data_url(payload.app_icon_data_url)
    if icon_filename:
        app_meta["icon_filename"] = icon_filename
        app_meta.pop("icon_data_url", None)
    app_meta["updated_at"] = now_ts()
    store["app"] = app_meta

    save_store(store)

    token = create_session_token(store, admin_user["id"])
    response.set_cookie(
        key=SESSION_COOKIE,
        value=token,
        httponly=True,
        samesite=SESSION_COOKIE_SAMESITE,
        secure=SESSION_COOKIE_SECURE,
        max_age=SESSION_TTL_SECONDS,
        path="/",
    )

    return {
        "ok": True,
        "user": sanitize_user_for_client(admin_user),
        "app": _serialize_app_meta_for_client(app_meta),
    }


@app.post("/api/auth/login")
def auth_login(payload: LoginInput, response: Response):
    store = load_store()
    if not is_initialized(store):
        raise HTTPException(status_code=409, detail="应用尚未初始化，请先完成首次设置")

    user = find_user_by_username(store, payload.username)
    if not user or not user.get("enabled", True):
        raise HTTPException(status_code=401, detail="用户名或密码错误")

    if user.get("password_sha256") != sha256_hex(payload.password):
        raise HTTPException(status_code=401, detail="用户名或密码错误")

    token = create_session_token(store, user["id"])
    response.set_cookie(
        key=SESSION_COOKIE,
        value=token,
        httponly=True,
        samesite=SESSION_COOKIE_SAMESITE,
        secure=SESSION_COOKIE_SECURE,
        max_age=SESSION_TTL_SECONDS,
        path="/",
    )
    return {"ok": True, "user": sanitize_user_for_client(user)}


@app.post("/api/auth/logout")
def auth_logout(response: Response):
    response.delete_cookie(
        SESSION_COOKIE,
        path="/",
        secure=SESSION_COOKIE_SECURE,
        httponly=True,
        samesite=SESSION_COOKIE_SAMESITE,
    )
    return {"ok": True}


@app.get("/api/state")
def get_state(current_user: dict[str, Any] = Depends(require_auth)):
    store = load_store()
    user = find_user_by_id(store, current_user.get("id"))
    if not user or not user.get("enabled", True):
        raise HTTPException(status_code=401, detail="未登录或会话无效")

    changed = _ensure_user_default_model(store, user)
    changed = _ensure_user_default_mask(store, user) or changed
    if changed:
        save_store(store)

    is_admin = user.get("role") == "admin"
    app_meta = store.get("app") or {}

    chats = [c for c in store.get("chats", []) if c.get("user_id") == user.get("id")]
    chats.sort(key=lambda c: _safe_int(c.get("updated_at"), 0), reverse=True)

    model_source = store.get("models", []) if is_admin else [m for m in store.get("models", []) if m.get("enabled", True)]
    models = [_serialize_model_for_user(m, is_admin) for m in model_source]
    visible_masks = [serialize_mask_for_user(m, user, store) for m in visible_masks_for_user(store, user)]

    return {
        "app": _serialize_app_meta_for_client(app_meta),
        "current_user": sanitize_user_for_client(user),
        "permissions": {
            "is_admin": is_admin,
            "can_manage_models": is_admin,
            "can_manage_users": is_admin,
            "can_manage_app": is_admin,
        },
        "users": _admin_users_for_state(store) if is_admin else [],
        "models": models,
        "masks": visible_masks,
        "chats": chats,
        "default_model_id": user.get("default_model_id"),
        "default_model": user.get("default_model_id"),
        "default_mask_id": user.get("default_mask_id"),
        "default_mask": user.get("default_mask_id"),
    }


@app.get("/api/models")
def get_models(_admin: dict[str, Any] = Depends(require_admin)):
    store = load_store()
    return {"models": [_serialize_model_for_user(m, True) for m in store.get("models", [])]}


@app.post("/api/models")
def upsert_model(payload: ModelInput, _admin: dict[str, Any] = Depends(require_admin)):
    store = load_store()
    model = None
    if payload.id:
        for item in store.get("models", []):
            if item.get("id") == payload.id:
                model = item
                break

    if model is None:
        model = {
            "id": new_id(),
            "created_at": now_ts(),
        }
        store.setdefault("models", []).append(model)

    model.update(
        {
            "name": payload.name.strip(),
            "provider": payload.provider.strip() or "openai-compatible",
            "base_url": payload.base_url.strip(),
            "api_key": payload.api_key.strip(),
            "enabled": payload.enabled,
            "use_streaming": payload.use_streaming,
            "updated_at": now_ts(),
        }
    )
    save_store(store)
    return {"ok": True, "model": _serialize_model_for_user(model, True)}


@app.post("/api/models/{model_id}/streaming")
def update_model_streaming(
    model_id: str,
    payload: ModelStreamingInput,
    _admin: dict[str, Any] = Depends(require_admin),
):
    store = load_store()
    model = None
    for item in store.get("models", []):
        if item.get("id") == model_id:
            model = item
            break
    if model is None:
        raise HTTPException(status_code=404, detail="模型不存在")

    model["use_streaming"] = bool(payload.use_streaming)
    model["updated_at"] = now_ts()
    save_store(store)
    return {"ok": True, "model": _serialize_model_for_user(model, True)}


@app.delete("/api/models/{model_id}")
def delete_model(model_id: str, _admin: dict[str, Any] = Depends(require_admin)):
    store = load_store()
    store["models"] = [m for m in store.get("models", []) if m.get("id") != model_id]
    for user in store.get("users", []):
        if user.get("default_model_id") == model_id:
            user["default_model_id"] = None
            user["updated_at"] = now_ts()
    for chat in store.get("chats", []):
        if chat.get("model_id") == model_id:
            chat["model_id"] = None
            chat["updated_at"] = now_ts()
    save_store(store)
    return {"ok": True}


@app.get("/api/masks")
def get_masks(current_user: dict[str, Any] = Depends(require_auth)):
    store = load_store()
    user = find_user_by_id(store, current_user.get("id"))
    if not user:
        raise HTTPException(status_code=401, detail="未登录或会话无效")
    masks = [serialize_mask_for_user(m, user, store) for m in visible_masks_for_user(store, user)]
    return {"masks": masks}


@app.post("/api/masks")
def upsert_mask(payload: MaskInput, current_user: dict[str, Any] = Depends(require_auth)):
    store = load_store()
    user = find_user_by_id(store, current_user.get("id"))
    if not user:
        raise HTTPException(status_code=401, detail="未登录或会话无效")

    mask = None
    if payload.id:
        mask = find_mask_by_id(store, payload.id)
        if not mask:
            raise HTTPException(status_code=404, detail="agent 不存在")
        if not can_edit_mask(mask, user):
            raise HTTPException(status_code=403, detail="只能编辑自己的 Agent")

    if mask is None:
        mask = {
            "id": new_id(),
            "owner_user_id": user.get("id"),
            "created_at": now_ts(),
            "is_builtin": False,
        }
        store.setdefault("masks", []).append(mask)

    is_admin = user.get("role") == "admin"
    if is_admin and payload.is_public is not None:
        is_public = bool(payload.is_public)
    elif mask.get("is_public") is not None:
        is_public = bool(mask.get("is_public"))
    else:
        is_public = False

    mask.update(
        {
            "name": payload.name.strip(),
            "prompt": payload.prompt,
            "is_public": bool(is_public),
            "updated_at": now_ts(),
        }
    )

    if (
        str(mask.get("name") or "") in BUILTIN_AGENT_NAME_SET
        and mask.get("owner_user_id") == user.get("id")
        and is_admin
    ):
        mask["is_builtin"] = True
        mask["is_public"] = True
        if not str(mask.get("prompt") or "").strip():
            builtin_def = _get_builtin_agent_definition_by_name(mask.get("name"))
            if builtin_def:
                mask["prompt"] = str(builtin_def.get("prompt") or "")

    save_store(store)
    return {"ok": True, "mask": serialize_mask_for_user(mask, user, store)}


@app.post("/api/masks/{mask_id}/public")
def set_mask_public(mask_id: str, payload: MaskPublicInput, current_user: dict[str, Any] = Depends(require_auth)):
    store = load_store()
    user = find_user_by_id(store, current_user.get("id"))
    if not user:
        raise HTTPException(status_code=401, detail="未登录或会话无效")
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="仅管理员可设置公共 Agent")

    mask = find_mask_by_id(store, mask_id)
    if not mask:
        raise HTTPException(status_code=404, detail="agent 不存在")
    if not can_edit_mask(mask, user):
        raise HTTPException(status_code=403, detail="只能设置自己创建的 Agent")

    if mask.get("is_builtin"):
        mask["is_public"] = True
    else:
        mask["is_public"] = bool(payload.is_public)
    mask["updated_at"] = now_ts()
    save_store(store)
    return {"ok": True, "mask": serialize_mask_for_user(mask, user, store)}


@app.delete("/api/masks/{mask_id}")
def delete_mask(mask_id: str, current_user: dict[str, Any] = Depends(require_auth)):
    store = load_store()
    user = find_user_by_id(store, current_user.get("id"))
    if not user:
        raise HTTPException(status_code=401, detail="未登录或会话无效")

    mask = find_mask_by_id(store, mask_id)
    if not mask:
        raise HTTPException(status_code=404, detail="agent 不存在")
    if not can_edit_mask(mask, user):
        raise HTTPException(status_code=403, detail="只能删除自己的 Agent")

    store["masks"] = [m for m in store.get("masks", []) if m.get("id") != mask_id]
    for u in store.get("users", []):
        if u.get("default_mask_id") == mask_id:
            u["default_mask_id"] = None
            u["updated_at"] = now_ts()
    for chat in store.get("chats", []):
        if chat.get("mask_id") == mask_id:
            chat["mask_id"] = None
            chat["updated_at"] = now_ts()
    save_store(store)
    return {"ok": True}


@app.get("/api/default-agent")
def get_default_agent(current_user: dict[str, Any] = Depends(require_auth)):
    store = load_store()
    user = find_user_by_id(store, current_user.get("id"))
    if not user:
        raise HTTPException(status_code=401, detail="未登录或会话无效")
    return {"default_agent_id": user.get("default_mask_id")}


@app.post("/api/default-agent")
def set_default_agent(payload: DefaultAgentInput, current_user: dict[str, Any] = Depends(require_auth)):
    store = load_store()
    user = find_user_by_id(store, current_user.get("id"))
    if not user:
        raise HTTPException(status_code=401, detail="未登录或会话无效")

    if payload.agent_id is not None:
        mask = find_mask_by_id(store, payload.agent_id)
        if not mask or not can_view_mask(mask, user):
            raise HTTPException(status_code=400, detail="agent 不可用")

    user["default_mask_id"] = payload.agent_id
    user["updated_at"] = now_ts()
    save_store(store)
    return {"ok": True, "default_agent_id": user.get("default_mask_id")}


@app.post("/api/user/retention-settings")
def set_user_retention_settings(
    payload: UserRetentionSettingsInput,
    current_user: dict[str, Any] = Depends(require_auth),
):
    store = load_store()
    user = find_user_by_id(store, current_user.get("id"))
    if not user:
        raise HTTPException(status_code=401, detail="未登录或会话无效")

    text_ttl_hours = _normalize_retention_hours(payload.message_text_ttl_hours, 0)
    media_ttl_hours = _normalize_retention_hours(payload.message_media_ttl_hours, 0)
    user["message_text_ttl_hours"] = text_ttl_hours
    user["message_media_ttl_hours"] = media_ttl_hours
    user["updated_at"] = now_ts()
    save_store(store)
    return {
        "ok": True,
        "message_text_ttl_hours": user.get("message_text_ttl_hours", 0),
        "message_media_ttl_hours": user.get("message_media_ttl_hours", 0),
    }


@app.get("/api/default-model")
def get_default_model(current_user: dict[str, Any] = Depends(require_auth)):
    store = load_store()
    user = find_user_by_id(store, current_user.get("id"))
    if not user:
        raise HTTPException(status_code=401, detail="未登录或会话无效")

    changed = _ensure_user_default_model(store, user)
    if changed:
        save_store(store)
    return {"default_model_id": user.get("default_model_id")}


@app.post("/api/default-model")
def set_default_model(payload: DefaultModelInput, current_user: dict[str, Any] = Depends(require_auth)):
    store = load_store()
    user = find_user_by_id(store, current_user.get("id"))
    if not user:
        raise HTTPException(status_code=401, detail="未登录或会话无效")

    if payload.model_id is not None:
        model = _resolve_visible_model(store, user, payload.model_id)
        if not model:
            raise HTTPException(status_code=400, detail="模型不可用")
        if not model.get("enabled", True):
            raise HTTPException(status_code=400, detail="模型已禁用")

    user["default_model_id"] = payload.model_id
    user["updated_at"] = now_ts()
    save_store(store)
    return {"ok": True, "default_model_id": user.get("default_model_id")}


@app.get("/api/chats")
def get_chats(current_user: dict[str, Any] = Depends(require_auth)):
    store = load_store()
    user_id = current_user.get("id")
    chats = [c for c in store.get("chats", []) if c.get("user_id") == user_id]
    chats.sort(key=lambda c: _safe_int(c.get("updated_at"), 0), reverse=True)
    return {"chats": chats}


@app.post("/api/chats/clear")
def clear_my_chats(current_user: dict[str, Any] = Depends(require_auth)):
    store = load_store()
    user = find_user_by_id(store, current_user.get("id"))
    if not user:
        raise HTTPException(status_code=401, detail="未登录或会话无效")

    removed_chats = _clear_chats_in_store(store, user.get("id"))
    if removed_chats > 0:
        save_store(store)
    return {"ok": True, "removed_chats": removed_chats}


@app.post("/api/chats")
def create_chat(payload: ChatCreateInput, current_user: dict[str, Any] = Depends(require_auth)):
    store = load_store()
    user = find_user_by_id(store, current_user.get("id"))
    if not user:
        raise HTTPException(status_code=401, detail="未登录或会话无效")

    model_changed = _ensure_user_default_model(store, user)
    mask_changed = _ensure_user_default_mask(store, user)

    use_payload_model = _payload_has_field(payload, "model_id")
    chat_model_id = payload.model_id if use_payload_model else user.get("default_model_id")
    if chat_model_id is not None:
        chat_model = _resolve_visible_model(store, user, chat_model_id)
        if not chat_model:
            raise HTTPException(status_code=400, detail="模型不可用")
        if not chat_model.get("enabled", True):
            raise HTTPException(status_code=400, detail="模型已禁用")

    default_mask_id = user.get("default_mask_id")
    use_payload_mask = _payload_has_field(payload, "mask_id")
    chat_mask_id = payload.mask_id if use_payload_mask else default_mask_id
    if chat_mask_id is not None:
        chat_mask = _resolve_visible_mask(store, user, chat_mask_id)
        if not chat_mask:
            raise HTTPException(status_code=400, detail="Agent 不可用")

    use_payload_reasoning = _payload_has_field(payload, "reasoning_effort")
    chat_reasoning_effort = _normalize_reasoning_effort(payload.reasoning_effort if use_payload_reasoning else None)
    now = now_ts()
    chat = {
        "id": new_id(),
        "user_id": user.get("id"),
        "title": payload.title.strip() or "新对话",
        "model_id": chat_model_id,
        "mask_id": chat_mask_id,
        "reasoning_effort": chat_reasoning_effort,
        "created_at": now,
        "updated_at": now,
        "messages": [],
    }
    store.setdefault("chats", []).insert(0, chat)
    if model_changed or mask_changed:
        user["updated_at"] = now
    save_store(store)
    return {"ok": True, "chat": chat}


@app.post("/api/chats/{chat_id}/config")
def update_chat_config(
    chat_id: str,
    payload: ChatConfigInput,
    current_user: dict[str, Any] = Depends(require_auth),
):
    store = load_store()
    user = find_user_by_id(store, current_user.get("id"))
    if not user:
        raise HTTPException(status_code=401, detail="未登录或会话无效")

    chat = find_chat_for_user(store, chat_id, user.get("id"))
    if not chat:
        raise HTTPException(status_code=404, detail="chat 不存在")

    model_id = payload.model_id
    if model_id is not None:
        model = _resolve_visible_model(store, user, model_id)
        if not model:
            raise HTTPException(status_code=400, detail="模型不可用")
        if not model.get("enabled", True):
            raise HTTPException(status_code=400, detail="模型已禁用")

    mask_id = payload.mask_id
    if mask_id is not None:
        mask = _resolve_visible_mask(store, user, mask_id)
        if not mask:
            raise HTTPException(status_code=400, detail="Agent 不可用")

    chat["model_id"] = model_id
    chat["mask_id"] = mask_id
    chat["reasoning_effort"] = _normalize_reasoning_effort(payload.reasoning_effort)
    chat["updated_at"] = now_ts()
    save_store(store)
    return {"ok": True, "chat": chat}


@app.delete("/api/chats/{chat_id}")
def delete_chat(chat_id: str, current_user: dict[str, Any] = Depends(require_auth)):
    store = load_store()
    user_id = current_user.get("id")
    before = len(store.get("chats", []))
    removed_chats = [
        c
        for c in store.get("chats", [])
        if c.get("id") == chat_id and c.get("user_id") == user_id
    ]
    store["chats"] = [c for c in store.get("chats", []) if not (c.get("id") == chat_id and c.get("user_id") == user_id)]
    if len(store["chats"]) == before:
        raise HTTPException(status_code=404, detail="chat 不存在")
    removed_files: set[Path] = set()
    for chat in removed_chats:
        if isinstance(chat, dict):
            removed_files.update(_iter_chat_local_attachment_paths(chat))
    kept_files = _collect_store_local_attachment_paths(store)
    _delete_local_attachment_files(removed_files, kept_paths=kept_files)
    save_store(store)
    return {"ok": True}


def _resolve_chat_model_and_mask(
    store: dict[str, Any],
    user: dict[str, Any],
    chat: dict[str, Any],
    payload: SendMessageInput,
):
    use_payload_model = _payload_has_field(payload, "model_id")
    target_model_id = payload.model_id if use_payload_model else chat.get("model_id")
    if target_model_id is None:
        target_model_id = user.get("default_model_id")

    model = _resolve_visible_model(store, user, target_model_id) if target_model_id else None
    if not model and payload.model_name:
        named_model = find_model(store, None, payload.model_name)
        if named_model and (user.get("role") == "admin" or named_model.get("enabled", True)):
            model = named_model
    if not model:
        model = _pick_first_enabled_model(store)
    if not model:
        raise HTTPException(status_code=400, detail="请先配置模型")
    if not model.get("enabled", True):
        raise HTTPException(status_code=400, detail="模型已禁用")

    use_payload_mask = _payload_has_field(payload, "mask_id")
    target_mask_id = payload.mask_id if use_payload_mask else chat.get("mask_id")
    mask = None
    if target_mask_id:
        mask = _resolve_visible_mask(store, user, target_mask_id)
        if not mask:
            raise HTTPException(status_code=400, detail="Agent 不可用")

    return model, mask


def _append_assistant_message_to_latest_chat(
    chat_id: str,
    user_id: str,
    assistant_text: str,
    model_id: str,
    assistant_mask_id: str | None,
    assistant_agent_name: str | None = None,
    assistant_attachments: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    latest_store = load_store()
    latest_chat = find_chat_for_user(latest_store, chat_id, user_id)
    if not latest_chat:
        return None

    assistant_msg = {
        "id": new_id(),
        "role": "assistant",
        "content": assistant_text,
        "created_at": now_ts(),
    }
    if assistant_mask_id is not None:
        assistant_msg["mask_id"] = assistant_mask_id
    if str(assistant_agent_name or "").strip():
        assistant_msg["agent_name"] = str(assistant_agent_name).strip()
    normalized_attachments = _normalize_attachments(assistant_attachments or [])
    if normalized_attachments:
        assistant_msg["attachments"] = normalized_attachments
    latest_chat.setdefault("messages", []).append(assistant_msg)
    latest_chat["updated_at"] = now_ts()
    latest_chat["model_id"] = model_id
    if assistant_mask_id is not None:
        latest_chat["mask_id"] = assistant_mask_id

    save_store(latest_store)
    return assistant_msg


@app.post("/api/chat")
async def send_message(payload: SendMessageInput, current_user: dict[str, Any] = Depends(require_auth)):
    store = load_store()
    user = find_user_by_id(store, current_user.get("id"))
    if not user:
        raise HTTPException(status_code=401, detail="未登录或会话无效")

    chat = find_chat_for_user(store, payload.chat_id, user.get("id"))
    if not chat:
        raise HTTPException(status_code=404, detail="chat 不存在")

    model, mask = _resolve_chat_model_and_mask(store, user, chat, payload)
    use_payload_mask = _payload_has_field(payload, "mask_id")
    use_payload_reasoning = _payload_has_field(payload, "reasoning_effort")
    reasoning_effort = _normalize_reasoning_effort(
        payload.reasoning_effort if use_payload_reasoning else chat.get("reasoning_effort")
    )
    message_text = str(payload.message or "").strip()
    raw_attachments = []
    for att in payload.attachments or []:
        if hasattr(att, "model_dump"):
            raw_attachments.append(att.model_dump())
        elif hasattr(att, "dict"):
            raw_attachments.append(att.dict())
        else:
            raw_attachments.append(att)
    attachments = _normalize_attachments(raw_attachments)
    if not message_text and not attachments:
        raise HTTPException(status_code=400, detail="消息内容和附件不能同时为空")

    url, protocol = _resolve_upstream_target(
        base_url=model.get("base_url", ""),
        model_name=model.get("name", ""),
        message_text=message_text,
    )
    if _looks_like_svg_text_request(message_text) and protocol == "images_generations":
        raise HTTPException(
            status_code=400,
            detail="当前模型仅支持位图图片生成；SVG/矢量代码请求请改用文本模型。",
        )

    _append_user_message(chat, message_text, attachments)

    req_messages = _build_request_messages(chat, mask, protocol=protocol)
    enable_image_generation = protocol == "responses" and _looks_like_image_generation_request(
        message_text,
        model.get("name", ""),
    )
    headers = {"Content-Type": "application/json"}
    api_key = model.get("api_key", "").strip()
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    body = _build_upstream_body(
        protocol=protocol,
        model_name=model["name"],
        req_messages=req_messages,
        temperature=payload.temperature,
        reasoning_effort=reasoning_effort,
        enable_image_generation=enable_image_generation,
        image_prompt=message_text,
    )

    chat["updated_at"] = now_ts()
    chat["model_id"] = model["id"]
    if use_payload_mask:
        chat["mask_id"] = payload.mask_id
    if use_payload_reasoning:
        chat["reasoning_effort"] = reasoning_effort
    save_store(store)

    data = await _call_upstream_chat_completion(url=url, headers=headers, body=body)
    assistant_text = _extract_assistant_text(data, protocol=protocol)
    assistant_generated_attachments = _extract_generated_image_attachments(data)
    svg_attachments = _extract_svg_attachments_from_text(
        assistant_text,
        start_index=len(assistant_generated_attachments) + 1,
    )
    if svg_attachments:
        _merge_attachments_unique(assistant_generated_attachments, svg_attachments)
    if not assistant_text and not assistant_generated_attachments:
        assistant_text = "[空响应]"

    assistant_msg = _append_assistant_message_to_latest_chat(
        chat_id=chat["id"],
        user_id=user["id"],
        assistant_text=assistant_text,
        model_id=model["id"],
        assistant_mask_id=mask.get("id") if mask else None,
        assistant_agent_name=mask.get("name") if mask else None,
        assistant_attachments=assistant_generated_attachments,
    )
    if assistant_msg is None:
        raise HTTPException(status_code=404, detail="chat 已被删除")

    latest_store = load_store()
    latest_chat = find_chat_for_user(latest_store, chat["id"], user["id"])
    return {"ok": True, "assistant": assistant_msg, "chat": latest_chat}


@app.post("/api/chat/stream")
async def send_message_stream(
    payload: SendMessageInput,
    request: Request,
    current_user: dict[str, Any] = Depends(require_auth),
):
    store = load_store()
    user = find_user_by_id(store, current_user.get("id"))
    if not user:
        raise HTTPException(status_code=401, detail="未登录或会话无效")

    chat = find_chat_for_user(store, payload.chat_id, user.get("id"))
    if not chat:
        raise HTTPException(status_code=404, detail="chat 不存在")

    model, mask = _resolve_chat_model_and_mask(store, user, chat, payload)
    use_payload_mask = _payload_has_field(payload, "mask_id")
    use_payload_reasoning = _payload_has_field(payload, "reasoning_effort")
    reasoning_effort = _normalize_reasoning_effort(
        payload.reasoning_effort if use_payload_reasoning else chat.get("reasoning_effort")
    )
    message_text = str(payload.message or "").strip()
    raw_attachments = []
    for att in payload.attachments or []:
        if hasattr(att, "model_dump"):
            raw_attachments.append(att.model_dump())
        elif hasattr(att, "dict"):
            raw_attachments.append(att.dict())
        else:
            raw_attachments.append(att)
    attachments = _normalize_attachments(raw_attachments)
    if not message_text and not attachments:
        raise HTTPException(status_code=400, detail="消息内容和附件不能同时为空")

    url, protocol = _resolve_upstream_target(
        base_url=model.get("base_url", ""),
        model_name=model.get("name", ""),
        message_text=message_text,
    )
    if _looks_like_svg_text_request(message_text) and protocol == "images_generations":
        raise HTTPException(
            status_code=400,
            detail="当前模型仅支持位图图片生成；SVG/矢量代码请求请改用文本模型。",
        )

    _append_user_message(chat, message_text, attachments)

    req_messages = _build_request_messages(chat, mask, protocol=protocol)
    enable_image_generation = protocol == "responses" and _looks_like_image_generation_request(
        message_text,
        model.get("name", ""),
    )
    headers = {"Content-Type": "application/json"}
    api_key = model.get("api_key", "").strip()
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    body = _build_upstream_body(
        protocol=protocol,
        model_name=model["name"],
        req_messages=req_messages,
        temperature=payload.temperature,
        reasoning_effort=reasoning_effort,
        stream=protocol != "images_generations",
        enable_image_generation=enable_image_generation,
        image_prompt=message_text,
    )

    chat["updated_at"] = now_ts()
    chat["model_id"] = model["id"]
    if use_payload_mask:
        chat["mask_id"] = payload.mask_id
    if use_payload_reasoning:
        chat["reasoning_effort"] = reasoning_effort
    save_store(store)

    try:
        audit_request_params = await _extract_request_params_for_audit(request)
    except Exception:
        audit_request_params = {"body": "[请求参数解析失败]"}
    audit_request_params = _enrich_request_params_for_audit(
        request=request,
        request_params=audit_request_params,
        store=store,
        user=user,
    )

    async def event_stream():
        chunks: list[str] = []
        generated_attachments: list[dict[str, Any]] = []
        latest_payload_obj: dict[str, Any] | None = None
        latest_final_text = ""
        stream_audit_logged = False

        def _log_stream_audit(status_code: int, extra: dict[str, Any] | None = None) -> None:
            nonlocal stream_audit_logged
            if stream_audit_logged:
                return
            stream_audit_logged = True
            _audit_log_api_request(
                request=request,
                status_code=status_code,
                user=user,
                request_params=audit_request_params,
                extra=extra,
            )

        async def _handle_stream_payload(payload_obj: dict[str, Any]):
            nonlocal latest_payload_obj, latest_final_text
            latest_payload_obj = payload_obj
            final_text = _extract_stream_final_text(payload_obj, protocol)
            if final_text:
                latest_final_text = final_text

            raw_piece = _extract_stream_delta_text(payload_obj, protocol)
            emitted_text = "".join(chunks)
            delta = _dedupe_stream_piece(raw_piece, emitted_text)
            if delta:
                chunks.append(delta)
                yield f"data: {json.dumps({'type': 'delta', 'delta': delta}, ensure_ascii=False)}\n\n"

            images = _extract_generated_image_attachments(payload_obj)
            if images:
                _merge_attachments_unique(generated_attachments, images)

        try:
            if protocol == "images_generations":
                payload_obj = await _call_upstream_chat_completion(url=url, headers=headers, body=body)
                assistant_text = _extract_assistant_text(payload_obj, protocol=protocol)
                images = _extract_generated_image_attachments(payload_obj)
                if images:
                    _merge_attachments_unique(generated_attachments, images)
                svg_attachments = _extract_svg_attachments_from_text(
                    assistant_text,
                    start_index=len(generated_attachments) + 1,
                )
                if svg_attachments:
                    _merge_attachments_unique(generated_attachments, svg_attachments)
                if not assistant_text and not generated_attachments:
                    assistant_text = "[经流式传输完成，但无文本输出]"

                assistant_msg = _append_assistant_message_to_latest_chat(
                    chat_id=chat["id"],
                    user_id=user["id"],
                    assistant_text=assistant_text,
                    model_id=model["id"],
                    assistant_mask_id=mask.get("id") if mask else None,
                    assistant_agent_name=mask.get("name") if mask else None,
                    assistant_attachments=generated_attachments,
                )
                if assistant_msg is None:
                    detail = "chat 已被删除"
                    _log_stream_audit(404, {"流式结果": "失败", "错误": detail})
                    yield f"data: {json.dumps({'type': 'error', 'error': detail}, ensure_ascii=False)}\n\n"
                    return

                _log_stream_audit(200, {"流式结果": "完成"})
                yield f"data: {json.dumps({'type': 'done', 'assistant': assistant_msg, 'chat_id': chat['id']}, ensure_ascii=False)}\n\n"
                return

            async with httpx.AsyncClient(timeout=httpx.Timeout(180.0, connect=30.0)) as client:
                async with client.stream("POST", url, headers=headers, json=body) as res:
                    if res.status_code >= 400:
                        raw = await res.aread()
                        raw_text = raw.decode("utf-8", errors="ignore")
                        err = raw_text[:500]
                        try:
                            parsed = json.loads(raw_text)
                            if isinstance(parsed, dict):
                                if isinstance(parsed.get("detail"), str):
                                    err = parsed["detail"][:500]
                                elif isinstance(parsed.get("error"), dict) and isinstance(
                                    parsed["error"].get("message"),
                                    str,
                                ):
                                    err = parsed["error"]["message"][:500]
                        except Exception:
                            pass
                        raise HTTPException(status_code=502, detail=f"上游错误({res.status_code}): {err}")

                    pending_data_lines: list[str] = []
                    async for line in res.aiter_lines():
                        if line is None:
                            continue
                        line = line.rstrip("\r")
                        if not line:
                            if pending_data_lines:
                                data_text = "\n".join(pending_data_lines).strip()
                                pending_data_lines = []
                                if data_text and data_text != "[DONE]":
                                    try:
                                        payload_obj = json.loads(data_text)
                                    except json.JSONDecodeError:
                                        payload_obj = None
                                    if isinstance(payload_obj, dict):
                                        async for out in _handle_stream_payload(payload_obj):
                                            yield out
                            continue

                        if line.startswith("data:"):
                            pending_data_lines.append(line[5:].lstrip())

                    if pending_data_lines:
                        data_text = "\n".join(pending_data_lines).strip()
                        if data_text and data_text != "[DONE]":
                            try:
                                payload_obj = json.loads(data_text)
                            except json.JSONDecodeError:
                                payload_obj = None
                            if isinstance(payload_obj, dict):
                                async for out in _handle_stream_payload(payload_obj):
                                    yield out

            assistant_text = "".join(chunks).strip()
            if latest_final_text.strip():
                assistant_text = latest_final_text.strip()
            if latest_payload_obj is not None:
                if not assistant_text:
                    fallback_text = _extract_assistant_text(latest_payload_obj, protocol)
                    if fallback_text:
                        assistant_text = fallback_text.strip()
                fallback_images = _extract_generated_image_attachments(latest_payload_obj)
                if fallback_images:
                    _merge_attachments_unique(generated_attachments, fallback_images)
            svg_attachments = _extract_svg_attachments_from_text(
                assistant_text,
                start_index=len(generated_attachments) + 1,
            )
            if svg_attachments:
                _merge_attachments_unique(generated_attachments, svg_attachments)
            if not assistant_text and not generated_attachments:
                assistant_text = "[空响应]"
            assistant_msg = _append_assistant_message_to_latest_chat(
                chat_id=chat["id"],
                user_id=user["id"],
                assistant_text=assistant_text,
                model_id=model["id"],
                assistant_mask_id=mask.get("id") if mask else None,
                assistant_agent_name=mask.get("name") if mask else None,
                assistant_attachments=generated_attachments,
            )
            if assistant_msg is None:
                detail = "chat 已被删除"
                _log_stream_audit(404, {"流式结果": "失败", "错误": detail})
                yield f"data: {json.dumps({'type': 'error', 'error': detail}, ensure_ascii=False)}\n\n"
                return

            _log_stream_audit(200, {"流式结果": "完成"})
            yield f"data: {json.dumps({'type': 'done', 'assistant': assistant_msg, 'chat_id': chat['id']}, ensure_ascii=False)}\n\n"
        except asyncio.CancelledError:
            partial_text = "".join(chunks).strip()
            partial_svg_attachments = _extract_svg_attachments_from_text(
                partial_text,
                start_index=len(generated_attachments) + 1,
            )
            if partial_svg_attachments:
                _merge_attachments_unique(generated_attachments, partial_svg_attachments)
            if partial_text or generated_attachments:
                _append_assistant_message_to_latest_chat(
                    chat_id=chat["id"],
                    user_id=user["id"],
                    assistant_text=partial_text,
                    model_id=model["id"],
                    assistant_mask_id=mask.get("id") if mask else None,
                    assistant_agent_name=mask.get("name") if mask else None,
                    assistant_attachments=generated_attachments,
                )
            _log_stream_audit(499, {"流式结果": "客户端取消"})
            raise
        except HTTPException as e:
            detail = e.detail if isinstance(e.detail, str) else str(e.detail)
            _log_stream_audit(e.status_code, {"流式结果": "失败", "错误": detail})
            yield f"data: {json.dumps({'type': 'error', 'error': detail}, ensure_ascii=False)}\n\n"
        except Exception as e:
            detail = f"流式请求失败: {e}"
            _log_stream_audit(500, {"流式结果": "失败", "错误": detail})
            yield f"data: {json.dumps({'type': 'error', 'error': detail}, ensure_ascii=False)}\n\n"

    request.state.skip_auto_audit = True
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/admin/users")
def admin_get_users(_admin: dict[str, Any] = Depends(require_admin)):
    store = load_store()
    return {"users": _admin_users_for_state(store)}


@app.post("/api/admin/users")
def admin_create_user(payload: AdminUserCreateInput, _admin: dict[str, Any] = Depends(require_admin)):
    store = load_store()

    username = _normalize_username(payload.username)
    if not username:
        raise HTTPException(status_code=400, detail="用户名不能为空")
    if find_user_by_username(store, username):
        raise HTTPException(status_code=400, detail="用户名已存在")

    role = _normalize_role(payload.role)
    ts = now_ts()
    creator_admin = find_user_by_id(store, _admin.get("id")) if isinstance(_admin, dict) else None

    default_builtin_mask = _configured_default_builtin_mask(store)
    default_mask_id = default_builtin_mask.get("id") if default_builtin_mask else None
    default_model_id = _default_model_id_for_new_user(store, creator_admin)

    user = {
        "id": new_id(),
        "username": username,
        "password_sha256": sha256_hex(payload.password),
        "role": role,
        "enabled": True,
        "default_model_id": default_model_id,
        "default_mask_id": default_mask_id,
        "message_text_ttl_hours": 0,
        "message_media_ttl_hours": 0,
        "created_at": ts,
        "updated_at": ts,
    }
    _ensure_user_default_model(store, user)
    store.setdefault("users", []).append(user)
    save_store(store)
    return {"ok": True, "user": sanitize_user_for_client(user)}


@app.post("/api/admin/users/{user_id}/password")
def admin_reset_user_password(
    user_id: str,
    payload: AdminUserPasswordInput,
    _admin: dict[str, Any] = Depends(require_admin),
):
    store = load_store()
    user = find_user_by_id(store, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="用户不存在")

    user["password_sha256"] = sha256_hex(payload.password)
    user["updated_at"] = now_ts()
    save_store(store)
    return {"ok": True}


@app.delete("/api/admin/users/{user_id}")
def admin_delete_user(user_id: str, admin_user: dict[str, Any] = Depends(require_admin)):
    store = load_store()
    user = find_user_by_id(store, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="用户不存在")
    if user.get("id") == admin_user.get("id"):
        raise HTTPException(status_code=400, detail="不能删除当前登录管理员")

    if user.get("role") == "admin":
        admin_count = sum(1 for u in store.get("users", []) if u.get("role") == "admin")
        if admin_count <= 1:
            raise HTTPException(status_code=400, detail="至少保留一个管理员账号")

    removed_mask_ids = {m.get("id") for m in store.get("masks", []) if m.get("owner_user_id") == user_id}

    store["users"] = [u for u in store.get("users", []) if u.get("id") != user_id]
    store["chats"] = [c for c in store.get("chats", []) if c.get("user_id") != user_id]
    store["masks"] = [m for m in store.get("masks", []) if m.get("owner_user_id") != user_id]

    if removed_mask_ids:
        for u in store.get("users", []):
            if u.get("default_mask_id") in removed_mask_ids:
                u["default_mask_id"] = None
                u["updated_at"] = now_ts()
        for c in store.get("chats", []):
            if c.get("mask_id") in removed_mask_ids:
                c["mask_id"] = None
                c["updated_at"] = now_ts()

    save_store(store)
    return {"ok": True}


@app.post("/api/admin/app-settings")
def admin_update_app_settings(
    payload: AdminAppSettingsInput,
    _admin: dict[str, Any] = Depends(require_admin),
):
    store = load_store()
    app_meta = store.get("app") or {}

    title = (payload.app_title or "").strip()
    subtitle = (payload.app_subtitle or "").strip()
    if title:
        app_meta["title"] = title
    else:
        app_meta["title"] = DEFAULT_APP_TITLE
    if subtitle:
        app_meta["subtitle"] = subtitle
    else:
        app_meta["subtitle"] = DEFAULT_APP_SUBTITLE
    if _payload_has_field(payload, "app_icon_data_url"):
        raw_icon = str(payload.app_icon_data_url or "").strip()
        if raw_icon:
            icon_filename = _store_app_icon_data_url(raw_icon)
            if not icon_filename:
                raise HTTPException(status_code=400, detail="图标格式无效或超过 512KB")
            app_meta["icon_filename"] = icon_filename
        else:
            app_meta["icon_filename"] = ""
            _remove_app_icon_files()
        app_meta.pop("icon_data_url", None)
    app_meta["updated_at"] = now_ts()
    store["app"] = app_meta

    save_store(store)

    return {
        "ok": True,
        "app": _serialize_app_meta_for_client(app_meta),
    }


@app.post("/api/admin/chats/clear")
def admin_clear_all_chats(_admin: dict[str, Any] = Depends(require_admin)):
    store = load_store()
    removed_chats = _clear_chats_in_store(store, None)
    if removed_chats > 0:
        save_store(store)
    return {"ok": True, "removed_chats": removed_chats}



