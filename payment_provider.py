"""
payment_provider.py
--------------------
Оплата бронирования через ЮKassa (СБП + карта на одной платёжной странице).

Прод-режим (заданы YOOKASSA_SHOP_ID + YOOKASSA_SECRET_KEY):
  - create_yookassa_payment() → confirmation_url для кнопки «Оплатить»
  - webhook payment.succeeded → статус «подтверждена» (см. app.py)
  - cancel_yookassa_payment() при отмене неоплаченной записи

Без ключей ЮKassa:
  - честный fallback «оплата на месте / перевод» (без фейкового QR)
  - опционально Telegram Payments, если задан PAYMENT_PROVIDER_TOKEN
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import uuid
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import qrcode

logger = logging.getLogger(__name__)

YOOKASSA_API = "https://api.yookassa.ru/v3"
YOOKASSA_SHOP_ID = (os.getenv("YOOKASSA_SHOP_ID") or "").strip()
YOOKASSA_SECRET_KEY = (os.getenv("YOOKASSA_SECRET_KEY") or "").strip()

# IP-подсети ЮKassa для входящих webhook (документация ЮKassa).
# Проверка мягкая: при PROXY/неизвестном IP логируем, но не режем жёстко
# по умолчанию — см. YOOKASSA_WEBHOOK_ENFORCE_IP.
YOOKASSA_WEBHOOK_IPS = frozenset({
    "185.71.76.0/27",
    "185.71.77.0/27",
    "77.75.153.0/25",
    "77.75.156.11",
    "77.75.156.35",
    "77.75.154.128/25",
    "2a02:5180::/32",
})


def is_yookassa_configured() -> bool:
    return bool(YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY)


def is_card_provider_configured() -> bool:
    """Telegram Payments (карта через BotFather) — запасной канал без ЮKassa."""
    return bool(os.getenv("PAYMENT_PROVIDER_TOKEN"))


def _basic_auth_header() -> str:
    token = base64.b64encode(
        f"{YOOKASSA_SHOP_ID}:{YOOKASSA_SECRET_KEY}".encode("utf-8")
    ).decode("ascii")
    return f"Basic {token}"


def _sync_request(
    method: str,
    path: str,
    body: Optional[dict] = None,
    idempotence_key: Optional[str] = None,
) -> dict:
    """Синхронный HTTP к API ЮKassa (для Flask webhook / отмен из CRM)."""
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = {
        "Authorization": _basic_auth_header(),
        "Content-Type": "application/json",
    }
    if idempotence_key:
        headers["Idempotence-Key"] = idempotence_key
    req = Request(
        f"{YOOKASSA_API}{path}",
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        logger.error("YooKassa %s %s → %s: %s", method, path, e.code, err_body)
        raise RuntimeError(f"YooKassa HTTP {e.code}: {err_body}") from e
    except URLError as e:
        logger.error("YooKassa network error %s %s: %s", method, path, e)
        raise RuntimeError(f"YooKassa network error: {e}") from e


async def _async_request(
    method: str,
    path: str,
    body: Optional[dict] = None,
    idempotence_key: Optional[str] = None,
) -> dict:
    """Асинхронный HTTP к API ЮKassa (из aiogram handlers)."""
    import aiohttp

    headers = {
        "Authorization": _basic_auth_header(),
        "Content-Type": "application/json",
    }
    if idempotence_key:
        headers["Idempotence-Key"] = idempotence_key
    auth = aiohttp.BasicAuth(YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY)
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(auth=auth, timeout=timeout) as session:
        async with session.request(
            method,
            f"{YOOKASSA_API}{path}",
            json=body,
            headers={k: v for k, v in headers.items() if k != "Authorization"},
        ) as resp:
            text = await resp.text()
            if resp.status >= 400:
                logger.error("YooKassa %s %s → %s: %s", method, path, resp.status, text)
                raise RuntimeError(f"YooKassa HTTP {resp.status}: {text}")
            return json.loads(text) if text else {}


def create_yookassa_payment_sync(
    *,
    amount: float,
    payment_id: int,
    booking_id: int,
    description: str,
    return_url: str,
    customer_phone: Optional[str] = None,
) -> Dict[str, Any]:
    """Создаёт платёж (redirect: СБП или карта на стороне ЮKassa)."""
    body = _payment_body(
        amount=amount,
        payment_id=payment_id,
        booking_id=booking_id,
        description=description,
        return_url=return_url,
        customer_phone=customer_phone,
    )
    data = _sync_request(
        "POST",
        "/payments",
        body,
        idempotence_key=str(uuid.uuid4()),
    )
    return _normalize_payment(data)


async def create_yookassa_payment(
    *,
    amount: float,
    payment_id: int,
    booking_id: int,
    description: str,
    return_url: str,
    customer_phone: Optional[str] = None,
) -> Dict[str, Any]:
    body = _payment_body(
        amount=amount,
        payment_id=payment_id,
        booking_id=booking_id,
        description=description,
        return_url=return_url,
        customer_phone=customer_phone,
    )
    data = await _async_request(
        "POST",
        "/payments",
        body,
        idempotence_key=str(uuid.uuid4()),
    )
    return _normalize_payment(data)


def _payment_body(
    *,
    amount: float,
    payment_id: int,
    booking_id: int,
    description: str,
    return_url: str,
    customer_phone: Optional[str],
) -> dict:
    body: Dict[str, Any] = {
        "amount": {
            "value": f"{float(amount):.2f}",
            "currency": "RUB",
        },
        "capture": True,
        "confirmation": {
            "type": "redirect",
            "return_url": return_url,
        },
        "description": (description or f"Бронь #{booking_id}")[:128],
        "metadata": {
            "payment_id": str(payment_id),
            "booking_id": str(booking_id),
        },
    }
    # 54-ФЗ: чек, если магазин требует и передан телефон.
    if os.getenv("YOOKASSA_SEND_RECEIPT", "0").lower() in {"1", "true", "yes"} and customer_phone:
        phone = "".join(ch for ch in str(customer_phone) if ch.isdigit() or ch == "+")
        body["receipt"] = {
            "customer": {"phone": phone},
            "items": [{
                "description": (description or "Игра в падел")[:128],
                "quantity": "1.00",
                "amount": {"value": f"{float(amount):.2f}", "currency": "RUB"},
                "vat_code": int(os.getenv("YOOKASSA_VAT_CODE", "1")),
                "payment_mode": "full_payment",
                "payment_subject": "service",
            }],
        }
    return body


def _normalize_payment(data: dict) -> Dict[str, Any]:
    confirmation = data.get("confirmation") or {}
    return {
        "id": data.get("id"),
        "status": data.get("status"),
        "confirmation_url": confirmation.get("confirmation_url"),
        "paid": bool(data.get("paid")),
        "amount": (data.get("amount") or {}).get("value"),
        "metadata": data.get("metadata") or {},
        "raw": data,
    }


async def get_yookassa_payment(provider_payment_id: str) -> Dict[str, Any]:
    data = await _async_request("GET", f"/payments/{provider_payment_id}")
    return _normalize_payment(data)


def get_yookassa_payment_sync(provider_payment_id: str) -> Dict[str, Any]:
    data = _sync_request("GET", f"/payments/{provider_payment_id}")
    return _normalize_payment(data)


async def cancel_yookassa_payment(provider_payment_id: str) -> Optional[Dict[str, Any]]:
    """Отмена неоплаченного платежа в ЮKassa (best-effort)."""
    try:
        data = await _async_request("POST", f"/payments/{provider_payment_id}/cancel", {})
        return _normalize_payment(data)
    except Exception as e:
        logger.warning("Не удалось отменить платёж ЮKassa %s: %s", provider_payment_id, e)
        return None


def parse_webhook_notification(payload: dict) -> Dict[str, Any]:
    """Разбирает тело webhook ЮKassa.

    Возвращает:
      event, provider_payment_id, status, paid, amount, payment_id, booking_id
    """
    event = payload.get("event") or ""
    obj = payload.get("object") or {}
    metadata = obj.get("metadata") or {}
    amount_obj = obj.get("amount") or {}
    payment_id = metadata.get("payment_id")
    booking_id = metadata.get("booking_id")
    try:
        payment_id_int = int(payment_id) if payment_id is not None else None
    except (TypeError, ValueError):
        payment_id_int = None
    try:
        booking_id_int = int(booking_id) if booking_id is not None else None
    except (TypeError, ValueError):
        booking_id_int = None
    try:
        amount = float(amount_obj.get("value")) if amount_obj.get("value") is not None else None
    except (TypeError, ValueError):
        amount = None
    return {
        "event": event,
        "provider_payment_id": obj.get("id"),
        "status": obj.get("status"),
        "paid": bool(obj.get("paid")),
        "amount": amount,
        "payment_id": payment_id_int,
        "booking_id": booking_id_int,
        "payment_method": ((obj.get("payment_method") or {}).get("type")),
    }


# ---------------------------------------------------------------------------
# Fallback / legacy helpers (без ЮKassa)
# ---------------------------------------------------------------------------

def generate_stub_reference(prefix: str, payment_id: int) -> str:
    return f"{prefix}-{payment_id}-{uuid.uuid4().hex[:8]}"


def build_sbp_payload(amount: float, reference: str) -> str:
    """Устарело для прода: оставляем только на случай локальных тестов QR."""
    return f"STUB-SBP|amount={amount:.2f}RUB|ref={reference}"


def make_qr_image_bytes(payload: str) -> bytes:
    img = qrcode.make(payload)
    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    return buffer.getvalue()
