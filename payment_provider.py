"""Заглушки оплаты в боте: карта и СБП (QR) до подключения эквайринга клуба."""

from __future__ import annotations

import io
import logging
import uuid

import qrcode

logger = logging.getLogger(__name__)


def generate_stub_reference(prefix: str, payment_id: int) -> str:
    return f"{prefix}-{payment_id}-{uuid.uuid4().hex[:8]}"


def build_sbp_payload(amount: float, reference: str) -> str:
    """Тестовый payload для QR-заглушки СБП (не настоящий платёж)."""
    return f"STUB-SBP|amount={amount:.2f}RUB|ref={reference}"


def make_qr_image_bytes(payload: str) -> bytes:
    img = qrcode.make(payload)
    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    return buffer.getvalue()
