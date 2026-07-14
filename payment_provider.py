"""
payment_provider.py
--------------------
Заглушка платёжного провайдера для оплаты бронирования через бота.

Что здесь настоящее, а что — заглушка:
- Интерфейс (кнопки «Оплатить картой» / «Оплатить по СБП (QR)», генерация
  QR-кода, тексты) — рабочий, показывается игроку сразу после записи.
- Фактическое списание денег — ЗАГЛУШКА. Никакой реальный банк/эквайер не
  подключён, поэтому в демо-режиме бот не может сам узнать, что оплата
  прошла: игрок нажимает «✅ Я оплатил», после чего администратор проверяет
  и подтверждает оплату в CRM (раздел «Оплаты»), как и раньше.

Как подключить реальную оплату картой (Telegram Payments):
1. В @BotFather: /mybots -> выбрать бота -> Payments -> подключить
   провайдера (например, тестовый провайдер Stripe для проверки без
   реальных денег, или банк, поддерживающий Telegram Payments в РФ).
2. Положить полученный provider_token в .env как PAYMENT_PROVIDER_TOKEN.
3. Готово: is_card_provider_configured() вернёт True, и bot.py вместо
   заглушки вызовет настоящий Bot.send_invoice(...) — см. process_pay_card.

Как подключить реальный приём по СБП:
- Нужен банк-эквайер/агрегатор с API приёма платежей по СБП (СБП не входит
  в стандартный Telegram Payments API). Тогда build_sbp_payload() нужно
  заменить на настоящую ссылку/QR-payload, полученный от API эквайера, а
  оплату подтверждать по вебхуку эквайера (аналогично
  process_successful_payment для карты), а не по кнопке «Я оплатил».
"""

import io
import os
import uuid

import qrcode


def is_card_provider_configured() -> bool:
    """True, если в .env указан настоящий provider_token — тогда оплата
    картой пойдёт через реальный Telegram Payments API, а не через
    заглушку с кнопкой «Я оплатил»."""
    return bool(os.getenv("PAYMENT_PROVIDER_TOKEN"))


def generate_stub_reference(prefix: str, payment_id: int) -> str:
    """Человекочитаемый номер операции для заглушки — просто чтобы у
    игрока/администратора было что сверить в переписке, реального смысла
    в платёжной системе у него нет."""
    return f"{prefix}-{payment_id}-{uuid.uuid4().hex[:8]}"


def build_sbp_payload(amount: float, reference: str) -> str:
    """Строка, которая кодируется в QR. В реальной интеграции с СБП сюда
    нужно подставить настоящую ссылку от банка/эквайера вида
    https://qr.nspk.ru/... — тогда сканирование откроет настоящее платёжное
    приложение банка."""
    return f"STUB-SBP|amount={amount:.2f}RUB|ref={reference}"


def make_qr_image_bytes(payload: str) -> bytes:
    """Генерирует PNG QR-код с заданными данными и возвращает его как bytes,
    готовые к отправке через bot.send_photo/message.answer_photo."""
    img = qrcode.make(payload)
    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    return buffer.getvalue()
