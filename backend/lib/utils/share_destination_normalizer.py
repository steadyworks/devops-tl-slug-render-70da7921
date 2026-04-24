import phonenumbers as pn
from phonenumbers import NumberParseException, PhoneNumberFormat

from backend.db.data_models import (
    ShareChannelType,
)

DEFAULT_REGION = "US"  # or pull from user profile / request locale


def normalize_phone_e164(raw: str, default_region: str | None = DEFAULT_REGION) -> str:
    raw = (raw or "").strip()
    if not raw:
        raise ValueError("Phone number is empty")

    try:
        # If raw starts with +, region is ignored (which is fine).
        num = pn.parse(raw, default_region)
    except NumberParseException as e:
        raise ValueError(f"Invalid phone number: {e}")

    # Basic validity checks
    if not pn.is_possible_number(num) or not pn.is_valid_number(num):
        raise ValueError("Phone number is not a valid, dialable number")

    # Optional: ensure it's SMS-capable (mobile or fixed_line_or_mobile)
    ntype = pn.number_type(num)
    if ntype not in (
        pn.PhoneNumberType.MOBILE,
        pn.PhoneNumberType.FIXED_LINE_OR_MOBILE,
    ):
        # Many countries can send SMS to fixed-line numbers via conversion, but often not.
        # Keep or drop FIXED_LINE_OR_MOBILE depending on your provider’s capabilities.
        raise ValueError("Phone number is not a mobile/SMS-capable destination")

    # Extensions aren’t usable for SMS
    if getattr(num, "extension", None):
        raise ValueError(
            "Phone number includes an extension, which is not supported for SMS"
        )

    return pn.format_number(num, PhoneNumberFormat.E164)


def normalize_destination(channel_type: ShareChannelType, value: str) -> str:
    try:
        v = (value or "").strip()
        if channel_type == ShareChannelType.EMAIL:
            return v.lower()
        if channel_type == ShareChannelType.SMS:
            return normalize_phone_e164(v, DEFAULT_REGION)
        # apns / link: keep as-is (you can add light validation later if you want)
        return v
    except Exception:
        return value
