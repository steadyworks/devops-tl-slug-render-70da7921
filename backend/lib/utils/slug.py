from typing import Final
from uuid import UUID

_ALPHABET: Final[str] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
_BASE: Final[int] = len(_ALPHABET)  # 62
_FIXED_LEN: Final[int] = 22  # ceil(log_62(2^128)) = 22


def uuid_to_base62(u: UUID) -> str:
    try:
        n = u.int
        if n == 0:
            encoded = _ALPHABET[0]
        else:
            encoded_chars: list[str] = []
            while n:
                n, rem = divmod(n, _BASE)
                encoded_chars.append(_ALPHABET[rem])
            encoded = "".join(reversed(encoded_chars))

        return encoded.rjust(_FIXED_LEN, _ALPHABET[0])
    except Exception:
        # Fallback: return canonical UUID string
        return str(u)
