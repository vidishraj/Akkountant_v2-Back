"""ak-erkm: cross-worker single-use state for confirm tokens
(utils/confirm_tokens.py, ak-dixt / ak-2ye9).

A row exists once a token can no longer be used: it was consumed by an
Apply ('consumed') or revoked by Cancel / an unapplied close-out
('revoked'). The token itself is stateless HMAC; "live" == no row.
The primary key on `nonce` makes check-and-mark ONE atomic INSERT, so
two gunicorn workers can never both accept the same token.

Holds no args and no secret: nonce (random, from the signed payload),
uid, tool name, state, expiry. Rows past `expires_at` are pruned.
New table → created by db.create_all() on boot (no ALTER needed).
"""
from sqlalchemy import Column, DateTime, String, func

from models.Base import Base


class ConfirmTokenState(Base):
    __tablename__ = "confirmTokenStates"

    nonce = Column(String(64), primary_key=True)
    uid = Column(String(100), nullable=False)
    tool = Column(String(100), nullable=False)
    state = Column(String(16), nullable=False)        # 'consumed' | 'revoked'
    expires_at = Column(DateTime, nullable=False, index=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
