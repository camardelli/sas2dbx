"""LogBuffer — handler de logging em memória para streaming ao frontend.

Armazena as últimas N mensagens de log em um deque thread-safe e expõe
um endpoint de polling que retorna entradas novas desde um timestamp.

Uso:
  # Em app.py
  from sas2dbx.web.log_buffer import LogBuffer
  buf = LogBuffer(maxlen=500)
  buf.install()          # registra no root logger
  app.state.log_buffer = buf
"""

from __future__ import annotations

import logging
import threading
from collections import deque
from datetime import datetime, timezone


class _BufferHandler(logging.Handler):
    """Handler que grava entradas de log no LogBuffer."""

    def __init__(self, buffer: "LogBuffer") -> None:
        super().__init__()
        self._buf = buffer

    def emit(self, record: logging.LogRecord) -> None:
        try:
            ts = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()
            # Formata a mensagem (aplica args e exc_info)
            msg = self.format(record)
            self._buf._append(
                {
                    "ts": ts,
                    "ts_ms": int(record.created * 1000),
                    "level": record.levelname,
                    "logger": record.name,
                    "message": msg,
                }
            )
        except Exception:  # noqa: BLE001
            # Nunca deixa o handler travar o logger
            pass


class LogBuffer:
    """Buffer circular thread-safe de entradas de log.

    Args:
        maxlen: Número máximo de entradas retidas. Default 500.
    """

    def __init__(self, maxlen: int = 500) -> None:
        self._lock = threading.Lock()
        self._entries: deque[dict] = deque(maxlen=maxlen)
        self._handler: _BufferHandler | None = None
        self._seq = 0  # sequência monotônica para ordering garantido

    def install(self, level: int = logging.DEBUG) -> None:
        """Instala o handler no root logger.

        Args:
            level: Nível mínimo de log capturado. Default DEBUG
                (root logger já filtra por INFO; permite capturar DEBUG de módulos específicos).
        """
        handler = _BufferHandler(self)
        handler.setLevel(level)
        # Formatter simples — sem timestamp (já está em ts)
        handler.setFormatter(
            logging.Formatter("%(levelname)s %(name)s: %(message)s")
        )
        logging.getLogger().addHandler(handler)
        self._handler = handler

    def since(self, ts_ms: int = 0, limit: int = 200) -> list[dict]:
        """Retorna entradas mais recentes que ts_ms (milliseconds epoch UTC).

        Args:
            ts_ms: Timestamp de corte em ms epoch. 0 = todas.
            limit: Máximo de entradas retornadas.

        Returns:
            Lista de entradas em ordem cronológica.
        """
        with self._lock:
            entries = [e for e in self._entries if e["ts_ms"] > ts_ms]
        return entries[-limit:]

    def clear(self) -> None:
        """Limpa todas as entradas do buffer."""
        with self._lock:
            self._entries.clear()

    def _append(self, entry: dict) -> None:
        with self._lock:
            self._seq += 1
            entry["seq"] = self._seq
            self._entries.append(entry)
