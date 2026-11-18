"""Transactional, acknowledgment-based binlog consumption.

This module builds on :class:`~pymysqlreplication.BinLogStreamReader` and adds
a transaction boundary on top of the raw event stream:

* :meth:`TransactionalBinLogConsumer.read_transaction` returns a complete
  :class:`TransactionBatch` once the commit boundary of a transaction has been
  observed.  Several batches may be delivered (and therefore in flight) before
  any of them is acknowledged.
* :meth:`TransactionalBinLogConsumer.ack` accepts acknowledgments in any order.
  The durable checkpoint only advances over the *continuous prefix* of
  delivered and acknowledged transactions, so a gap keeps every later
  transaction from being considered committed.
* On connection failure or when the stream is reopened, reading resumes from
  that committed checkpoint (or, before the first acknowledgment, from the
  original start position).  Every reopen starts a new delivery attempt and
  tokens minted by previous attempts can no longer be acknowledged.

The reader position (where the network stream currently is) is always kept
separate from the committed consumption position (what was durably
acknowledged); one can never be reported as the other.

This module never writes to any business table.  The only persistence it
performs goes through the user supplied :class:`CheckpointStore`.  Checkpoint
store write failures never move the committed position forward.

Delivery semantics
------------------
Batches are delivered at-least-once: transactions past the last durable
checkpoint may be redelivered after a reconnect.  Whether the combination with
a downstream system is exactly-once depends entirely on the downstream side
(idempotent writes, transactional outbox, ...); this library does not and
cannot promise end-to-end exactly-once delivery to arbitrary sinks.
"""

import json
import os
import re
import tempfile
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import pymysql

from .binlogstream import BinLogStreamReader, MYSQL_EXPECTED_ERROR_CODES
from .event import (
    GtidEvent,
    HeartbeatLogEvent,
    MariadbGtidEvent,
    QueryEvent,
    RotateEvent,
    StopEvent,
    XAPrepareEvent,
    XidEvent,
)
from .row_event import TableMapEvent


# Connection errors that are safe to retry by reopening the stream.  The
# reader itself recovers from 2006/2013 while reading packets; a refused
# connection (2003) is raised while opening the socket and retried here.
RETRYABLE_CONNECTION_ERROR_CODES = (2003,) + tuple(MYSQL_EXPECTED_ERROR_CODES)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class CheckpointError(Exception):
    """Raised when a checkpoint could not be persisted or loaded."""


class StaleDeliveryTokenError(Exception):
    """Raised when acknowledging a batch that does not belong to the current
    delivery attempt.

    This happens after a stream reopen: every batch handed out before the
    reopen carries an old attempt id and its token must not be used to confirm
    work against the new attempt.  Such batches are redelivered with fresh
    tokens.
    """


# ---------------------------------------------------------------------------
# Positions and batches
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BinLogPosition:
    """An immutable binlog file/position pair.

    ``log_pos`` is the position of the *next* event, i.e. the end offset of the
    event this position was taken from.  Resuming from a position replays the
    event whose bytes start at that offset.
    """

    log_file: str
    log_pos: int

    def to_dict(self) -> Dict[str, Any]:
        return {"log_file": self.log_file, "log_pos": self.log_pos}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BinLogPosition":
        return cls(log_file=data["log_file"], log_pos=int(data["log_pos"]))


@dataclass
class TransactionBatch:
    """A complete transaction delivered at a commit boundary.

    :ivar events: the user-visible events belonging to the transaction, in
        binlog order.  Internal control events (table maps, rotations,
        GTID/begin markers, ...) are never part of this list even when table
        or schema filters are in use.
    :ivar token: opaque acknowledgment token to hand back to
        :meth:`TransactionalBinLogConsumer.ack`.
    :ivar start_position: binlog position of the transaction's first event.
    :ivar end_position: binlog position just after the commit event.  This is
        the position the durable checkpoint takes when the batch is
        acknowledged.
    :ivar sequence: monotonically increasing delivery sequence, unique for the
        life of the consumer.
    :ivar gtid: GTID of the transaction when GTID logging is in use, else
        ``None``.
    :ivar empty: ``True`` for transactions that committed without any
        user-visible event (e.g. an empty ``COMMIT``).  Empty batches can be
        acknowledged and advance the checkpoint like any other batch.
    """

    events: List[Any]
    token: "DeliveryToken"
    start_position: BinLogPosition
    end_position: BinLogPosition
    sequence: int
    gtid: Optional[str] = None
    empty: bool = False

    def __iter__(self):
        return iter(self.events)

    def __len__(self):
        return len(self.events)


@dataclass(frozen=True)
class DeliveryToken:
    """Acknowledgment token of a delivered batch.

    Tokens are opaque to callers: only the whole object (or the tuple produced
    by :meth:`as_tuple`) must be passed back to
    :meth:`TransactionalBinLogConsumer.ack`.
    """

    sequence: int
    attempt: int
    end_position: BinLogPosition

    def as_tuple(self) -> Tuple[int, int]:
        """Compact ``(sequence, attempt)`` representation of the token.

        The tuple form is convenient to store alongside pending work.  It is
        accepted by :meth:`TransactionalBinLogConsumer.ack` together with the
        full token object.
        """

        return (self.sequence, self.attempt)


# ---------------------------------------------------------------------------
# Checkpoint stores
# ---------------------------------------------------------------------------


class CheckpointStore(ABC):
    """Persistence interface for the committed consumption checkpoint.

    Implementations must make :meth:`save` durable before returning
    (``fsync``/equivalent for file based stores).  When :meth:`save` raises,
    the consumer keeps the previous checkpoint and never moves the committed
    position forward.
    """

    @abstractmethod
    def save(self, position: BinLogPosition) -> None:
        """Persist ``position`` as the new committed checkpoint.

        Raises :class:`CheckpointError` (or any other exception) when the
        position could not be made durable.
        """

    @abstractmethod
    def load(self) -> Optional[BinLogPosition]:
        """Return the last saved checkpoint or ``None`` on a fresh store."""


class MemoryCheckpointStore(CheckpointStore):
    """Checkpoint store keeping the position in process memory.

    Useful for tests and for pipelines that manage their own durability
    elsewhere.  Nothing is written to disk or to any database table.
    """

    def __init__(self, initial: Optional[BinLogPosition] = None):
        self._position = initial
        self.save_failures_remaining = 0

    def save(self, position: BinLogPosition) -> None:
        if self.save_failures_remaining > 0:
            self.save_failures_remaining -= 1
            raise CheckpointError("simulated checkpoint write failure")
        self._position = position

    def load(self) -> Optional[BinLogPosition]:
        return self._position


class FileCheckpointStore(CheckpointStore):
    """Atomic JSON file checkpoint store.

    The checkpoint is written to a temporary file in the same directory and
    atomically renamed over the destination, then the directory is fsynced.
    A crashed write therefore leaves either the previous checkpoint or the new
    one, never a truncated file.
    """

    def __init__(self, path: str):
        self.path = path

    def save(self, position: BinLogPosition) -> None:
        directory = os.path.dirname(os.path.abspath(self.path))
        try:
            fd, tmp_path = tempfile.mkstemp(
                prefix=".checkpoint-", dir=directory
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    json.dump(position.to_dict(), handle)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(tmp_path, self.path)
                self._fsync_directory(directory)
            except BaseException:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                raise
        except OSError as exc:
            raise CheckpointError(
                f"Unable to persist checkpoint at {self.path}: {exc}"
            ) from exc

    def load(self) -> Optional[BinLogPosition]:
        if not os.path.exists(self.path):
            return None
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            return BinLogPosition.from_dict(data)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise CheckpointError(
                f"Unable to load checkpoint at {self.path}: {exc}"
            ) from exc

    @staticmethod
    def _fsync_directory(directory: str) -> None:
        try:
            dir_fd = os.open(directory, os.O_RDONLY)
        except OSError:
            # fsync of a directory is unsupported on some platforms; the
            # atomic rename already guarantees file contents correctness.
            return
        try:
            os.fsync(dir_fd)
        except OSError:
            pass
        finally:
            os.close(dir_fd)


# ---------------------------------------------------------------------------
# Internal bookkeeping
# ---------------------------------------------------------------------------


# Events that frame transactions but must never be handed to the user inside
# a batch.  TableMap/Rotate/FormatDescription/etc. are internal control events
# the stream reader needs in order to decode later events.
_INTERNAL_CONTROL_EVENT_TYPES = tuple(
    [
        RotateEvent,
        TableMapEvent,
        StopEvent,
        HeartbeatLogEvent,
    ]
)

# Events marking (part of) a transaction boundary.
_BOUNDARY_EVENT_TYPES = tuple(
    [
        GtidEvent,
        MariadbGtidEvent,
        XidEvent,
        XAPrepareEvent,
    ]
)

_BEGIN_RE = re.compile(r"^\s*BEGIN\s*$", re.IGNORECASE)
_COMMIT_RE = re.compile(r"^\s*(?:XA\s+)?COMMIT(?:\s+WORK)?\s*$", re.IGNORECASE)
_ROLLBACK_RE = re.compile(
    r"^\s*(?:XA\s+)?ROLLBACK(?:\s+WORK)?\s*$", re.IGNORECASE
)


@dataclass
class _PendingBatch:
    """Bookkeeping of a delivered, not yet fully acknowledged batch."""

    sequence: int
    attempt: int
    end_position: BinLogPosition
    acked: bool = False


@dataclass
class _OpenGroup:
    """Events accumulated while a transaction is being read."""

    events: List[Any] = field(default_factory=list)
    gtid: Optional[str] = None
    start_position: Optional[BinLogPosition] = None
    last_event: Any = None


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------


class TransactionalBinLogConsumer(object):
    """Read binlog events grouped into acknowledged transactions.

    The constructor accepts the same connection and filtering parameters as
    :class:`~pymysqlreplication.BinLogStreamReader` (plus a few consumer
    specific ones).  The underlying stream is opened lazily on the first
    :meth:`read_transaction` call.

    :param connection_settings: MySQL connection settings, identical to
        ``BinLogStreamReader``.
    :param server_id: replication slave server id, identical to
        ``BinLogStreamReader``.
    :param checkpoint_store: persistence backend for the committed
        checkpoint.  Required, because acknowledgment without durability would
        provide no replay guarantee.
    :param only_events: events the caller wants to receive inside batches.
        Internal control and transaction boundary events are always retained
        internally regardless of this filter, so filtered row events can never
        corrupt transaction boundaries.  ``None`` keeps every supported event.
    :param blocking: forwarded to the stream reader.
    :param reconnect_retries: number of reconnect attempts after a connection
        failure before giving up.  ``None`` (the default) retries forever.
    :param reconnect_delay: seconds to wait between reconnect attempts.
    :param reader_factory: callable used to create the underlying
        :class:`~pymysqlreplication.BinLogStreamReader`; mainly a testing seam.
    """

    def __init__(
        self,
        connection_settings,
        server_id,
        checkpoint_store,
        ctl_connection_settings=None,
        resume_stream=False,
        blocking=True,
        only_events=None,
        log_file=None,
        log_pos=None,
        end_log_pos=None,
        only_tables=None,
        ignored_tables=None,
        only_schemas=None,
        ignored_schemas=None,
        freeze_schema=False,
        skip_to_timestamp=None,
        report_slave=None,
        slave_uuid=None,
        slave_heartbeat=None,
        is_mariadb=False,
        annotate_rows_event=False,
        ignore_decode_errors=False,
        verify_checksum=False,
        enable_logging=True,
        use_column_name_cache=False,
        reconnect_retries=None,
        reconnect_delay=1.0,
        reader_factory=None,
    ):
        if reader_factory is None:
            reader_factory = BinLogStreamReader

        self._reader_factory = reader_factory
        self._checkpoint_store = checkpoint_store
        self._only_events = frozenset(only_events) if only_events else None
        self._reconnect_retries = reconnect_retries
        self._reconnect_delay = reconnect_delay

        # Reader construction parameters.  auto_position is deliberately not
        # exposed: this consumer checkpoints file/pos coordinates and mixing
        # the two resume modes would silently corrupt replay.
        self._reader_kwargs = dict(
            connection_settings=connection_settings,
            server_id=server_id,
            ctl_connection_settings=ctl_connection_settings,
            resume_stream=resume_stream,
            blocking=blocking,
            log_file=log_file,
            log_pos=log_pos,
            end_log_pos=end_log_pos,
            only_tables=only_tables,
            ignored_tables=ignored_tables,
            only_schemas=only_schemas,
            ignored_schemas=ignored_schemas,
            freeze_schema=freeze_schema,
            skip_to_timestamp=skip_to_timestamp,
            report_slave=report_slave,
            slave_uuid=slave_uuid,
            slave_heartbeat=slave_heartbeat,
            is_mariadb=is_mariadb,
            annotate_rows_event=annotate_rows_event,
            ignore_decode_errors=ignore_decode_errors,
            verify_checksum=verify_checksum,
            enable_logging=enable_logging,
            use_column_name_cache=use_column_name_cache,
        )

        self._reader = None
        # Bumped at every successful stream (re)open; invalidates old tokens.
        self._attempt = 0
        # Monotonic delivery sequence, unique for the life of the consumer.
        self._next_sequence = 1
        # Delivered but not yet removed by prefix advancement, keyed by
        # sequence.
        self._pending = {}  # type: Dict[int, _PendingBatch]
        # Committed checkpoint; None until the first successful save.
        self._committed = checkpoint_store.load()
        # Lowest position that must still be replayed after a reopen: the
        # committed checkpoint, or the start of the oldest delivered but
        # uncommitted transaction (or the original stream start, captured on
        # the first successful connection).
        self._delivery_floor = self._committed
        self._open_group = None
        self._closed = False

    # -- lifecycle ---------------------------------------------------------

    def close(self):
        """Close the underlying stream.  Pending state is left untouched."""

        self._closed = True
        if self._reader is not None:
            self._reader.close()
            self._reader = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def _ensure_reader(self):
        if self._closed:
            raise RuntimeError("Consumer is closed")
        if self._reader is not None:
            return self._reader

        self._reader = self._reader_factory(
            resume_position_callback=self._resume_position,
            on_reconnect_callback=self._on_reconnect,
            **self._reader_kwargs,
        )
        return self._reader

    def _on_reconnect(self):
        # New delivery attempt: everything delivered by the old connection is
        # stale and will be replayed from the delivery floor.
        self._attempt += 1
        self._pending.clear()
        self._open_group = None

    def _resume_position(self):
        """Position the reader must reopen the stream at.

        This is *never* the raw network read position: it is the last durable
        checkpoint or, failing that, the start of the oldest uncommitted
        delivery.
        """

        if self._delivery_floor is not None:
            return (
                self._delivery_floor.log_file,
                self._delivery_floor.log_pos,
            )
        return None

    # -- positions ---------------------------------------------------------

    @property
    def committed_position(self):
        """Last durably acknowledged position, or ``None`` before the first
        successful :meth:`ack`."""

        return self._committed

    @property
    def read_position(self):
        """Current raw stream read position.

        This is where the underlying network connection is, not a consumption
        checkpoint: it may be arbitrarily far ahead of
        :attr:`committed_position` while batches are in flight.
        """

        reader = self._reader
        if reader is None or reader.log_file is None:
            return None
        return BinLogPosition(log_file=reader.log_file, log_pos=reader.log_pos)

    # -- transaction grouping ---------------------------------------------

    def read_transaction(self):
        """Read the next complete transaction.

        Returns a :class:`TransactionBatch` at the commit boundary, or
        ``None`` when the underlying non-blocking stream reached its end (a
        blocking stream does not return ``None`` unless it is closed).
        """

        reader = self._ensure_reader()
        while True:
            event = self._fetch_event(reader)
            if event is None:
                return None

            batch = self._handle_event(event)
            if batch is not None:
                return batch

    def _fetch_event(self, reader):
        """fetchone with reopen handling and retry on connection errors.

        The reader reopens its own connection after an OperationalError or an
        EOF; the reopen applies ``_resume_position`` and fires
        ``_on_reconnect`` through the constructor hooks.  A refused reopen
        (error 2003) propagates out of the reader and is retried here.
        """

        attempts = 0
        while True:
            try:
                return reader.fetchone()
            except pymysql.err.OperationalError as exc:
                code = exc.args[0] if exc.args else None
                if code not in RETRYABLE_CONNECTION_ERROR_CODES:
                    raise
                if (
                    self._reconnect_retries is not None
                    and attempts >= self._reconnect_retries
                ):
                    raise
                attempts += 1
                if self._reconnect_delay:
                    time.sleep(self._reconnect_delay)

    def _handle_event(self, event):
        # GTID markers open a new group, flushing any group left open without
        # an explicit boundary (defensive; normally boundaries pair up).
        if isinstance(event, (GtidEvent, MariadbGtidEvent)):
            batch = self._deliver_open_group(event)
            self._open_group = _OpenGroup(gtid=event.gtid)
            return batch

        if isinstance(event, XidEvent):
            # Commit of a row/XA-capable transaction.  An XID without a
            # preceding marker (statement mode corner cases) still produces a
            # batch, possibly an empty one.
            group = self._open_group
            self._open_group = None
            return self._build_batch(group or _OpenGroup(), event)

        if isinstance(event, XAPrepareEvent):
            # XA PREPARE is the durable boundary of the prepared transaction;
            # the prepare event itself is a boundary marker, not user data.
            group = self._open_group
            self._open_group = None
            return self._build_batch(group or _OpenGroup(), event)

        if isinstance(event, QueryEvent):
            kind = self._query_kind(event)
            if kind == "begin":
                batch = self._deliver_open_group(event)
                self._open_group = _OpenGroup()
                return batch
            if kind == "commit":
                group = self._open_group
                self._open_group = None
                # Empty transactions (BEGIN; COMMIT;) must still be
                # deliverable so their checkpoint can advance.
                return self._build_batch(group or _OpenGroup(), event)
            if kind == "rollback":
                self._open_group = None
                return None
            # Ordinary statement (DDL, DML in statement mode, ...).  In
            # autocommit mode it is its own single-statement transaction.
            group = self._open_group
            own_group = group is None
            if own_group:
                group = _OpenGroup()
            self._append_user_event(group, event)
            if own_group:
                self._open_group = None
                return self._build_batch(group, event)
            return None

        if self._is_internal_control_event(event):
            # Rotation, table maps, heartbeats and stop markers keep the
            # stream working but never belong to a transaction batch.
            return None

        group = self._open_group
        if group is None:
            # Data event outside an explicit transaction: treat it as its own
            # transaction rather than losing it.
            group = _OpenGroup()
            self._append_user_event(group, event)
            return self._build_batch(group, event)

        self._append_user_event(group, event)
        return None

    def _append_user_event(self, group, event):
        if not self._is_user_visible(event):
            return
        if group.start_position is None:
            group.start_position = self._event_start_position(event)
        group.events.append(event)
        group.last_event = event

    def _deliver_open_group(self, next_event):
        """Flush a group that was never explicitly committed. Returns a batch
        or ``None`` when there was nothing open."""

        group = self._open_group
        if group is None:
            return None
        self._open_group = None
        return self._build_batch(group, next_event)

    def _build_batch(self, group, boundary_event):
        start_position = group.start_position
        if start_position is None:
            start_position = self._event_start_position(boundary_event)
        # The checkpoint position is always the end of the commit boundary
        # (XID/COMMIT/XA PREPARE), so resuming from it never replays the
        # committed transaction, including across binlog file rotation.
        end_position = self._event_end_position(boundary_event)

        sequence = self._next_sequence
        self._next_sequence += 1
        attempt = self._attempt
        token = DeliveryToken(
            sequence=sequence,
            attempt=attempt,
            end_position=end_position,
        )
        batch = TransactionBatch(
            events=group.events,
            token=token,
            start_position=start_position,
            end_position=end_position,
            sequence=sequence,
            gtid=group.gtid,
            empty=len(group.events) == 0,
        )
        self._pending[sequence] = _PendingBatch(
            sequence=sequence,
            attempt=attempt,
            end_position=end_position,
        )
        if self._delivery_floor is None:
            self._delivery_floor = start_position
        return batch

    def _is_user_visible(self, event):
        if self._is_internal_control_event(event):
            return False
        if isinstance(event, _BOUNDARY_EVENT_TYPES):
            return False
        if self._only_events is None:
            return True
        return isinstance(event, tuple(self._only_events))

    @staticmethod
    def _is_internal_control_event(event):
        return isinstance(event, _INTERNAL_CONTROL_EVENT_TYPES)

    @staticmethod
    def _query_kind(event):
        query = event.query.strip().rstrip(";").strip()
        if _BEGIN_RE.match(query):
            return "begin"
        if _COMMIT_RE.match(query):
            return "commit"
        if _ROLLBACK_RE.match(query):
            return "rollback"
        return "statement"

    def _event_start_position(self, event):
        """Position of the first byte of ``event`` within its binlog file."""

        reader = self._reader
        log_file = reader.log_file
        # packet.log_pos is the next-event offset and packet.event_size the
        # full on-disk size (header + body + optional checksum), so the
        # subtraction yields the event's start offset in the current file.
        log_pos = event.packet.log_pos - event.packet.event_size
        return BinLogPosition(log_file=log_file, log_pos=log_pos)

    def _event_end_position(self, event):
        """Next-event position right after ``event``."""

        return BinLogPosition(
            log_file=self._reader.log_file,
            log_pos=event.packet.log_pos,
        )

    # -- acknowledgment ----------------------------------------------------

    def ack(self, token):
        """Acknowledge a delivered batch.

        Acks may arrive in any order.  The checkpoint only advances over the
        continuous prefix of acknowledged batches; later acks with a gap in
        front of them are merely recorded.

        :param token: the :class:`DeliveryToken` carried by the batch, or its
            ``(sequence, attempt)`` tuple form.
        :raises StaleDeliveryTokenError: the token belongs to an older
            delivery attempt (e.g. after a reopen) or refers to an unknown
            sequence.
        :raises CheckpointError: the checkpoint store rejected the write; in
            that case the committed position is unchanged.
        """

        if isinstance(token, DeliveryToken):
            sequence, attempt = token.sequence, token.attempt
        else:
            sequence, attempt = token

        pending = self._pending.get(sequence)
        if pending is None or pending.attempt != attempt:
            raise StaleDeliveryTokenError(
                f"Token (sequence={sequence}, attempt={attempt}) is not "
                f"valid for the current delivery attempt "
                f"(attempt={self._attempt})"
            )

        pending.acked = True
        self._advance_prefix()

    def _advance_prefix(self):
        # Walk the continuous acknowledged prefix in delivery order.
        prefix_end = None
        sequence = self._lowest_pending_sequence()
        while sequence is not None:
            pending = self._pending.get(sequence)
            if pending is None or not pending.acked:
                break
            prefix_end = pending
            sequence += 1

        if prefix_end is None:
            return

        # A failed save must not move any in-memory committed state: keep the
        # acked flags so the caller can retry the same ack.
        self._checkpoint_store.save(prefix_end.end_position)
        self._committed = prefix_end.end_position
        self._delivery_floor = prefix_end.end_position

        # Drop every pending entry up to and including the prefix.
        for seq in range(self._lowest_pending_sequence(), prefix_end.sequence + 1):
            self._pending.pop(seq, None)

    def _lowest_pending_sequence(self):
        if not self._pending:
            return None
        return min(self._pending)

    # -- introspection -----------------------------------------------------

    def pending_sequences(self):
        """Sequences of batches currently delivered but not fully removed by
        prefix advancement, in delivery order."""

        return sorted(self._pending)

    @property
    def attempt(self):
        """Current delivery attempt id (0 before the first connection)."""

        return self._attempt
