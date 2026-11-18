"""Offline tests for the transactional, acknowledgment-based consumer.

These tests drive the real transaction grouping and acknowledgment logic with
synthetic binlog packets and a fake stream reader, so no MySQL server is
required.  Reconnect behavior against the real BinLogStreamReader is covered
with the same packet-mocking approach as test_binlogstream.py.
"""

import os
import struct

import pytest
from pymysql.protocol import MysqlPacket
from unittest.mock import Mock

import pymysql
from pymysqlreplication import (
    BinLogPosition,
    BinLogStreamReader,
    CheckpointError,
    FileCheckpointStore,
    MemoryCheckpointStore,
    StaleDeliveryTokenError,
    TransactionalBinLogConsumer,
)
from pymysqlreplication import constants
from pymysqlreplication.constants import FIELD_TYPE
from pymysqlreplication.event import (
    GtidEvent,
    HeartbeatLogEvent,
    QueryEvent,
    RotateEvent,
    XidEvent,
)
from pymysqlreplication.packet import BinLogPacketWrapper
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    TableMapEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

_ALL_EVENTS = frozenset(
    [
        GtidEvent,
        HeartbeatLogEvent,
        QueryEvent,
        RotateEvent,
        XidEvent,
        TableMapEvent,
        WriteRowsEvent,
        UpdateRowsEvent,
        DeleteRowsEvent,
    ]
)


# ---------------------------------------------------------------------------
# Synthetic packet builders
# ---------------------------------------------------------------------------


def _packet(event_type, body, timestamp=200, log_pos=100):
    header = struct.pack(
        "<cIBIIIH", b"\0", timestamp, event_type, 1, 19 + len(body), log_pos, 0
    )
    return MysqlPacket(header + body, "utf8")


def _rotate_packet(filename, position=4, timestamp=0):
    return _packet(
        constants.ROTATE_EVENT,
        struct.pack("<Q", position) + filename.encode(),
        timestamp=timestamp,
        log_pos=position,
    )


def _gtid_packet(gno=1, log_pos=120):
    # commit_flag(1) + sid(16) + gno(8) + lt_type(1) + last_committed(8) +
    # sequence_number(8), matching a 5.7+ server.
    sid = b"\x11" * 16
    body = (
        b"\x00"
        + sid
        + struct.pack("<Q", gno)
        + b"\x02"
        + struct.pack("<Q", 0)
        + struct.pack("<Q", gno)
    )
    return _packet(constants.GTID_LOG_EVENT, body, log_pos=log_pos)


def _query_packet(query, schema=b"test_db", log_pos=200):
    query_bytes = query.encode()
    fixed = struct.pack("<II", 0, 0) + bytes([len(schema)]) + struct.pack("<HH", 0, 0)
    body = fixed + schema + b"\0" + query_bytes
    return _packet(constants.QUERY_EVENT, body, log_pos=log_pos)


def _xid_packet(xid=7, log_pos=300):
    return _packet(constants.XID_EVENT, struct.pack("<Q", xid), log_pos=log_pos)


def _heartbeat_packet(ident="mysql-bin.000001"):
    return _packet(
        constants.HEARTBEAT_LOG_EVENT, ident.encode(), log_pos=0
    )


def _table_map_packet(table, column_type, schema="test_db", log_pos=150):
    schema_b = schema.encode()
    table_b = table.encode()
    metadata = struct.pack("<H", 255) if column_type == FIELD_TYPE.VARCHAR else b""
    body = (
        (7).to_bytes(6, "little")
        + struct.pack("<H", 0)
        + bytes([len(schema_b)])
        + schema_b
        + b"\0"
        + bytes([len(table_b)])
        + table_b
        + b"\0\x01"
        + bytes([column_type, len(metadata)])
        + metadata
        + b"\0"
    )
    return _packet(constants.TABLE_MAP_EVENT, body, log_pos=log_pos)


def _write_packet(value, log_pos=250):
    # V2 rows event post-header: table_id(6) + flags(2) + extra_data_length(2).
    # extra_data_length counts itself plus its extra bytes, so 2 means no
    # extra payload; _advance_to_payload then consumes no extra bytes.
    body = (
        (7).to_bytes(6, "little")
        + struct.pack("<HH", 0, 2)
        + b"\x01"  # number-of-columns length-coded int (one column)
        + b"\x01"  # columns-present bitmap: the single column is present
        + b"\0"  # per-row null bitmap: bit clear means not NULL
        + value
    )
    return _packet(constants.WRITE_ROWS_EVENT_V2, body, log_pos=log_pos)


def _write_packet_with_checksum(value, log_pos=250):
    """Same as _write_packet but with a real CRC32 binlog checksum footer.

    The on-disk event then carries a 4-byte checksum, so body-only sizing must
    not be confused with the full event size when freezing the row payload.
    """

    import zlib

    body = (
        (7).to_bytes(6, "little")
        + struct.pack("<HH", 0, 2)
        + b"\x01\x01\x00"
        + value
    )
    header = struct.pack(
        "<cIBIIIH",
        b"\0",
        200,
        constants.WRITE_ROWS_EVENT_V2,
        1,
        19 + len(body) + 4,
        log_pos,
        0,
    )
    footer = zlib.crc32(header + body).to_bytes(4, "little")
    return MysqlPacket(header + body + footer, "utf8")


def _varchar_value(text):
    raw = text.encode()
    return bytes([len(raw)]) + raw


def _update_packet(before, after, log_pos=250):
    body = (
        (7).to_bytes(6, "little")
        + struct.pack("<HH", 0, 2)
        + b"\x01\x01\x01"
        + b"\0"
        + before
        + b"\0"
        + after
    )
    return _packet(constants.UPDATE_ROWS_EVENT_V2, body, log_pos=log_pos)


# ---------------------------------------------------------------------------
# Fake reader: replays scripted "connections", each a list of packets.
# ---------------------------------------------------------------------------


class _CtlConnection:
    charset = "utf8"

    def _get_dbms(self):
        return "mysql"


class FakeReader:
    """Reader double implementing the surface used by the consumer.

    Packets are decoded with the real BinLogPacketWrapper so the consumer
    exercises the production event decoders, including lazy row decoding.
    """

    def __init__(
        self,
        connections,
        resume_position_callback=None,
        on_reconnect_callback=None,
        **kwargs,
    ):
        self._connections = list(connections)
        self._resume_position_callback = resume_position_callback
        self._on_reconnect_callback = on_reconnect_callback
        self._connection_index = 0
        self.log_file = "mysql-bin.000001"
        self.log_pos = 4
        self.table_map = {}
        self.closed = False
        self.reopen_positions = []
        self._load_connection(initial=True)

    def _load_connection(self, initial):
        scripted = self._connections[self._connection_index]
        # A real server resends fresh bytes on every new connection, so each
        # replay gets packets whose cursor starts at zero.  The scripted
        # MysqlPacket objects may already have been drained by a prior
        # connection; rebuild them from their immutable payload instead.
        # Exception markers are passed through untouched.
        self._packets = [
            item
            if isinstance(item, Exception)
            else MysqlPacket(item._data, "utf8")
            for item in scripted
        ]
        self._cursor = 0
        if not initial:
            position = None
            if self._resume_position_callback is not None:
                position = self._resume_position_callback()
            self.reopen_positions.append(position)
            if position is not None:
                self.log_file, self.log_pos = position
            if self._on_reconnect_callback is not None:
                self._on_reconnect_callback()

    def _wrap(self, packet):
        # Allow every supported event through; filtering happens in the
        # consumer.  TableMap/Rotate are always included, mirroring the real
        # reader's packet-level allowlist.
        return BinLogPacketWrapper(
            packet,
            self.table_map,
            _CtlConnection(),
            (5, 6, 0),
            False,
            _ALL_EVENTS,
            None,
            None,
            None,
            None,
            False,
            False,
            False,
            False,
            False,
            False,
            None,
        )

    def fetchone(self):
        dead_error = getattr(self, "_dead_error", None)
        if dead_error is not None:
            raise dead_error
        if getattr(self, "_exhausted", False):
            return None
        while self._cursor < len(self._packets):
            packet = self._packets[self._cursor]
            self._cursor += 1
            if isinstance(packet, Exception):
                # Mirror the real reader: the connection is dead and the next
                # fetchone reopens from the resume position.  When no further
                # connection is scripted, every subsequent call keeps failing.
                if self._connection_index + 1 < len(self._connections):
                    self._connection_index += 1
                    self._load_connection(initial=False)
                else:
                    self._dead_error = packet
                raise packet
            if packet.is_eof_packet():
                # Mirror the real reader: EOF closes the stream; the next
                # fetchone reopens it (if more scripted connections exist).
                self._reopen_or_end()
                return None
            wrapped = self._wrap(packet)
            if wrapped.event_type == constants.ROTATE_EVENT:
                self.table_map = {}
                self.log_pos = wrapped.event.position
                self.log_file = wrapped.event.next_binlog
            elif wrapped.log_pos:
                self.log_pos = wrapped.log_pos
            if (
                wrapped.event_type == constants.TABLE_MAP_EVENT
                and wrapped.event is not None
            ):
                self.table_map[wrapped.event.table_id] = wrapped.event.get_table()
            if wrapped.event is None:
                continue
            return wrapped.event
        # The scripted connection ended without an explicit EOF marker.  Mirror
        # the real reader, whose next fetchone reconnects from the resume
        # position while more connections remain, and only reports a permanent
        # end of stream once every connection has been drained.
        self._reopen_or_end()
        if getattr(self, "_exhausted", False):
            return None
        return self.fetchone()

    def _reopen_or_end(self):
        self._connection_index += 1
        if self._connection_index < len(self._connections):
            self._load_connection(initial=False)
        else:
            # No more scripted connections: mirror a reader whose stream is
            # closed, i.e. further reads keep returning None instead of
            # reopening forever.
            self._exhausted = True

    def close(self):
        self.closed = True


@pytest.fixture
def consumer_factory():
    consumers = []

    def factory(packets_or_connections, store=None, **kwargs):
        if packets_or_connections and isinstance(
            packets_or_connections[0], (list, tuple)
        ):
            connections = packets_or_connections
        else:
            connections = [packets_or_connections]
        store = store or MemoryCheckpointStore()

        def reader_factory(**reader_kwargs):
            return FakeReader(connections, **reader_kwargs)

        consumer = TransactionalBinLogConsumer(
            {},
            server_id=1,
            checkpoint_store=store,
            blocking=False,
            reconnect_delay=0,
            reader_factory=reader_factory,
            **kwargs,
        )
        consumers.append(consumer)
        return consumer

    yield factory
    for consumer in consumers:
        consumer.close()


# ---------------------------------------------------------------------------
# Transaction grouping and delivery
# ---------------------------------------------------------------------------


def test_row_transaction_delivered_complete_at_xid(consumer_factory):
    packets = [
        _gtid_packet(gno=1, log_pos=120),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=150),
        _write_packet(_varchar_value("a"), log_pos=250),
        _xid_packet(xid=9, log_pos=300),
        MysqlPacket(b"\xfe", "utf8"),
    ]
    consumer = consumer_factory(packets)

    batch = consumer.read_transaction()
    assert batch is not None
    assert batch.sequence == 1
    assert batch.gtid is not None and batch.gtid.endswith(":1")
    assert len(batch.events) == 1
    assert isinstance(batch.events[0], WriteRowsEvent)
    # Control events and the GTID/XID boundaries are not part of the batch.
    assert all(not isinstance(ev, (GtidEvent, XidEvent, RotateEvent)) for ev in batch.events)
    assert batch.start_position == BinLogPosition("mysql-bin.000001", 216)
    assert batch.end_position == BinLogPosition("mysql-bin.000001", 300)
    assert not batch.empty

    assert consumer.read_transaction() is None
    # Read position is ahead of the committed position before any ack.
    assert consumer.read_position == BinLogPosition("mysql-bin.000001", 300)
    assert consumer.committed_position is None


def test_multiple_batches_in_flight_with_monotonic_sequences(consumer_factory):
    packets = [
        _gtid_packet(gno=1, log_pos=120),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=150),
        _write_packet(_varchar_value("a"), log_pos=250),
        _xid_packet(xid=9, log_pos=300),
        _gtid_packet(gno=2, log_pos=320),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=350),
        _write_packet(_varchar_value("b"), log_pos=450),
        _xid_packet(xid=10, log_pos=500),
        MysqlPacket(b"\xfe", "utf8"),
    ]
    consumer = consumer_factory(packets)

    first = consumer.read_transaction()
    second = consumer.read_transaction()
    assert [first.sequence, second.sequence] == [1, 2]
    assert consumer.pending_sequences() == [1, 2]
    assert consumer.committed_position is None


def test_empty_transaction_is_deliverable_and_advances_checkpoint(consumer_factory):
    store = MemoryCheckpointStore()
    packets = [
        _query_packet("BEGIN", log_pos=120),
        _query_packet("COMMIT", log_pos=180),
        MysqlPacket(b"\xfe", "utf8"),
    ]
    consumer = consumer_factory(packets, store=store)

    batch = consumer.read_transaction()
    assert batch.empty
    # Empty batch spans BEGIN..COMMIT but contains no user event: its start
    # is the boundary event's own offset, its end is the position after it.
    assert batch.start_position == BinLogPosition("mysql-bin.000001", 134)
    assert batch.end_position == BinLogPosition("mysql-bin.000001", 180)

    consumer.ack(batch.token)
    assert consumer.committed_position == BinLogPosition("mysql-bin.000001", 180)
    assert store.load() == BinLogPosition("mysql-bin.000001", 180)
    assert consumer.pending_sequences() == []


def test_user_event_filter_keeps_boundaries_and_control_events(consumer_factory):
    packets = [
        _gtid_packet(gno=1, log_pos=120),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=150),
        _write_packet(_varchar_value("a"), log_pos=250),
        _xid_packet(xid=9, log_pos=300),
        MysqlPacket(b"\xfe", "utf8"),
    ]
    consumer = consumer_factory(packets, only_events=[UpdateRowsEvent])

    # The write event is filtered out but the transaction still closes.
    batch = consumer.read_transaction()
    assert batch.empty
    assert batch.end_position == BinLogPosition("mysql-bin.000001", 300)
    consumer.ack(batch.token)
    assert consumer.committed_position == BinLogPosition("mysql-bin.000001", 300)


def test_heartbeat_and_rotate_are_never_batched(consumer_factory):
    packets = [
        _heartbeat_packet(),
        _gtid_packet(gno=1, log_pos=120),
        _rotate_packet("mysql-bin.000002", position=4),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=150),
        _write_packet(_varchar_value("a"), log_pos=250),
        _xid_packet(xid=9, log_pos=300),
        MysqlPacket(b"\xfe", "utf8"),
    ]
    consumer = consumer_factory(packets)
    batch = consumer.read_transaction()
    assert len(batch.events) == 1
    assert isinstance(batch.events[0], WriteRowsEvent)
    # The batch starts at the write event in the rotated file, not at the
    # control events, and its coordinates carry the new file name.
    assert batch.start_position.log_file == "mysql-bin.000002"
    assert batch.end_position.log_file == "mysql-bin.000002"


def test_rollback_discards_transaction(consumer_factory):
    packets = [
        _query_packet("BEGIN", log_pos=120),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=150),
        _write_packet(_varchar_value("a"), log_pos=250),
        _query_packet("ROLLBACK", log_pos=300),
        _gtid_packet(gno=2, log_pos=320),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=350),
        _write_packet(_varchar_value("b"), log_pos=450),
        _xid_packet(xid=10, log_pos=500),
        MysqlPacket(b"\xfe", "utf8"),
    ]
    consumer = consumer_factory(packets)

    batch = consumer.read_transaction()
    assert batch.gtid is not None and batch.gtid.endswith(":2")
    assert batch.events[0].rows[0]["values"] == {"UNKNOWN_COL0": "b"}


def test_autocommit_statement_is_its_own_transaction(consumer_factory):
    packets = [
        _query_packet("CREATE TABLE t (id INT)", log_pos=200),
        MysqlPacket(b"\xfe", "utf8"),
    ]
    consumer = consumer_factory(packets)
    batch = consumer.read_transaction()
    assert len(batch.events) == 1
    assert isinstance(batch.events[0], QueryEvent)
    assert batch.events[0].query == "CREATE TABLE t (id INT)"
    assert batch.end_position == BinLogPosition("mysql-bin.000001", 200)


# ---------------------------------------------------------------------------
# Acknowledgment and prefix advancement
# ---------------------------------------------------------------------------


def test_out_of_order_ack_only_advances_continuous_prefix(consumer_factory):
    store = MemoryCheckpointStore()
    packets = [
        _gtid_packet(gno=1, log_pos=120),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=150),
        _write_packet(_varchar_value("a"), log_pos=250),
        _xid_packet(xid=9, log_pos=300),
        _gtid_packet(gno=2, log_pos=320),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=350),
        _write_packet(_varchar_value("b"), log_pos=450),
        _xid_packet(xid=10, log_pos=500),
        _gtid_packet(gno=3, log_pos=520),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=550),
        _write_packet(_varchar_value("c"), log_pos=650),
        _xid_packet(xid=11, log_pos=700),
        MysqlPacket(b"\xfe", "utf8"),
    ]
    consumer = consumer_factory(packets, store=store)
    first = consumer.read_transaction()
    second = consumer.read_transaction()
    third = consumer.read_transaction()

    # Acknowledge out of order: 2 then 3. The checkpoint must not move.
    consumer.ack(second.token)
    assert consumer.committed_position is None
    assert store.load() is None
    consumer.ack(third.token.as_tuple())
    assert consumer.committed_position is None
    assert consumer.pending_sequences() == [1, 2, 3]

    # Closing the gap advances straight through the whole prefix in one save.
    saves = []
    original_save = store.save

    def recording_save(position):
        saves.append(position)
        return original_save(position)

    store.save = recording_save
    consumer.ack(first.token)
    assert saves == [BinLogPosition("mysql-bin.000001", 700)]
    assert consumer.committed_position == BinLogPosition("mysql-bin.000001", 700)
    assert consumer.pending_sequences() == []


def test_checkpoint_write_failure_does_not_advance(consumer_factory):
    store = MemoryCheckpointStore()
    store.save_failures_remaining = 1
    packets = [
        _gtid_packet(gno=1, log_pos=120),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=150),
        _write_packet(_varchar_value("a"), log_pos=250),
        _xid_packet(xid=9, log_pos=300),
        MysqlPacket(b"\xfe", "utf8"),
    ]
    consumer = consumer_factory(packets, store=store)
    batch = consumer.read_transaction()

    with pytest.raises(CheckpointError):
        consumer.ack(batch.token)
    assert consumer.committed_position is None
    assert store.load() is None
    # The ack is still pending; retrying after recovery succeeds.
    assert consumer.pending_sequences() == [1]
    consumer.ack(batch.token)
    assert consumer.committed_position == BinLogPosition("mysql-bin.000001", 300)
    assert consumer.pending_sequences() == []


def test_unknown_and_wrong_attempt_tokens_are_rejected(consumer_factory):
    packets = [
        _gtid_packet(gno=1, log_pos=120),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=150),
        _write_packet(_varchar_value("a"), log_pos=250),
        _xid_packet(xid=9, log_pos=300),
        MysqlPacket(b"\xfe", "utf8"),
    ]
    consumer = consumer_factory(packets)
    batch = consumer.read_transaction()

    with pytest.raises(StaleDeliveryTokenError):
        consumer.ack((999, 0))
    with pytest.raises(StaleDeliveryTokenError):
        consumer.ack((batch.sequence, batch.token.attempt + 1))
    # Rejected acks do not move state.
    assert consumer.committed_position is None
    assert consumer.pending_sequences() == [1]


# ---------------------------------------------------------------------------
# Reconnect semantics
# ---------------------------------------------------------------------------


def _two_connections():
    first = [
        _gtid_packet(gno=1, log_pos=120),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=150),
        _write_packet(_varchar_value("a"), log_pos=250),
        _xid_packet(xid=9, log_pos=300),
        MysqlPacket(b"\xfe", "utf8"),
    ]
    second = [
        _gtid_packet(gno=1, log_pos=120),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=150),
        _write_packet(_varchar_value("a"), log_pos=250),
        _xid_packet(xid=9, log_pos=300),
        MysqlPacket(b"\xfe", "utf8"),
    ]
    return first, second


def test_reopen_uses_delivery_floor_and_invalidates_old_tokens(consumer_factory):
    first, second = _two_connections()
    consumer = consumer_factory([first, second])

    batch = consumer.read_transaction()
    assert consumer.attempt == 0
    assert consumer.read_transaction() is None  # EOF triggers the reopen

    assert consumer.attempt == 1
    # Nothing was acknowledged: reopen at the original transaction start,
    # never at the raw read position (300).
    reader = consumer._reader
    assert reader.reopen_positions == [("mysql-bin.000001", 216)]

    # The old token can no longer be acknowledged.
    with pytest.raises(StaleDeliveryTokenError):
        consumer.ack(batch.token)

    redelivered = consumer.read_transaction()
    assert redelivered.sequence == 2
    assert redelivered.token.attempt == 1
    assert redelivered.events[0].rows[0]["values"] == {"UNKNOWN_COL0": "a"}
    consumer.ack(redelivered.token)
    assert consumer.committed_position == BinLogPosition("mysql-bin.000001", 300)


def test_reopen_after_ack_resumes_from_committed_prefix(consumer_factory):
    first = [
        _gtid_packet(gno=1, log_pos=120),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=150),
        _write_packet(_varchar_value("a"), log_pos=250),
        _xid_packet(xid=9, log_pos=300),
        MysqlPacket(b"\xfe", "utf8"),
    ]
    second = [
        _gtid_packet(gno=2, log_pos=320),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=350),
        _write_packet(_varchar_value("b"), log_pos=450),
        _xid_packet(xid=10, log_pos=500),
        MysqlPacket(b"\xfe", "utf8"),
    ]
    consumer = consumer_factory([first, second])

    first_batch = consumer.read_transaction()
    consumer.ack(first_batch.token)
    assert consumer.committed_position == BinLogPosition("mysql-bin.000001", 300)

    assert consumer.read_transaction() is None  # EOF -> reopen
    assert consumer.attempt == 1
    reader = consumer._reader
    assert reader.reopen_positions == [("mysql-bin.000001", 300)]

    next_batch = consumer.read_transaction()
    assert next_batch.gtid.endswith(":2")


def test_connection_error_retries_then_redelivers(consumer_factory):
    tx = [
        _gtid_packet(gno=1, log_pos=120),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=150),
        _write_packet(_varchar_value("a"), log_pos=250),
        _xid_packet(xid=9, log_pos=300),
    ]
    # Connection 1 delivers the transaction, then the connection dies.
    # Connection 2 dies before any event; connection 3 replays everything.
    first = tx + [pymysql.err.OperationalError(2013, "Lost connection")]
    second = [pymysql.err.OperationalError(2013, "Lost connection")]
    third = tx + [MysqlPacket(b"\xfe", "utf8")]
    consumer = consumer_factory([first, second, third])

    batch = consumer.read_transaction()
    assert batch.token.attempt == 0

    # The next read loses the connection twice; the consumer retries via
    # reopens (new attempt, stale tokens, floor-based resume) and then
    # redelivers.
    redelivered = consumer.read_transaction()
    assert consumer.attempt == 2
    assert redelivered.token.attempt == 2
    assert redelivered.sequence == 2
    with pytest.raises(StaleDeliveryTokenError):
        consumer.ack(batch.token)
    consumer.ack(redelivered.token)
    assert consumer.committed_position == BinLogPosition("mysql-bin.000001", 300)
    # Every reopen resumed at the delivery floor (the transaction's start),
    # never at the raw read position (300).
    assert consumer._reader.reopen_positions == [
        ("mysql-bin.000001", 216),
        ("mysql-bin.000001", 216),
    ]


def test_retry_budget_is_enforced(consumer_factory):
    packets = [pymysql.err.OperationalError(2013, "Lost connection")]
    consumer = consumer_factory([packets], reconnect_retries=2)
    with pytest.raises(pymysql.err.OperationalError):
        consumer.read_transaction()


def test_non_retryable_error_propagates(consumer_factory):
    packets = [pymysql.err.OperationalError(1045, "Access denied")]
    consumer = consumer_factory([packets])
    with pytest.raises(pymysql.err.OperationalError):
        consumer.read_transaction()


# ---------------------------------------------------------------------------
# Rotation, lazy decoding, and position semantics
# ---------------------------------------------------------------------------


def test_rotation_coordinates_cross_files(consumer_factory):
    packets = [
        _gtid_packet(gno=1, log_pos=900),
        _rotate_packet("mysql-bin.000002", position=4),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=150),
        _write_packet(_varchar_value("a"), log_pos=250),
        _xid_packet(xid=9, log_pos=300),
        MysqlPacket(b"\xfe", "utf8"),
    ]
    consumer = consumer_factory(packets)
    batch = consumer.read_transaction()
    assert batch.start_position.log_file == "mysql-bin.000002"
    assert batch.end_position == BinLogPosition("mysql-bin.000002", 300)
    consumer.ack(batch.token)
    assert consumer.committed_position == BinLogPosition("mysql-bin.000002", 300)


def test_lazy_row_decoding_survives_later_reads(consumer_factory):
    packets = [
        _gtid_packet(gno=1, log_pos=120),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=150),
        _write_packet(_varchar_value("first"), log_pos=250),
        _xid_packet(xid=9, log_pos=300),
        _gtid_packet(gno=2, log_pos=320),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=350),
        _write_packet(_varchar_value("second"), log_pos=450),
        _xid_packet(xid=10, log_pos=500),
        MysqlPacket(b"\xfe", "utf8"),
    ]
    consumer = consumer_factory(packets)
    first = consumer.read_transaction()
    second = consumer.read_transaction()
    # Decode only after more packets have flowed through the stream.
    assert first.events[0].rows[0]["values"] == {"UNKNOWN_COL0": "first"}
    assert second.events[0].rows[0]["values"] == {"UNKNOWN_COL0": "second"}


def test_lazy_row_decoding_with_binlog_checksum():
    # A checksum footer must be excluded from the frozen row payload; using
    # body-only event size to compute the boundary would otherwise freeze zero
    # (or the wrong number of) bytes and fail to decode the row.
    table_map = {}
    ctl = _CtlConnection()
    table_map_packet = _table_map_packet(
        "orders", FIELD_TYPE.VARCHAR, log_pos=150
    )
    BinLogPacketWrapper(
        table_map_packet,
        table_map,
        ctl,
        (5, 6, 0),
        True,
        _ALL_EVENTS,
        None,
        None,
        None,
        None,
        False,
        False,
        False,
        frozenset(),
        False,
    )
    wrapped = BinLogPacketWrapper(
        _write_packet_with_checksum(_varchar_value("b"), log_pos=250),
        table_map,
        ctl,
        (5, 6, 0),
        True,
        _ALL_EVENTS,
        None,
        None,
        None,
        None,
        False,
        False,
        False,
        frozenset(),
        False,
    )
    event = wrapped.event
    assert isinstance(event, WriteRowsEvent)
    # The frozen payload is exactly the per-row null bitmap plus the value;
    # the 4-byte checksum is not part of it.
    assert event.packet.payload_size == 3
    assert event.rows == [
        {"values": {"UNKNOWN_COL0": "b"}, "none_sources": {}}
    ]
    assert event._remaining_payload() == 0


def test_read_position_is_not_committed_position(consumer_factory):
    packets = [
        _gtid_packet(gno=1, log_pos=120),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=150),
        _write_packet(_varchar_value("a"), log_pos=250),
        _xid_packet(xid=9, log_pos=300),
        MysqlPacket(b"\xfe", "utf8"),
    ]
    consumer = consumer_factory(packets)
    consumer.read_transaction()
    assert consumer.read_position == BinLogPosition("mysql-bin.000001", 300)
    assert consumer.committed_position is None


# ---------------------------------------------------------------------------
# Checkpoint store and real reader integration
# ---------------------------------------------------------------------------


def test_file_checkpoint_store_roundtrip_and_atomicity(tmp_path):
    path = tmp_path / "checkpoint.json"
    store = FileCheckpointStore(str(path))
    assert store.load() is None
    store.save(BinLogPosition("mysql-bin.000007", 4242))
    reloaded = FileCheckpointStore(str(path)).load()
    assert reloaded == BinLogPosition("mysql-bin.000007", 4242)
    # No temp files are left behind.
    assert os.listdir(str(tmp_path)) == ["checkpoint.json"]


def test_corrupt_checkpoint_file_raises_checkpoint_error(tmp_path):
    path = tmp_path / "checkpoint.json"
    path.write_text("{not json")
    with pytest.raises(CheckpointError):
        FileCheckpointStore(str(path)).load()


def test_real_reader_hooks_reposition_and_fence_on_reopen():
    """Drive the real BinLogStreamReader hooks with mocked connections."""

    first = [
        _gtid_packet(gno=1, log_pos=120),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=150),
        _write_packet(_varchar_value("a"), log_pos=250),
        _xid_packet(xid=9, log_pos=300),
        MysqlPacket(b"\xfe", "utf8"),
    ]
    second = [
        _gtid_packet(gno=1, log_pos=120),
        _table_map_packet("orders", FIELD_TYPE.VARCHAR, log_pos=150),
        _write_packet(_varchar_value("a"), log_pos=250),
        _xid_packet(xid=9, log_pos=300),
        MysqlPacket(b"\xfe", "utf8"),
    ]

    stream = BinLogStreamReader(
        {},
        server_id=1,
        log_file="mysql-bin.000001",
        log_pos=4,
        blocking=False,
        enable_logging=False,
    )

    connection_count = {"n": 0}

    def connect_factory(**kwargs):
        conn = Mock()
        # SHOW GLOBAL VARIABLES / SET queries issued while opening a stream.
        cursor = Mock()
        cursor.fetchone.return_value = None
        conn.cursor.return_value = cursor
        # The first connection is injected below; every later connection is a
        # reopen and must replay the second script.
        conn._read_packet.side_effect = (
            list(second) if connection_count["n"] > 0 else list(first)
        )
        connection_count["n"] += 1
        return conn

    stream.pymysql_wrapper = connect_factory
    stream._stream_connection = connect_factory()
    stream._ctl_connection = Mock(charset="utf8")
    # The real constructor installs the reader's bound vendor probe as
    # ``_get_dbms``; replacing the control connection with a Mock loses that
    # binding.  Reinstall it and pre-seed ``dbms`` so the probe returns without
    # calling the Mock's unconfigured get_server_info().
    stream._ctl_connection._get_dbms = stream._BinLogStreamReader__get_dbms
    stream.dbms = "mysql"
    stream._BinLogStreamReader__connected_stream = True
    stream._BinLogStreamReader__connected_ctl = True
    # The injected first connection stands in for a successful
    # __connect_to_stream, which is what flips this flag; the reopen path only
    # applies the resume callback once it is set.
    stream._BinLogStreamReader__ever_connected = True

    resumes = []
    reconnects = []

    def resume_callback():
        position = ("mysql-bin.000001", 216)
        resumes.append(position)
        return position

    stream._BinLogStreamReader__resume_position_callback = resume_callback
    stream._BinLogStreamReader__on_reconnect_callback = lambda: reconnects.append(1)

    # Drain the first connection; EOF closes the stream.
    events = list(iter(stream.fetchone, None))
    assert any(isinstance(event, WriteRowsEvent) for event in events)

    # The next fetchone reopens: the resume position is applied and the
    # reconnect fence callback fires exactly once.
    redelivered = list(iter(stream.fetchone, None))
    assert resumes == [("mysql-bin.000001", 216)]
    assert reconnects == [1]
    assert stream.log_file == "mysql-bin.000001"
    assert stream.log_pos == 300
    assert any(isinstance(event, WriteRowsEvent) for event in redelivered)
    stream.close()
