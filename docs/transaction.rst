############################
TransactionalBinLogConsumer
############################

The transactional consumer groups the raw event stream into complete
transactions and adds an acknowledgment based checkpoint on top of
:class:`~pymysqlreplication.BinLogStreamReader`.

Delivery model
==============

* :meth:`~pymysqlreplication.transaction.TransactionalBinLogConsumer.read_transaction`
  returns a complete
  :class:`~pymysqlreplication.transaction.TransactionBatch` once the commit
  boundary of a transaction is observed. Several batches can be in flight at
  the same time.
* A batch is acknowledged with
  :meth:`~pymysqlreplication.transaction.TransactionalBinLogConsumer.ack`.
  Acknowledgments may arrive in any order. The durable checkpoint only
  advances over the *continuous prefix* of acknowledged transactions; a gap
  keeps every later transaction from being considered committed.
* On connection failure or when the stream is reopened, reading resumes from
  that committed checkpoint (or, before the first acknowledgment, from the
  original start position). Every reopen starts a new delivery attempt and
  tokens minted by previous attempts raise
  :class:`~pymysqlreplication.transaction.StaleDeliveryTokenError` when used.
* The raw reader position is always kept separate from the committed
  consumption position; one is never reported as the other.

Delivery is **at-least-once**: transactions past the last durable checkpoint
may be redelivered after a reconnect. Whether the combination with a
downstream system is exactly-once depends entirely on the downstream side
(idempotent writes, a transactional outbox, ...). This library does not write
to any business table and cannot promise end-to-end exactly-once delivery to
arbitrary sinks.

Checkpoint stores
=================

A :class:`~pymysqlreplication.transaction.CheckpointStore` persists the
committed position. :class:`~pymysqlreplication.transaction.MemoryCheckpointStore`
keeps it in process and :class:`~pymysqlreplication.transaction.FileCheckpointStore`
writes it atomically (temp file + rename + fsync). A failed checkpoint write
raises :class:`~pymysqlreplication.transaction.CheckpointError` and never moves
the committed position forward.

Example
=======

.. code-block:: python

    from pymysqlreplication import (
        TransactionalBinLogConsumer,
        FileCheckpointStore,
    )

    consumer = TransactionalBinLogConsumer(
        {"host": "127.0.0.1", "port": 3306, "user": "u", "passwd": "p"},
        server_id=100,
        checkpoint_store=FileCheckpointStore("checkpoint.json"),
    )

    while True:
        batch = consumer.read_transaction()
        if batch is None:
            continue
        for event in batch.events:
            handle(event)  # idempotent downstream work
        consumer.ack(batch.token)


.. automodule:: pymysqlreplication.transaction
    :members:
