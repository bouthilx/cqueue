CQueue
======

Message Queue Primitives.
CQueue allows you to start the server in the same script as your worker,
removing the need for you to deploy a server before hand.

The cockroachdb backend even allows you to create a highly resilient queue
that can survive multiple server failures.


.. code-block::

    from cqueue import make_message_client, make_message_client

    uri = 'cockroach://192.168.0.10:8123'

    # -- Start a broker in the background
    broker = make_message_broker(uri)

    # start the server
    broker.start()

    # -- connect to the broker and pull/push  messages
    client = make_message_client(uri)

    client.push('queue_name', {'my_message': 123})

    message = client.pop('queue_name').message

Dependencies
~~~~~~~~~~~~


For mongodb:

.. code-block::

    sudo apt-get install mongodb-server
