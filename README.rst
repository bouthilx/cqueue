CQueue
======

Message Queue Primitives


.. code-block::

    from cqueue import make_message_client, make_message_client

    uri = 'cockroach://192.168.0.10:8123'

    # -- Start a broker in the background
    broker = make_message_client(uri)
    broker.init()
    broker.start()

    # -- connect to the broker and pull/push  messages
    client = make_message_client(uri)
    
    client.push('queue_name', {'my_message': 123})

    message = client.pop('queue_name').message