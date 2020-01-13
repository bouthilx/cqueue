CQueue
======

Message Queue Primitives.
CQueue allows you to start the server in the same script as your worker,
removing the need for you to deploy a server before hand.

The cockroachdb backend even allows you to create a highly resilient queue
that can survive multiple server failures.


Install
~~~~~~~

.. code-block:: bash

    pip install git+git://github.com/Delaunay/cqueue


Simple Workflow
~~~~~~~~~~~~~~~

.. code-block:: python

    from cqueue import new_client

    client = new_client(
        uri='cockroach://192.168.0.10:8123',
        namespace='task',       # Queue Namespace
        log_capture=True,       # Capture Worker Log and Stdout
        timeout=60)             # Client Time Out

    # remove a message from the `work` queue
    message = client.pop('work')

    result = processing(message)

    # Message has finished being processed
    client.mark_actioned(message)

    # put a message in the `result` queue
    client.push('result', result)


Start servers at will

.. code-block:: python

    from cqueue import new_server

    # start a new cockroach server for clients
    # to push and pull their messages
    server = new_server(uri='cockroach://192.168.0.10:8123')
    server.start()

Detect errors & Inspect logs

.. code-block:: python

    from cqueue import new_monitor

    monitor = new_monitor(uri='cockroach://192.168.0.10:8123')

    # get messages that were assigned to a currently dead worker & that have not finished
    dead_messages = monitor.fetch_lost_messages('task')

    # Requeue all messages that did not finish
    monitor.requeue_messages()

    # get unresponsive worker entries
    dead_workers = monitor.fetch_dead_agents('task')

    # Get logs of the dead worker
    log = monitor.get_log('task', dead_workers[0])

    print(log)

    # Get all messages (read & unread) from the `task` namespace and the `work` queue
    # For analysis
    messages = monitor.get_all_messages('task', 'work')
    for msg in messages:
        print(
        m.read,             # Was the message read
        m.read_time,        # when was the message read
        m.actioned,         # Was the message actioned
        n.actioned_time,    # when was the message actioned
        m.message           # User provided data (json)
    )


Dependencies
~~~~~~~~~~~~

For mongodb:

.. code-block::

    sudo apt-get install mongodb-server
