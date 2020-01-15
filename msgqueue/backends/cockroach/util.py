from msgqueue.backends.queue import Message, Agent


def _parse(result):
    if result is None:
        return None

    return Message(*result)


def _parse_agent(result):
    if result is None:
        return None

    return Agent(*result)
