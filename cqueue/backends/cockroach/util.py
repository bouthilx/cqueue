from cqueue.backends.queue import Message, Agent


def _parse(result):
    if result is None:
        return None

    return Message(
        result[0],
        result[1],
        result[2],
        result[3],
        result[4],
        result[5],
        result[6],
        result[7],
        result[8],
    )


def _parse_agent(result):
    if result is None:
        return None

    return Agent(
        result[0],
        result[1],
        result[2],
        result[3],
        result[4],
        result[5],
        result[6]
    )
