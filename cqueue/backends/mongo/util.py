from cqueue.backends.queue import Message, Agent


def _parse_agent(agent):
    agent['uid'] = agent['_id']
    agent.pop('_id')
    return Agent(**agent)


def _parse(result):
    if result is None:
        return None

    return Message(
        result['_id'],
        result['time'],
        result['mtype'],
        result['read'],
        result['read_time'],
        result['actioned'],
        result['actioned_time'],
        result['replying_to'],
        result['message'],
    )
