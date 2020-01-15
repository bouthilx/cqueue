from msgqueue.backends.queue import Message, Agent


def _parse_agent(agent):
    agent['uid'] = agent['_id']
    agent.pop('_id')
    return Agent(**agent)


def _parse(result):
    if result is None:
        return None

    result['uid'] = result['_id']
    result.pop('_id')

    return Message(**result)
