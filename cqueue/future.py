import time


def check_reply_fun(client, result_queue, message_id):
    def check_ready():
        return client.get_reply(result_queue, message_id)
    return check_ready


class Future:
    def __init__(self, client, result_queue, message_id):
        self.check = check_reply_fun(client, result_queue, message_id)
        self.result = None

    def ready(self):
        self.result = self.check()

        if self.result is not None:
            self.result = self.result.message[-1]

        return self.result

    def wait(self):
        while self.result is None:
            time.sleep(0.01)
            self.ready()
