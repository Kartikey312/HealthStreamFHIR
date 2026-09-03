"""
Long-lived resources handed to every node executor for a run, so executors
don't open/close a connection per node call.
"""
class ExecutionContext:
    def __init__(self, producer):
        self.producer = producer
