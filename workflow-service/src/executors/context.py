"""
Long-lived resources handed to every node executor for a run, so executors
don't open/close a connection per node call. outputs is the run's own
{node_id: output} map, filled in as nodes finish, so a node can read any
earlier node's output ("$node.<id>...") and not only the previous one's.
"""
class ExecutionContext:
    def __init__(self, producer, outputs=None):
        self.producer = producer
        self.outputs = outputs if outputs is not None else {}
