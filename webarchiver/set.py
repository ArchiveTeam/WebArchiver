"""Customized set."""
import threading


class LockedSet(set):
    """A set with a lock included.

    Attributes:
        lock (:obj:`_thread.lock`): The lock to use for the set.
    """

    def __init__(self, *args, **kwargs):
        """Inits the set with lock.

        *args: Argument list for the set.
        **kwargs: Keyword arguments for the set.
        """
        super().__init__(*args, **kwargs)
        self.lock = threading.Lock()

