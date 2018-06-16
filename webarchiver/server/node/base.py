"""Base node in the network."""


class BaseNode:
    """The base of a node in the network.

    Attributes:
        confirmed (bool): Whether the node is confirmed.
        pong (bool): Whether the node send a pong back after a ping.
    """

    def __init__(self):
        """Inits the base node."""
        self.confirmed = False
        self.pong = None

__all__ = ()

