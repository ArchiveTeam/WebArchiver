"""Nodes from a stager server."""
from webarchiver.server.node.base import BaseNode


class StagerNodeBase(BaseNode):
    """A node in the network of a stager server."""
    pass


class StagerNodeStager(StagerNodeBase):
    """A stager server node in the network of a stager server."""
    pass


class StagerNodeCrawler(StagerNodeBase):
    """A crawler server node in the network of a stager server."""
    pass

