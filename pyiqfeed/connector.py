# coding=utf-8
"""
Connect and disconnect from IQFeed using RAII.

We want to ensure that the various XXXConn classes can connect and disconnect
in a robust way and disconnection happens even if an exception is thrown or
some such. The best way to do this is using the with idiom. We may want to use
multiple XXXConn classes and multiple nested with statements are annoying.

We don't actually care about construction and destruction of the classes, we
just want to ensure the reading threads are stopped and the sockets to
IQFEed.exe are cleaned up.

Given this we can solve this using this Connector class. We create our Conn
classes and put them in a list and do with Connect[list of Conn classes]:

"""

from typing import List
from .conn import FeedConn


class ConnConnector:
    """
    Allows calling connect and disconnect on a list of XXXConn objects using a
    context manager, ie using with syntax.

    So if you have a QuoteConn object called qc and and AdminConn object
    called ac, instead of calling the connect() method on qc and ac and then
    disconnect() at the end you can simply do

    with ConnConnector([qc, ac]):
        do_something()
        do_something_else()

    and when you leave the scope disconnect is called automatically even if an
    exception is thrown.

    """

    def __init__(self, conn_list: List[FeedConn]):
        self._conn_list = conn_list

    def __enter__(self):
        for conn in self._conn_list:
            conn.connect()
        return self._conn_list

    def __exit__(self, exc_type, exc_value, traceback):
        for conn in self._conn_list:
            conn.disconnect()
