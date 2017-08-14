# coding=utf-8

"""
Connections to IQFeed.exe to get different types of data.

This module contains various Conn classes called XXXConn each of
which connects to IQFeed and helps to return market data from it.

Some of the XXXConn classes (like HistoryConn), which provide data
that should be available when requested, provide the data
requested as the return value of the function that requests the
data. Other XXXConn classes (like QuoteConn) which provide streaming
data, require you to implement a class that derives from one of the
Listeners in listeners.py and provide the data by calling lookbacks
in those classes as it comes in.

All XXXConn classes send status messages to listener classes. While
a listener class is not strictly necessary when using something like
HistoryConn, if things aren't working right, you may want to use a
listener to make sure you aren't getting a message telling you why
that you are ignoring.

Data that you are likely to use for analysis is returned as numpy
structured arrays. Other data is normally returned as a namedtuple
specific to that message time.

FeedConn is the base class for all the XXXConn classes.

QuoteConn provides real-time tick-data and real-time news headlines.

AdminConn provides status messages about the status of the Feed etc.

HistoryConn provides historical data.

LookupConn lets you lookup symbols and option and futures chains.

TableConn provides reference data like condition codes and exchanges.

BarConn lets you request real-time interval bars instead of calculating them
yourself from the tick-data (from QuoteConn).

NewsConn lets you get news-headlines in bulk (as opposed to real-time news
which you can get from QuoteConn) and full news stories from the story id.

See http://www.iqfeed.net/dev/main.cfm for more information.
"""

import datetime
import itertools
import select
import socket
import threading
import time

from collections import deque, namedtuple
from typing import Sequence, List
# noinspection PyPep8Naming
import xml.etree.ElementTree as etree

import numpy as np
from .exceptions import NoDataError, UnexpectedField, UnexpectedMessage
from .exceptions import UnexpectedProtocol, UnauthorizedError
from . import field_readers as fr


class FeedConn:
    """
    FeedConn is the base class for other XXXConn classes

    It handles connecting, disconnecting, sending messages to IQFeed,
    reading responses from IQFeed, feed status messages etc.

    """

    protocol = "5.2"
    host = "127.0.0.1"
    quote_port = 5009
    lookup_port = 9100
    depth_port = 9200
    admin_port = 9300
    deriv_port = 9400

    port = quote_port

    ConnStatsMsg = namedtuple('ConnStatsMsg', (
        'server_ip', 'server_port', 'max_sym', 'num_sym', 'num_clients',
        'secs_since_update', 'num_recon', 'num_fail_recon', 'conn_tm', 'mkt_tm',
        'status', 'feed_version', 'login', 'kbs_recv', 'kbps_recv',
        'avg_kbps_recv',
        'kbs_sent', 'kbps_sent', 'avg_kbps_sent'))

    TimeStampMsg = namedtuple("TimeStampMsg", ("date", "time"))

    def __init__(self, name: str, host: str, port: int):
        self._host = host
        self._port = port
        self._name = name

        self._stop = threading.Event()
        self._start_lock = threading.Lock()
        self._connected = False
        self._reconnect_failed = False
        self._pf_dict = {}
        self._sm_dict = {}
        self._listeners = []
        self._buf_lock = threading.RLock()
        self._send_lock = threading.RLock()
        self._recv_buf = ""
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._read_thread = threading.Thread(group=None, target=self,
                                             name="%s-reader" % self._name,
                                             args=(), kwargs={}, daemon=None)
        self._set_message_mappings()

    def connect(self) -> None:
        """
        Connect to the appropriate socket and start the reading thread.

        You must call this before you start using an XXXConn class. If
        this thread is not running, no callbacks will be called, no data
        will be returned by functions which return data immediately.

        """
        self._sock.connect((self._host, self._port))
        self._set_protocol(FeedConn.protocol)
        self._set_client_name(self.name())
        self._send_connect_message()
        self.start_runner()

    def start_runner(self) -> None:
        """Called to start the reading thread."""
        with self._start_lock:
            self._stop.clear()
            if not self.reader_running():
                self._read_thread.start()

    def disconnect(self) -> None:
        """
        Stop the reading thread and disconnect from the socket to IQFeed.exe

        Call this to ensure sockets are closed and we exit cleanly.

        """
        self.stop_runner()
        if self._sock:
            self._sock.shutdown(socket.SHUT_RDWR)
            self._sock.close()
            self._sock = None

    def stop_runner(self) -> None:
        """Called to stop the reading and message processing thread."""
        with self._start_lock:
            self._stop.set()
            if self.reader_running():
                self._read_thread.join(30)

    def reader_running(self) -> bool:
        """
        True if the reader thread is running.

        If you don't get updates for a while you "may" want to query this
        function.  Mainly useful for debugging during development of the
        library.  If the reader thread is crashing, there is likely a bug
        in the library or something else is going very wrong.
        """
        return self._read_thread.is_alive()

    def connected(self) -> bool:
        """
        Returns true if IQClient.exe is connected to DTN's servers.

        It may take a few seconds after connecting to IQFeed for IQFeed to tell
        us it is connected to DTN's servers. During these few seconds, this
        function will return False even though it's not actually a problem.

        NOTE: It's not telling you if you are connected to IQFeed.exe. It's
        telling you if IQFeed.exe is connected to DTN's servers.
        """
        return self._connected

    def name(self) -> str:
        """Return whatever you named this conn class in the constructor"""
        return self._name

    def _send_cmd(self, cmd: str) -> None:
        with self._send_lock:
            # noinspection PyArgumentEqualDefault
            self._sock.sendall(cmd.encode(encoding='latin-1', errors='strict'))

    def reconnect_failed(self) -> bool:
        """
        Returns true if IQClient.exe failed to reconnect to DTN's servers.

        It can and does happen that IQClient.exe drops a connection to DTN's
        servers and then reconnects. This is not a big problem. But if a
        reconnect fails this means there is a big problem and you should
        probably pause trading and figure out what's going on with your
        network.
        """
        return self._reconnect_failed

    def __call__(self):
        """The reader thread runs this in a loop."""
        while not self._stop.is_set():
            if self._read_messages():
                self._process_messages()

    def _read_messages(self) -> bool:
        """Read raw text sent by IQFeed on socket"""
        ready_list = select.select([self._sock], [], [self._sock], 5)
        if ready_list[2]:
            raise RuntimeError(
                    "Error condition on socket connection to IQFeed: %s,"
                    "" % self.name())
        if ready_list[0]:
            data_recvd = self._sock.recv(1024).decode('latin-1')
            with self._buf_lock:
                self._recv_buf += data_recvd
                return True
        return False

    def _next_message(self) -> str:
        """Next complete message from buffer of delimited messages"""
        with self._buf_lock:
            next_delim = self._recv_buf.find('\n')
            if next_delim != -1:
                message = self._recv_buf[:next_delim].strip()
                self._recv_buf = self._recv_buf[(next_delim + 1):]
                return message
            else:
                return ""

    def _set_message_mappings(self) -> None:
        """Creates map of message names to processing functions."""
        self._pf_dict['E'] = self._process_error
        self._pf_dict['T'] = self._process_timestamp
        self._pf_dict['S'] = self._process_system_message

        self._sm_dict["SERVER DISCONNECTED"] = \
            self._process_server_disconnected
        self._sm_dict["SERVER CONNECTED"] = self._process_server_connected
        self._sm_dict[
            "SERVER RECONNECT FAILED"] = self._process_reconnect_failed
        self._sm_dict["CURRENT PROTOCOL"] = self._process_current_protocol
        self._sm_dict["STATS"] = self._process_conn_stats

    def _process_messages(self) -> None:
        """Process the next complete message waiting to be processed"""
        message = self._next_message()
        while "" != message:
            fields = message.split(',')
            handle_func = self._processing_function(fields)
            handle_func(fields)
            message = self._next_message()

    def _processing_function(self, fields):
        """Returns the processing function for this specific message."""
        pf = self._pf_dict.get(fields[0][0])
        if pf is not None:
            return pf
        else:
            return self._process_unregistered_message

    def _process_unregistered_message(self, fields: Sequence[str]) -> None:
        """Called if we get a message we don't expect.

        Appropriate action here is probably to crash.

        """
        err_msg = ("Unexpected message received by %s: %s" % (
            self.name(), ",".join(fields)))
        raise UnexpectedMessage(err_msg)

    def _process_system_message(self, fields: Sequence[str]) -> None:
        """
        Called when the next message is a system message.

        System messages are messages about the state of the data delivery
        system, including IQConnect.exe, DTN servers and connectivity.

        """
        assert len(fields) > 1
        assert fields[0] == "S"
        processing_func = self._system_processing_function(fields)
        processing_func(fields)

    def _system_processing_function(self, fields):
        """Returns the appropriate system message handling function."""
        assert len(fields) > 1
        assert fields[0] == "S"
        spf = self._sm_dict.get(fields[1])
        if spf is not None:
            return spf
        else:
            return self._process_unregistered_system_message

    def _process_unregistered_system_message(self,
                                             fields: Sequence[str]) -> None:
        """
        Called if we get a system message we don't know how to handle.

        Appropriate action here is probably to crash.

        """
        err_msg = ("Unexpected message received by %s: %s" % (
            self.name(), ",".join(fields)))
        raise UnexpectedMessage(err_msg)

    def _process_current_protocol(self, fields: Sequence[str]) -> None:
        """
        Process the Current Protocol Message

        The first message we send IQFeed.exe upon connecting is the
        set protocol message. If we get this message and the protocol
        IQFeed tells us it's using does not match the expected protocol
        then the we really need to shutdown, fix the version mismatch by
        upgrading/downgrading IQFeed.exe and this library so they match.

        """
        assert len(fields) > 2
        assert fields[0] == "S"
        assert fields[1] == "CURRENT PROTOCOL"
        protocol = fields[2]
        if protocol != FeedConn.protocol:
            err_msg = ("Desired Protocol %s, Server Says Protocol %s in %s" % (
                FeedConn.protocol, protocol, self.name()))
            raise UnexpectedProtocol(err_msg)

    def _process_server_disconnected(self, fields: Sequence[str]) -> None:
        """Called when IQFeed.exe disconnects from DTN's servers."""
        assert len(fields) > 1
        assert fields[0] == "S"
        assert fields[1] == "SERVER DISCONNECTED"
        self._connected = False
        for listener in self._listeners:
            listener.feed_is_stale()

    def _process_server_connected(self, fields: Sequence[str]) -> None:
        """Called when IQFeed.exe connects or re-connects to DTN's servers."""
        assert len(fields) > 1
        assert fields[0] == "S"
        assert fields[1] == "SERVER CONNECTED"
        self._connected = True
        for listener in self._listeners:
            listener.feed_is_fresh()

    def _process_reconnect_failed(self, fields: Sequence[str]) -> None:
        """Called if IQFeed.exe cannot reconnect to DTN's servers."""
        assert len(fields) > 1
        assert fields[0] == "S"
        assert fields[1] == "SERVER RECONNECT FAILED"
        self._reconnect_failed = True
        self._connected = False
        for listener in self._listeners:
            listener.feed_is_stale()
            listener.feed_has_error()

    def _process_conn_stats(self, fields: Sequence[str]) -> None:
        """Parse and send ConnStatsMsg to listener."""
        assert len(fields) > 20
        assert fields[0] == "S"
        assert fields[1] == "STATS"
        conn_stats = FeedConn.ConnStatsMsg(
            server_ip=fields[2],
            server_port=fr.read_int(fields[3]),
            max_sym=fr.read_int(fields[4]),
            num_sym=fr.read_int(fields[5]),
            num_clients=fr.read_int(fields[6]),
            secs_since_update=fr.read_int(fields[7]),
            num_recon=fr.read_int(fields[8]),
            num_fail_recon=fr.read_int(fields[9]),
            conn_tm=(time.strptime(fields[10], "%b %d %I:%M%p")
                     if fields[10] != "" else None),
            mkt_tm=(time.strptime(fields[11], "%b %d %I:%M%p")
                    if self.connected() else None),
            status=(fields[12] == "Connected"),
            feed_version=fields[13],
            login=fields[14],
            kbs_recv=fr.read_float(fields[15]),
            kbps_recv=fr.read_float(fields[16]),
            avg_kbps_recv=fr.read_float(fields[17]),
            kbs_sent=fr.read_float(fields[18]),
            kbps_sent=fr.read_float(fields[19]),
            avg_kbps_sent=fr.read_float(fields[20]))
        for listener in self._listeners:
            listener.process_conn_stats(conn_stats)

    def _process_timestamp(self, fields: Sequence[str]) -> None:
        """Parse timestamp and send to listener."""
        # T,[YYYYMMDD HH:MM:SS]
        assert fields[0] == "T"
        assert len(fields) > 1
        dt_tm_tuple = fr.read_timestamp_msg(fields[1])
        timestamp = FeedConn.TimeStampMsg(date=dt_tm_tuple[0],
                                          time=dt_tm_tuple[1])
        for listener in self._listeners:
            listener.process_timestamp(timestamp)

    def _process_error(self, fields: Sequence[str]) -> None:
        """Called when IQFeed.exe sends us an error message."""
        assert fields[0] == "E"
        assert len(fields) > 1
        for listener in self._listeners:
            listener.process_error(fields)

    def add_listener(self, listener) -> None:
        """
        Call this to receive updates from this Conn class.

        :param listener: An object of the appropriate listener class.

        You need to call this function with each object that you want messages
        sent to. The object must be of (or derived from) the "appropriate"
        listener class. The various processing functions call callbacks in
        the listeners that have been registered to them when the Conn class
        receives messages.

        """
        if listener not in self._listeners:
            self._listeners.append(listener)

    def remove_listener(self, listener) -> None:
        """
        Call this to unsubscribe the listener object.

        :param listener: An object of the appropriate listener class.

        You must call this if a listener class that has been subscribed for
        updates is going to be deleted. Since python is GC'd, if you don't
        do this the object won't actually be destroyed since the Conn class
        holds a handle to it and it will keep sending the object messages.

        You may want to add something that unsubscribes to the listener
        object's destructor.

        """
        if listener in self._listeners:
            self._listeners.remove(listener)

    def _set_protocol(self, protocol) -> None:
        self._send_cmd("S,SET PROTOCOL,%s\r\n" % protocol)

    def _send_connect_message(self) -> None:
        msg = "S,CONNECT\r\n"
        self._send_cmd(msg)

    def _send_disconnect_message(self) -> None:
        self._send_cmd("S,DISCONNECT\r\n")

    def _set_client_name(self, name) -> None:
        self._name = name
        msg = "S,SET CLIENT NAME,%s\r\n" % name
        self._send_cmd(msg)


class QuoteConn(FeedConn):
    """
    QuoteConn provides real-time Level 1 data and real-time news.

    QuoteConn provides access to top-of-book quotes, regional quotes
    (quotes from a single exchange), fundamentals (which includes
    reference) data, streaming real-time news. It derives from FeedConn
    so it also provides timestamps and feed status messages and
    everything else FeedConn provides.

    Quotes, Trades and Fundamental data is provided as a numpy structured
    array since you are likely going to do something fancy with
    it.

    Regional Quote updates are provided as a structured array of type
    QuoteConn.regional_type

    Fundamental Data updates are provided as a structured array of type
    QuoteConn.fundamental_type. Fundamental data includes what one would
    consider reference data like for example the expiration date of an
    option  (if the symbol subscribed to was a option).

    READ THIS CAREFULLY: For quote updates (provided when the top of book
    quote changes or a trade happens) IQFeed.exe can send dynamic fieldsets.
    This means that you can ask for any fields (subset of the set available)
    you want. This map quote_msg_map lists all available fields. The keys of
    the map is the field name used by DTN. The values corresponding to each
    key a tuple of (FieldName used by DTN, FieldName used in Structured Array,
    numpy scalar type used for that field).

    We start with a default set of fields (same as default in the IQFeed
    docs. If you want a different set of fields, call
    select_update_fieldnames with the fieldnames you want. If you want
    a different set of fieldnames for options and stocks, create two
    instances of QuoteConn. Use one for all stock subscriptions and one for
    all options subscriptions. They can both update the same listener if
    that is what you want.

    If you don't understand the above two paragraphs, look at the code, look
    at the examples, run the examples and then read the above again.

    Information like connection statistics etc and news is provided as
    more vanilla Python types.

    The way to use this class is to create the class, then create an object
    of a class written by your derived from QuoteListener and subscribe to
    updates from this class by calling the member function add_listener
    with the Listener object. Then call the members of this class to
    subscribe to updates of various kinds for symbols, get real-time
    news updates etc.

    """
    host = FeedConn.host
    port = FeedConn.quote_port

    # Type of numpy structured array used to return regional quotes.
    regional_type = np.dtype([('Symbol', 'S64'), ('Regional Bid', 'f8'),
                              ('Regional BidSize', 'u8'),
                              ('Regional BidTime', 'u8'),
                              ('Regional Ask', 'f8'),
                              ('Regional AskSize', 'u8'),
                              ('Regional AskTime', 'u8'),
                              ('Market Center', 'u1'),
                              ('Fraction Display Code', 'u1'),
                              ('Decimal Precision', 'u2')])

    # List of fields provided by IQFeed.exe in the fundamentals message.
    fundamental_fields = ["Symbol", "Exchange ID", "PE", "Average Volume",
                          "52 Week High", "52 Week Low", "Calendar Year High",
                          "Calendar Year Low", "Dividend Yield",
                          "Dividend Amount", "Dividend Rate", "Pay Date",
                          "Ex-dividend Date", "(Reserved)", "(Reserved)",
                          "(Reserved)", "Short Interest", "(Reserved)",
                          "Current Year EPS", "Next Year EPS",
                          "Five-year Growth Percentage", "Fiscal Year End",
                          "(Reserved)", "Company Name", "Root Option Symbol",
                          "Percent Held By Institutions", "Beta", "Leaps",
                          "Current Assets", "Current Liabilities",
                          "Balance Sheet Date", "Long-term Debt",
                          "Common Shares Outstanding", "(Reserved)",
                          "Split Factor 1", "Split Factor 2", "(Reserved)",
                          "Market Center", "Format Code", "Precision", "SIC",
                          "Historical Volatility", "Security Type",
                          "Listed Market", "52 Week High Date",
                          "52 Week Low Date", "Calendar Year High Date",
                          "Calendar Year Low Date", "Year End Close",
                          "Maturity Date", "Coupon Rate", "Expiration Date",
                          "Strike Price", "NAICS", "Exchange Root",
                          "Option Premium Multiplier",
                          "Option Multiple Deliverable"]

    # Type of numpy structured array used to return fundamental data.
    fundamental_type = [('Symbol', 'S128'), ('PE', 'f8'),
                        ('Average Volume', 'f8'), ('52 Week High', 'f8'),
                        ('52 Week Low', 'f8'), ('Calendar Year High', 'f8'),
                        ('Calendar Year Low', 'f8'), ('Dividend Yield', 'f8'),
                        ('Dividend Amount', 'f8'), ('Dividend Rate', 'f8'),
                        ('Pay Date', 'M8[D]'), ('Ex-dividend Date', 'M8[D]'),
                        ('Short Interest', 'i8'), ('Current Year EPS', 'f8'),
                        ('Next Year EPS', 'f8'),
                        ('Five-year Growth Percentage', 'f8'),
                        ('Fiscal Year End', 'u1'), ('Company Name', 'S256'),
                        ('Root Option Symbol', 'S256'),
                        ('Percent Held By Institutions', 'f8'), ('Beta', 'f8'),
                        ('Leaps', 'S128'), ('Current Assets', 'f8'),
                        ('Current Liabilities', 'f8'),
                        ('Balance Sheet Date', 'M8[D]'),
                        ('Long-term Debt', 'f8'),
                        ('Common Shares Outstanding', 'f8'),
                        ('Split Factor 1 Date', 'M8[D]'),
                        ('Split Factor 1', 'f8'),
                        ('Split Factor 2 Date', 'M8[D]'),
                        ('Split Factor 2', 'f8'), ('Format Code', 'u1'),
                        ('Precision', 'u1'), ('SIC', 'u8'),
                        ('Historical Volatility', 'f8'),
                        ('Security Type', 'u1'), ('Listed Market', 'u1'),
                        ('52 Week High Date', 'M8[D]'),
                        ('52 Week Low Date', 'M8[D]'),
                        ('Calendar Year High Date', 'M8[D]'),
                        ('Calendar Year Low Date', 'M8[D]'),
                        ('Year End Close', 'f8'), ('Maturity Date', 'M8[D]'),
                        ('Coupon Rate', 'f8'), ('Expiration Date', 'M8[D]'),
                        ('Strike Price', 'f8'), ('NAICS', 'u8'),
                        ('Exchange Root', 'S128'),
                        ('Option Premium Multiplier', 'f8'),
                        ('Option Multiple Deliverable', 'u8')]

    # For quote updates (provided when the top of book quote changes or a
    # trade happens) IQFeed.exe can send dynamic fieldsets. This means that
    # you can ask for any fields you want. This map lists all available
    # fields and a numpy, the name used for that field in the numpy structured
    # array and the numpy scalar type used for that field.
    #
    # We start with a default set of fields (same as default in the IQFeed
    # docs. If you want a different set of fields, call
    # select_update_fieldnames with the fieldnames you want. If you want
    # a different set of fieldnames for options and stocks, create two
    # instances of QuoteConn. Use one for all stock subscriptions and one for
    # all options subscriptions. They can both update the same listener if
    # that is what you want.
    quote_msg_map = {'Symbol': ('Symbol', 'S128', lambda x: x),
                     '7 Day Yield': ('7 Day Yield', 'f8', fr.read_float64),
                     'Ask': ('Ask', 'f8', fr.read_float64),
                     'Ask Change': ('Ask Change', 'f8', fr.read_float64),
                     'Ask Market Center':
                         ('Ask Market Center', 'u1', fr.read_uint8),
                     'Ask Size': ('Ask Size', 'u8', fr.read_uint64),
                     'Ask Time': ('Ask Time', 'u8', fr.read_hhmmssus),
                     # TODO: Parse:
                     'Available Regions':
                         ('Available Regions', 'S128', lambda x: x),
                     'Average Maturity':
                         ('Average Maturity', 'f8', fr.read_float64),
                     'Bid': ('Bid', 'f8', fr.read_float64),
                     'Bid Change': ('Bid Change', 'f8', fr.read_float64),
                     'Bid Market Center':
                         ('Bid Market Center', 'u1', fr.read_uint8),
                     'Bid Size': ('Bid Size', 'u8', fr.read_uint64),
                     'Bid Time': ('Bid Time', 'u8', fr.read_hhmmssus),
                     'Change': ('Change', 'f8', fr.read_float64),
                     'Change From Open': (
                     'Change From Open', 'f8', fr.read_float64),
                     'Close': ('Close', 'f8', fr.read_float64),
                     'Close Range 1': ('Close Range 1', 'f8', fr.read_float64),
                     'Close Range 2': ('Close Range 2', 'f8', fr.read_float64),
                     'Days to Expiration':
                         ('Days to Expiration', 'u2', fr.read_uint16),
                     'Decimal Precision':
                         ('Decimal Precision', 'u1', fr.read_uint8),
                     'Delay': ('Delay', 'u1', fr.read_uint8),
                     'Exchange ID': ('Exchange ID', 'u1', fr.read_hex),
                     'Extended Trade': ('Extended Price', 'f8',
                                        fr.read_float64),
                     'Extended Trade Date':
                         ('Extended Trade Date', 'M8[D]', fr.read_mmddccyy),
                     'Extended Trade Market Center':
                         ('Extended Trade Market Center', 'u1', fr.read_uint8),
                     'Extended Trade Size':
                         ('Extended Trade Size', 'u8', fr.read_uint64),
                     'Extended Trade Time':
                         ('Extended Trade Time', 'u8', fr.read_hhmmssus),
                     'Extended Trading Change':
                         ('Extended Trading Change', 'f8', fr.read_float64),
                     'Extended Trading Difference':
                         ('Extended Trading Difference', 'f8', fr.read_float64),
                     # TODO: Parse:
                     'Financial Status Indicator':
                         ('Financial Status Indicator', 'S1', lambda x: x),
                     'Fraction Display Code':
                         ('Fraction Display Code', 'u1', fr.read_uint8),
                     'High': ('High', 'f8', fr.read_float64),
                     'Last': ('Last', 'f8', fr.read_float64),
                     'Last Date': ('Last Date', 'M8[D]', fr.read_mmddccyy),
                     'Last Market Center':
                         ('Last Market Center', 'u1', fr.read_uint8),
                     'Last Size': ('Last Size', 'u8', fr.read_uint64),
                     'Last Time': ('Last Time', 'u8', fr.read_hhmmssus),
                     'Low': ('Low', 'f8', fr.read_float64),
                     'Market Capitalization':
                         ('Market Capitalization', 'f8', fr.read_float64),
                     'Market Open':
                         ('Market Open', 'b1', fr.read_is_market_open),
                     # TODO: Parse:
                     'Message Contents':
                         ('Message Contents', 'S9', lambda x: x),
                     'Most Recent Trade':
                         ('Most Recent Trade', 'f8', fr.read_float64),
                     'Most Recent Trade Conditions':
                         ('Most Recent Trade Conditions', 'S16', lambda x: x),
                     # todo: Parse
                     'Most Recent Trade Date':
                         ('Most Recent Trade Date', 'M8[D]', fr.read_mmddccyy),
                     'Most Recent Trade Market Center':
                         ('Most Recent Trade Market Center', 'u1',
                          fr.read_uint8),
                     'Most Recent Trade Size':
                         ('Most Recent Trade Size', 'u8', fr.read_uint64),
                     'Most Recent Trade Time':
                         ('Most Recent Trade Time', 'u8', fr.read_hhmmssus),
                     'Net Asset Value':
                         ('Net Asset Value', 'f8', fr.read_float64),
                     'Number of Trades Today':
                         ('Number of Trades Today', 'u8', fr.read_uint64),
                     'Open': ('Open', 'f8', fr.read_float64),
                     'Open Interest': ('Open Interest', 'u8', fr.read_uint64),
                     'Open Range 1': ('Open Range 1', 'f8', fr.read_float64),
                     'Open Range 2': ('Open Range 2', 'f8', fr.read_float64),
                     'Percent Change':
                         ('Percent Change', 'f8', fr.read_float64),
                     'Percent Off Average Volume':
                         ('Percent Off Average Volume', 'f8', fr.read_float64),
                     'Previous Day Volume':
                         ('Previous Day Volume', 'u8', fr.read_uint64),
                     'Price-Earnings Ratio':
                         ('Price-Earnings Ratio', 'f8', fr.read_float64),
                     'Range': ('Range', 'f8', fr.read_float64),
                     'Restricted Code':
                         ('Restricted Code', 'b1', fr.read_is_short_restricted),
                     'Settle': ('Settle', 'f8', fr.read_float64),
                     'Settlement Date':
                         ('Settlement Date', 'M8[D]', fr.read_mmddccyy),
                     'Spread': ('Spread', 'f8', fr.read_float64),
                     'Tick': ('Tick', 'i8', fr.read_tick_direction),
                     'TickID': ('TickId', 'u8', fr.read_uint64),
                     'Total Volume': ('Total Volume', 'u8', fr.read_uint64),
                     'Volatility': ('Volatility', 'f8', fr.read_float64),
                     'VWAP': ('VWAP', 'f8', fr.read_float64)}

    NewsMsg = namedtuple("NewsMsg", (
        "story_id", "distributor", "symbol_list",
        "story_date", "story_time", "headline"))

    CustomerInfoMsg = namedtuple("CustomerInfoMsg", (
        "svc_type", "ip_address", "port", "token", "version", "rt_exchanges",
        "max_symbols", "flags"))

    def __init__(self, name: str = "QuoteConn", host: str = FeedConn.host,
                 port: int = port):
        super().__init__(name, host, port)
        self._current_update_fields = []
        self._update_names = []
        self._update_dtype = []
        self._update_reader = []
        self._set_message_mappings()
        self._current_update_fields = ["Symbol", "Most Recent Trade",
                                       "Most Recent Trade Size",
                                       "Most Recent Trade Time",
                                       "Most Recent Trade Market Center",
                                       "Total Volume", "Bid", "Bid Size",
                                       "Ask",
                                       "Ask Size", "Open", "High", "Low",
                                       "Close", "Message Contents",
                                       "Most Recent Trade Conditions"]
        self._num_update_fields = len(self._current_update_fields)
        self._set_current_update_structs(self._current_update_fields)

        self._empty_fundamental_msg = np.zeros(
                1, dtype=QuoteConn.fundamental_type)
        self._empty_regional_msg = np.zeros(1, dtype=QuoteConn.regional_type)

    def connect(self) -> None:
        """
        Call super.connect() and call make initialization requests.

        """
        super().connect()
        self._request_fundamental_fieldnames()
        self._request_all_update_fieldnames()
        self._request_current_update_fieldnames()

    def _set_message_mappings(self) -> None:
        """Creates map of message processing functions."""
        super()._set_message_mappings()
        self._pf_dict['n'] = self._process_invalid_symbol
        self._pf_dict['N'] = self._process_news
        self._pf_dict['R'] = self._process_regional_quote
        self._pf_dict['P'] = self._process_summary
        self._pf_dict['Q'] = self._process_update
        self._pf_dict['F'] = self._process_fundamentals

        self._sm_dict["KEY"] = self._process_auth_key
        self._sm_dict["KEYOK"] = self._process_keyok
        self._sm_dict["CUST"] = self._process_customer_info
        self._sm_dict["WATCHES"] = self._process_watches
        self._sm_dict["CURRENT LOG LEVELS"] = self._process_current_log_levels
        self._sm_dict[
            "SYMBOL LIMIT REACHED"] = self._process_symbol_limit_reached
        self._sm_dict["IP"] = self._process_ip_addresses_used
        self._sm_dict[
            "FUNDAMENTAL FIELDNAMES"] = self._process_fundamental_fieldnames
        self._sm_dict["UPDATE FIELDNAMES"] = self._process_update_fieldnames
        self._sm_dict[
            "CURRENT UPDATE FIELDNAMES"] = \
            self._process_current_update_fieldnames

    def _process_invalid_symbol(self, fields: Sequence[str]) -> None:
        """Called when IQFeed tells us we used and invalid symbol."""
        assert len(fields) > 1
        assert fields[0] == 'n'
        bad_sym = fields[1]
        for listener in self._listeners:
            listener.process_invalid_symbol(bad_sym)

    def _process_news(self, fields: Sequence[str]):
        """Process a real-time news story."""
        assert len(fields) > 5
        assert fields[0] == "N"
        distributor = fields[1]
        story_id = fields[2]
        symbol_list = fields[3].split(":")
        story_date, story_time = fr.read_live_news_timestamp(fields[4])
        headline = fields[5]
        news = QuoteConn.NewsMsg(
                story_id=story_id,
                distributor=distributor,
                symbol_list=symbol_list,
                story_date=story_date,
                story_time=story_time,
                headline=headline)
        for listener in self._listeners:
            listener.process_news(news)

    def _process_regional_quote(self, fields: Sequence[str]):
        """Process a regional quote message."""
        assert len(fields) > 11
        assert fields[0] == "R"
        rgn_quote = self._empty_regional_msg
        rgn_quote["Symbol"] = fields[1]
        rgn_quote["Regional Bid"] = fr.read_float64(fields[3])
        rgn_quote["Regional BidSize"] = fr.read_uint64(fields[4])
        rgn_quote["Regional BidTime"] = fr.read_hhmmss(fields[5])
        rgn_quote["Regional Ask"] = fr.read_float64(fields[6])
        rgn_quote["Regional AskSize"] = fr.read_uint64(fields[7])
        rgn_quote["Regional AskTime"] = fr.read_hhmmss(fields[8])
        rgn_quote["Fraction Display Code"] = fr.read_uint8(fields[9])
        rgn_quote["Decimal Precision"] = fr.read_uint8(fields[10])
        rgn_quote["Market Center"] = fr.read_uint8(fields[11])
        for listener in self._listeners:
            listener.process_regional_rgn_quote(rgn_quote)

    def _process_summary(self, fields: Sequence[str]) -> None:
        """Process a symbol summary message"""
        assert len(fields) > 2
        assert fields[0] == "P"
        update = self._create_update(fields)
        for listener in self._listeners:
            listener.process_summary(update)

    def _process_update(self, fields: Sequence[str]) -> None:
        """Process a symbol update message."""
        assert len(fields) > 2
        assert fields[0] == "Q"
        update = self._create_update(fields)
        for listener in self._listeners:
            listener.process_update(update)

    def _create_update(self, fields: Sequence[str]) -> np.array:
        """Create an update message."""
        update = self._empty_update_msg
        for field_num, field in enumerate(fields[1:]):
            if field_num >= self._num_update_fields and field == "":
                break
            update[self._update_names[field_num]] = self._update_reader[
                field_num](field)
        return update

    def _process_fundamentals(self, fields: Sequence[str]):
        """Process a fundamental data message."""
        assert len(fields) > 55
        assert fields[0] == 'F'
        msg = self._empty_fundamental_msg

        msg['Symbol'] = fields[1]
        msg['PE'] = fr.read_float64(fields[3])
        msg['Average Volume'] = fr.read_uint64(fields[4])
        msg['52 Week High'] = fr.read_float64(fields[5])
        msg['52 Week Low'] = fr.read_float64(fields[6])
        msg['Calendar Year High'] = fr.read_float64(fields[7])
        msg['Calendar Year Low'] = fr.read_float64(fields[8])
        msg['Dividend Yield'] = fr.read_float64(fields[9])
        msg['Dividend Amount'] = fr.read_float64(fields[10])
        msg['Dividend Rate'] = fr.read_float64(fields[11])
        msg['Pay Date'] = fr.read_mmddccyy(fields[12])
        msg['Ex-dividend Date'] = fr.read_mmddccyy(fields[13])
        msg['Short Interest'] = fr.read_uint64(fields[17])
        msg['Current Year EPS'] = fr.read_float64(fields[19])
        msg['Next Year EPS'] = fr.read_float64(fields[20])
        msg['Five-year Growth Percentage'] = fr.read_float64(fields[21])
        msg['Fiscal Year End'] = fr.read_uint8(fields[22])
        msg['Company Name'] = fields[24]
        msg['Root Option Symbol'] = fields[25]  # todo:Parse
        msg['Percent Held By Institutions'] = fr.read_float64(fields[26])
        msg['Beta'] = fr.read_float64(fields[27])
        msg['Leaps'] = fields[28]  # todo: Parse
        msg['Current Assets'] = fr.read_float64(fields[29])
        msg['Current Liabilities'] = fr.read_float64(fields[30])
        msg['Balance Sheet Date'] = fr.read_mmddccyy(fields[31])
        msg['Long-term Debt'] = fr.read_float64(fields[32])
        msg['Common Shares Outstanding'] = fr.read_float64(fields[33])
        (fact, split_date) = fr.read_split_string(fields[35])
        msg['Split Factor 1 Date'] = split_date
        msg['Split Factor 1'] = fact
        (fact, split_date) = fr.read_split_string(fields[36])
        msg['Split Factor 2 Date'] = split_date
        msg['Split Factor 2'] = fact
        msg['Format Code'] = fr.read_uint8(fields[39])
        msg['Precision'] = fr.read_uint8(fields[40])
        msg['SIC'] = fr.read_uint64(fields[41])
        msg['Historical Volatility'] = fr.read_float64(fields[42])
        msg['Security Type'] = fr.read_int(fields[43])
        msg['Listed Market'] = fr.read_uint8(fields[44])
        msg['52 Week High Date'] = fr.read_mmddccyy(fields[45])
        msg['52 Week Low Date'] = fr.read_mmddccyy(fields[46])
        msg['Calendar Year High Date'] = fr.read_mmddccyy(fields[47])
        msg['Calendar Year Low Date'] = fr.read_mmddccyy(fields[48])
        msg['Year End Close'] = fr.read_float64(fields[49])
        msg['Maturity Date'] = fr.read_mmddccyy(fields[50])
        msg['Coupon Rate'] = fr.read_float64(fields[51])
        msg['Expiration Date'] = fr.read_mmddccyy(fields[52])
        msg['Strike Price'] = fr.read_float64(fields[53])
        msg['NAICS'] = fr.read_uint8(fields[54])
        msg['Exchange Root'] = fields[55]
        msg['Option Premium Multiplier'] = fr.read_float64(fields[56])
        msg['Option Multiple Deliverable'] = fr.read_uint8(fields[57])
        for listener in self._listeners:
            listener.process_fundamentals(msg)

    def _process_auth_key(self, fields: Sequence[str]) -> None:
        """Still sent so needs to be handled, but obsolete."""
        assert len(fields) > 2
        assert fields[0] == "S"
        assert fields[1] == "KEY"
        auth_key = fields[2]
        for listener in self._listeners:
            listener.process_auth_key(auth_key)

    def _process_keyok(self, fields: Sequence[str]) -> None:
        """Still sent so needs to be handled, but obsolete."""
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == "KEYOK"
        for listener in self._listeners:
            listener.process_keyok()

    def _process_customer_info(self, fields: Sequence[str]) -> None:
        """Handle a customer information message."""
        assert len(fields) > 11
        assert fields[0] == 'S'
        assert fields[1] == "CUST"
        cust_info = QuoteConn.CustomerInfoMsg(
                svc_type=(fields[2] == "real_time"), ip_address=fields[3],
                port=int(fields[4]), token=fields[5], version=fields[6],
                rt_exchanges=fields[8].split(" "), max_symbols=int(fields[10]),
                flags=fields[11])
        for listener in self._listeners:
            listener.process_customer_info(cust_info)

    def _process_watches(self, fields: Sequence[str]) -> None:
        """Handle a watches message."""
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == "WATCHES"
        for listener in self._listeners:
            listener.process_watched_symbols(fields[2:])

    def _process_current_log_levels(self, fields: Sequence[str]) -> None:
        """Called when IQFeed acknowledges log levels have changed"""
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == "CURRENT LOG LEVELS"
        for listener in self._listeners:
            listener.process_log_levels(fields[2:])

    def _process_symbol_limit_reached(self, fields: Sequence[str]) -> None:
        """Handle IQFeed telling us the symbol limit has been reached."""
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == "SYMBOL LIMIT REACHED"
        sym = fields[2]
        for listener in self._listeners:
            listener.process_symbol_limit_reached(sym)

    def _process_ip_addresses_used(self, fields: Sequence[str]) -> None:
        """IP addresses IQFeed.exe is connecting to for data."""
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'IP'
        ip = fields[2]
        for listener in self._listeners:
            listener.process_ip_addresses_used(ip)

    def _process_fundamental_fieldnames(self, fields: Sequence[str]) -> None:
        """Process a fundamental data message."""
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'FUNDAMENTAL FIELDNAMES'
        for field in fields[2:]:
            if field not in QuoteConn.fundamental_fields:
                err_msg = ("%s not found in dtn_fundamental_fields in %s" %
                           (field, self.name()))
                raise UnexpectedField(err_msg)
        for field in QuoteConn.fundamental_fields:
            if field not in fields[2:]:
                err_msg = ("%s not found in FUNDAMENTAL FIELDNAMES in %s" %
                           (field, self.name()))
                raise UnexpectedField(err_msg)

    def _process_update_fieldnames(self, fields: Sequence[str]) -> None:
        """
        Process a message giving us a list of all update fieldnames.

        Really only used to ensure we are supporting all available fieldnames
        and doubly sanity checking that the protocol version matches.

        """
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'UPDATE FIELDNAMES'
        for field in fields[2:]:
            if field not in QuoteConn.quote_msg_map:
                err_msg = ("%s not found in dtn_update_map in %s" %
                           (field, self.name()))
                raise UnexpectedField(err_msg)
        for field in QuoteConn.quote_msg_map:
            if field not in fields[2:]:
                err_msg = (
                    "%s not found in UPDATE FIELDNAMES in %s" %
                    (field, self.name()))
                raise UnexpectedField(err_msg)

    def _process_current_update_fieldnames(self, fields: Sequence[str]) -> \
            None:
        """
        IQFeed has accepted our update fieldnames request.

        We need to update the update dtype and the functions that parse the
        update message here. Update messages after this message will include
        the new set of fields requested.

        """
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'CURRENT UPDATE FIELDNAMES'
        self._set_current_update_structs(fields[2:])

    def _set_current_update_structs(self, fields):
        """
        Update the various data structures used to parse update messages.

        :param fields: List of fields in the new dynamic fieldset.

        Read www.iqfeed.net/dev/api/docs/DynamicFieldsets.cfm first
        if you want to grok what's happening here.

        When select_update_fieldnames is called, IQFeed.exe responds with
        and UPDATE FIELDNAMES to acknowledge the request and all new summary
        and update messages have a new set of fields. This allows us to use
        one instance of QuoteConn (which opens it's own connection to IQFeed)
        for stocks and request stock specific fields and another instance of
        QuoteConn for options (for example) and request option specific fields
        so you aren't getting fields which make sense for options but not
        stocks on each update for a stock.

        When the UPDATE FIELDNAMES message is received we need to dynamically
        create the np.dtype that the message with this set of fields will
        be encoded as when sent to listeners as well as dynamically creating
        a function to parse the message.

        This function is where that magic happens. We update the np.dtype that
        an update message is encoded as when sent to listeners. We update a
        list of field reading functions that read each field in the update
        messages, the number of expected update fields etc.

        There does not seem to be a speed penalty relative to creating a
        separate QuoteConn derivative class with a separate update message
        parsing function for each set of update fieldnames we may want.

        """
        num_update_fields = len(fields)
        new_update_fields = list(itertools.repeat("", num_update_fields))
        new_update_names = new_update_fields
        new_update_dtypes = list(
            itertools.repeat(("no_name", 'i8'), num_update_fields))
        new_update_reader = list(
                itertools.repeat(lambda x: x, num_update_fields))
        for field_num, field in enumerate(fields):
            if field not in QuoteConn.quote_msg_map:
                raise RuntimeError("%s not in QuoteConn.dtn_update_map" %
                                   field)
            new_update_fields[field_num] = field
            dtn_update_tup = QuoteConn.quote_msg_map[field]
            new_update_names[field_num] = dtn_update_tup[0]
            new_update_dtypes[field_num] = (
                dtn_update_tup[0], dtn_update_tup[1])
            new_update_reader[field_num] = dtn_update_tup[2]
        self._current_update_fields = new_update_fields
        self._update_names = new_update_names
        self._update_dtype = new_update_dtypes
        self._update_reader = new_update_reader
        self._num_update_fields = len(new_update_fields)

        self._empty_update_msg = np.zeros(1, dtype=self._update_dtype)

    def _request_fundamental_fieldnames(self) -> None:
        """
        Request a list of all fields in the fundamentals message.

        Clients should never call this. Called internally to make sure what
        we expect is what IQFeed.exe expects to send us as a sanity-check.

        """
        self._send_cmd("S,REQUEST FUNDAMENTAL FIELDNAMES\r\n")

    def _request_all_update_fieldnames(self) -> None:
        """
        Request a list of all available fields for updates messages.

        Clients should never call this. Called internally to make sure what
        we expect is what IQFeed.exe expects to send us as a sanity-check.

        """
        self._send_cmd("S,REQUEST ALL UPDATE FIELDNAMES\r\n")

    def _request_current_update_fieldnames(self) -> None:
        """
        Request a list of all current fields for updates messages.

        Clients should never call this. Called internally to make sure what
        we expect is what IQFeed.exe expects to send us as a sanity-check.

        """
        self._send_cmd("S,REQUEST CURRENT UPDATE FIELDNAMES\r\n")

    def select_update_fieldnames(self, field_names: List[str]) -> None:
        """
        Request the fields you want sent in an update message.

        :param field_names: List of field names

        The first field will always be the symbol. If you specify symbol
        somewhere other than the first field, the location of the symbol field
        and the first field will be swapped. If you do not specify the symbol
        field, it will be inserted. Fields should be named as in
        www.iqfeed.net/dev/api/docs/DynamicFieldsets.cfm

        You may want to call this before subscribing to any symbols.

        """
        symbol_field = "Symbol"
        if symbol_field not in field_names:
            field_names.insert(0, symbol_field)
        else:
            symbol_idx = field_names.index("Symbol")
            if symbol_idx != 0:
                field_names[0], field_names[symbol_idx] = field_names[
                                                              symbol_idx], \
                                                          field_names[0]
        self._send_cmd("S,SELECT UPDATE FIELDS,%s\r\n" % ",".join(field_names))

    def req_timestamp(self) -> None:
        """
        Ask IQFeed.exe to send you a single timestamp message

        The timestamp message sent by IQFeed.exe is in addition to the
        messages normally. By default IQFeed.exe sends a timestamp
        message every second, but you can turn off and turn back on
        those messages. You probably only want to use this if you have
        turned off the once a second timestamp messages.

        process_timestamp is called in each QuoteListener
        when a timestamp message is received.

        """
        self._send_cmd("T\r\n")

    def timestamp_on(self) -> None:
        """
        Turn on automatic (1 msg/sec) timestamp messages.

        process_timestamp is called in each listener that is listening
        when a timestamp message is received.

        """
        self._send_cmd("S,TIMESTAMPSON\r\n")

    def timestamp_off(self) -> None:
        """Turn on automatic (1 msg/sec) timestamp messages."""
        self._send_cmd("S,TIMESTAMPSOFF\r\n")

    def trades_watch(self, symbol: str) -> None:
        """
        Watch a symbol requesting updates only when a trade happens.

        :param symbol: A valid symbol for a security or derivative.

        IQFeed.exe will send a summary message for the symbol followed
        by an update message every time there is a trade.

        process_summary is called in each listener when a summary message
        is received.
        process_fundamentals is called in each listener when a fundamentals
        message is received.
        process_update is called in each listener when an update message
        is received.

        """
        self._send_cmd("t%s\r\n" % symbol)

    def watch(self, symbol: str) -> None:
        """
        Watch a symbol requesting updates for both trades and quotes.

        :param symbol:  A valid symbol for a security or derivative.

        IQFeed.exe will send a summary message and a fundamental message for
        the symbol followed by an update message every time there is trade or
        the top of book quote changes.

        process_summary is called in each listener when a summary message is
        received.
        process_fundamentals is called in each listener when a fundamentals
        message is received.
        process_update is called in each listener when an update message is
        received.

        """
        self._send_cmd("w%s\r\n" % symbol)

    def unwatch(self, symbol: str) -> None:
        """
        Stop watching a symbol.

        :param symbol:  A valid symbol for a security or derivative.

        IQFeed.exe will send stop sending updates for this security. This
        unwatches trades watches and trades and quotes watches. This also
        stops all regional quote updates for this security.

        """
        self._send_cmd("r%s\r\n" % symbol)

    def regional_watch(self, symbol: str) -> None:
        """
        Request updates when the regional quote for a symbol changes.

        :param symbol:  A valid symbol for a security or derivative.

        IQFeed.exe will send a regional message for the symbol whenever
        the top of book quote on a regional exchange changes. This is
        the top of book on each exchange where the security or derivative
        trades, as opposed to the market-wide top of book which you get when
        you call watch on a symbol. It is NOT the full order book.

        process_regional_quote is called in each listener when a regional
        quote message is received.

        """
        self._send_cmd("S,REGON,%s\r\n" % symbol)

    def regional_unwatch(self, symbol: str) -> None:
        """
        Stop sending regional updates.

        :param symbol:  A valid symbol for a security or derivative.

        """
        self._send_cmd("S,REGOFF,%s\r\n" % symbol)

    def refresh(self, symbol: str) -> None:
        """
        Request a refresh for the symbol.

        :param symbol:  A valid symbol for a security or derivative.

        IQFeed.exe will send a summary message and a fundamental message
        for this security after it receives this message. This is useful if
        for some reason you believe you have bad data for the security in
        your cache. It cannot be used to get a data snapshot for a symbol
        you have not subscribed to. IQFeed.exe will ignore refresh requests
        for symbols you are not watching.

        It is a good idea to do this for all symbols you are watching if
        the feed disconnects and reconnects and you haven't gotten an
        update shortly after the reconnection.

        process_fundamental is called in each listener for the fundamentals
        message
        process_summary is called in each listener for the summary message.

        """
        self._send_cmd("f%s\r\n" % symbol)

    def request_watches(self) -> None:
        """
        Request a current watches message.

        IQFeed.exe will send you a list of all securities currently watched

        process_watched_symbols is called in each listener when the list of
        current watches message is received.

        """
        self._send_cmd("S,REQUEST WATCHES\r\n")

    def unwatch_all(self) -> None:
        """Unwatch all symbols."""
        self._send_cmd("S,UNWATCH ALL")

    def news_on(self) -> None:
        """
        Turn on real-time news messages.

        process_news is called in each listener when a news message is
        received.

        """
        self._send_cmd("S,NEWSON\r\n")

    def news_off(self) -> None:
        """Turn off real-time news messages"""
        self._send_cmd("S,NEWSOFF\r\n")

    def request_stats(self) -> None:
        """
        Request a connection statistics message.

        process_conn_stats is called for each listener when a connection
        statistics message is received.

        """
        self._send_cmd("S,REQUEST STATS\r\n")

    def set_log_levels(self, log_levels: Sequence[str]) -> None:
        """
        Set the logging level.

        :param log_levels: Sequence of log levels desired.

        See www.iqfeed.net/dev/api/docs/IQConnectLogging.cfm

        If log levels are updated it will be acknowledged by a call to
        process_log_levels in each listener

        """
        self._send_cmd("S,SET LOG LEVELS,%s\r\n" % ",".join(log_levels))


class AdminConn(FeedConn):
    """
    AdminConn provides a connection to IQFeed's Administrative socket.

    AdminConn is used to find out the health of the feed, figure out the
    status of each connection made to IQFeed, and also set various parameters
    to the feed.

    See www.iqfeed.net/dev/api/docs/AdminviaTCPIP.cfm

    You may be confused about duplicated functionality between this and some
    other XXXConn classes. This is because those socket connections were
    developed before IQFeed had a separate Administrative socket and the
    functionality there has not been removed. This is why you'll notice that
    substantial AdminConn functionality is simply inherited from FeedConn.

    """

    port = FeedConn.admin_port
    host = FeedConn.host

    ClientStatsMsg = namedtuple("ClientStatsMsg", (
        "client_type", "client_id", "client_name", "start_dt", "start_tm",
        "kb_sent", "kb_recvd", "kb_queued", "num_quote_subs", "num_reg_subs",
        "num_depth_subs"))

    def __init__(self, name: str = "AdminConn", host: str = host,
                 port: int = port):
        super().__init__(name, host, port)
        self._set_message_mappings()

    def _set_message_mappings(self) -> None:
        """Set message mappings."""
        super()._set_message_mappings()
        self._sm_dict[
            "REGISTER CLIENT APP COMPLETED"] = \
            self._process_register_client_app_completed
        self._sm_dict[
            "REMOVE CLIENT APP COMPLETED"] = \
            self._process_remove_client_app_completed
        self._sm_dict["CURRENT LOGINID"] = self._process_current_login
        self._sm_dict["CURRENT PASSWORD"] = self._process_current_password
        self._sm_dict["LOGIN INFO SAVED"] = self._process_login_info_saved
        self._sm_dict[
            "LOGIN INFO NOT SAVED"] = self._process_login_info_not_saved
        self._sm_dict["AUTOCONNECT ON"] = self._process_autoconnect_on
        self._sm_dict["AUTOCONNECT OFF"] = self._process_autoconnect_off
        self._sm_dict["CLIENTSTATS"] = self._process_client_stats

    def _process_register_client_app_completed(self,
                                               fields: Sequence[str]) -> None:
        """ Acknowledgement that the client app is registered. """
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == 'REGISTER CLIENT APP COMPLETED'
        for listener in self._listeners:
            listener.process_register_client_app_completed()

    def _process_remove_client_app_completed(self,
                                             fields: Sequence[str]) -> None:
        """
        Acknowledgement that the client app has de-registered.

        If your app is shutting down, another app may still be using IQFeed.
        Here you are telling IQFeed that all requests it is still getting are
        not from apps you wrote.

        """
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == 'REMOVE CLIENT APP COMPLETED'
        for listener in self._listeners:
            listener.process_remove_client_app_completed()

    def _process_current_login(self, fields: Sequence[str]) -> None:
        """IQFeed sending the current login."""
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'CURRENT LOGINID'
        login_id = fields[2]
        for listener in self._listeners:
            listener.process_current_login(login_id)

    def _process_current_password(self, fields: Sequence[str]) -> None:
        """IQFeed sending the current client password."""
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'CURRENT PASSWORD'
        password = fields[2]
        for listener in self._listeners:
            listener.process_current_password(password)

    def _process_login_info_saved(self, fields: Sequence[str]) -> None:
        """IQFeed sending the login info has been save by IQFeed."""
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == 'LOGIN INFO SAVED'
        for listener in self._listeners:
            listener.process_login_info_saved()

    def _process_login_info_not_saved(self, fields: Sequence[str]) -> None:
        """IQFeed sending the login info is no longer saved."""
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == 'LOGIN INFO NOT SAVED'
        for listener in self._listeners:
            listener.process_login_info_not_saved()

    def _process_autoconnect_on(self, fields: Sequence[str]) -> None:
        """IQFeed telling you that it will automatically connect on startup."""
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == 'AUTOCONNECT ON'
        for listener in self._listeners:
            listener.process_autoconnect_on()

    def _process_autoconnect_off(self, fields: Sequence[str]) -> None:
        """IQFeed will not autoconnect to DTN servers on startup."""
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == 'AUTOCONNECT OFF'
        for listener in self._listeners:
            listener.process_autoconnect_off()

    def _process_client_stats(self, fields: Sequence[str]) -> None:
        """Client statistics for a specific connection."""
        assert len(fields) > 10
        assert fields[0] == 'S'
        assert fields[1] == 'CLIENTSTATS'

        type_int = fr.read_int(fields[2])
        client_id = fr.read_int(fields[3])
        client_name = fields[4]
        (start_dt, start_tm) = fr.read_live_news_timestamp(fields[5])
        num_sym = fr.read_int(fields[6])
        num_reg_sym = fr.read_int(fields[7])
        kb_sent = fr.read_float(fields[8])
        kb_recvd = fr.read_float(fields[9])
        kb_queued = fr.read_float(fields[10])

        client_type = "Unknown"
        if 0 == type_int:
            client_type = "Admin"
        elif 1 == type_int:
            client_type = "Quote"
        elif 2 == type_int:
            client_type = "Depth"
        elif 3 == type_int:
            client_type = "Lookup"

        num_quote_subs = 0
        num_reg_subs = 0
        num_depth_subs = 0

        if 1 == type_int:
            num_quote_subs = num_sym
            num_reg_subs = num_reg_sym
        elif 2 == type_int:
            num_depth_subs = num_sym

        client_stats = AdminConn.ClientStatsMsg(
            client_type=client_type,
            client_id=client_id, client_name=client_name,
            start_dt=start_dt, start_tm=start_tm,
            kb_sent=kb_sent,
            kb_recvd=kb_recvd,
            kb_queued=kb_queued,
            num_quote_subs=num_quote_subs,
            num_reg_subs=num_reg_subs,
            num_depth_subs=num_depth_subs)
        for listener in self._listeners:
            listener.process_client_stats(client_stats)

    def register_client_app(self, product: str) -> None:
        """
        Register your application with IQFeed.

        :param product: Developer/Product token from DTN.

        IQFeed requires all developers to get a developer/app token from
        them before you develop using IQFeed. You can register you developer
        token either using the command line used to start IQFeed or here.

        IQFeed will not connect to DTN servers or send you any data without
        a developer token.

        process_register_client_app_completed called in listeners on success.

        """
        self._send_cmd("S,REGISTER CLIENT APP,%s\r\n" % product)

    def remove_client_app(self, product: str) -> None:
        """
        Unregister your application  with IQFeed.

        :param product: Developer/Product token from DTN.

        Unregister your developer token. Apps from other developers may still
        be talking to IQFeed. This tells IQFeed that any further calls are
        from them and not you.

        process_remove_client_app_completed called in listeners on success.

        """
        self._send_cmd("S REMOVE CLIENT APP,%s\r\n" % product)

    def set_login(self, login: str) -> None:
        """
        Set the user login.

        :param login: IQFeed subscriber's login

        The user of the app must have a subscription to IQFeed. Pass the user's
        login either here or in cmd line options when IQFeed is started.

        process_current_login called on listeners on success.

        """
        self._send_cmd("S,SET LOGINID,%s\r\n" % login)

    def set_password(self, password: str) -> None:
        """
        Set the user password.

        :param password: IQFeed subscriber's pwd

        The user of the app must have a subscription to IQFeed. Pass the user's
        password either here or in cmd line options when IQFeed is started.

        process_current_password called on listeners on success.

        """
        self._send_cmd("S,SET PASSWORD,%s\r\n" % password)

    def set_autoconnect(self, autoconnect: bool = True) -> None:
        """
        Tells IQFeed to autoconnect to DTN servers on startup or not

        :param autoconnect: True means autoconnect, False means don't

        process_autoconnect_[on/off] called on listeners to acknowledge
        success.

        """
        if autoconnect:
            self._send_cmd("S,SET AUTOCONNECT,On\r\n")
        else:
            self._send_cmd("S,SET AUTOCONNECT,Off\r\n")

    def save_login_info(self, save_info: bool = True) -> None:
        """
        Tells IQFeed to save the login info

        :param save_info: True means save info, False means don't

        process_login_info_[saved/no_saved] called on listeners to acknowledge
        success.

        """
        if save_info:
            self._send_cmd("S,SET SAVE LOGIN INFO,On\r\n")
        else:
            self._send_cmd("S,SET SAVE LOGIN INFO,Off\r\n")

    def client_stats_on(self) -> None:
        """
        Request client statistics from IQFeed.

        Call this is you want IQFeed to send you a message every second about
        the status of every connection. Lets you know things like number of
        subscriptions and if the connection is buffering.

        process_client_stats is called on listeners every second for each
        connection to IQFeed when you request client statistics.

        """
        self._send_cmd("S,CLIENTSTATS ON\r\n")

    def client_stats_off(self) -> None:
        """Turn off client statistics."""
        self._send_cmd("S,CLIENTSTATS OFF\r\n")

    def set_admin_variables(self, product: str, login: str, password: str,
                            autoconnect: bool = True,
                            save_info: bool = True) -> None:
        """Set the administrative variables."""
        self.register_client_app(product)
        self.set_login(login)
        self.set_password(password)
        self.set_autoconnect(autoconnect)
        self.save_login_info(save_info)


class HistoryConn(FeedConn):
    """
    HistoryConn is used to get historical data from IQFeed's lookup socket.

    This class returns historical data as the return value from the function
    used to request the data. So a listener is not strictly necessary.

    However it derives from FeedConn so basic administrative
    messages etc are still available via a generic IQFeedListener. If you
    aren't getting what you expect, it may be a good idea to listen for these
    messages to see if that tells you why.

    For more details see:
    www.iqfeed.net/dev/api/docs/HistoricalviaTCPIP.cfm

    """
    host = FeedConn.host
    port = FeedConn.lookup_port

    # Tick data is returned as a numpy array of this dtype. Note that
    # "tick-data" in IQFeed parlance means every trade with the latest top of
    # book quote at the time of the trade, NOT every quote and every trade.
    tick_type = np.dtype([('tick_id', 'u8'),
                          ('date', 'M8[D]'), ('time', 'm8[us]'),
                          ('last', 'f8'), ('last_sz', 'u8'),
                          ('last_type', 'S1'), ('mkt_ctr', 'u4'),
                          ('tot_vlm', 'u8'), ('bid', 'f8'), ('ask', 'f8'),
                          ('cond1', 'u1'), ('cond2', 'u1'), ('cond3', 'u1'),
                          ('cond4', 'u1')])
    tick_h5_type = np.dtype([('tick_id', 'u8'),
                             ('date', 'i8'), ('time', 'i8'),
                             ('last', 'f8'), ('last_sz', 'u8'),
                             ('last_type', 'S1'), ('mkt_ctr', 'u4'),
                             ('tot_vlm', 'u8'), ('bid', 'f8'), ('ask', 'f8'),
                             ('cond1', 'u1'), ('cond2', 'u1'), ('cond3', 'u1'),
                             ('cond4', 'u1')])

    # Bar data is returned as a numpy array of this type.
    bar_type = np.dtype([('date', 'M8[D]'), ('time', 'm8[us]'),
                         ('open_p', 'f8'), ('high_p', 'f8'),
                         ('low_p', 'f8'), ('close_p', 'f8'),
                         ('tot_vlm', 'u8'), ('prd_vlm', 'u8'),
                         ('num_trds', 'u8')])
    bar_h5_type = np.dtype([('date', 'i8'), ('time', 'i8'),
                            ('open_p', 'f8'), ('high_p', 'f8'),
                            ('low_p', 'f8'), ('close_p', 'f8'),
                            ('tot_vlm', 'u8'), ('prd_vlm', 'u8'),
                            ('num_trds', 'u8')])

    # Daily data is returned as a numpy array of this type.
    # Daily data means daily, weekly, monthly and annual data.
    daily_type = np.dtype(
            [('date', 'M8[D]'), ('open_p', 'f8'), ('high_p', 'f8'),
             ('low_p', 'f8'), ('close_p', 'f8'), ('prd_vlm', 'u8'),
             ('open_int', 'u8')])
    daily_h5_type = np.dtype(
            [('date', 'i8'), ('open_p', 'f8'), ('high_p', 'f8'),
             ('low_p', 'f8'), ('close_p', 'f8'), ('prd_vlm', 'u8'),
             ('open_int', 'u8')])

    # Private data structure
    _databuf = namedtuple("_databuf",
                          ['failed', 'err_msg', 'num_pts', 'raw_data'])

    def __init__(self, name: str = "HistoryConn", host: str = FeedConn.host,
                 port: int = port):
        super().__init__(name, host, port)
        self._set_message_mappings()
        self._req_num = 0
        self._req_buf = {}
        self._req_numlines = {}
        self._req_event = {}
        self._req_failed = {}
        self._req_err = {}
        self._req_lock = threading.RLock()
        self._req_num_lock = threading.RLock()

    def _set_message_mappings(self) -> None:
        """Set the message mappings"""
        super()._set_message_mappings()
        self._pf_dict['H'] = self._process_datum

    def _send_connect_message(self):
        """The lookup socket does not accept connect messages."""
        pass

    def _process_datum(self, fields: Sequence[str]) -> None:
        req_id = fields[0]
        if 'E' == fields[1]:
            # Error
            self._req_failed[req_id] = True
            err_msg = "Unknown Error"
            if len(fields) > 2:
                if fields[2] != "":
                    err_msg = fields[2]
            self._req_err[req_id] = err_msg
        elif '!ENDMSG!' == fields[1]:
            self._req_event[req_id].set()
        else:
            self._req_buf[req_id].append(fields)
            self._req_numlines[req_id] += 1

    def _get_next_req_id(self) -> str:
        with self._req_num_lock:
            req_id = "H_%.10d" % self._req_num
            self._req_num += 1
            return req_id

    def _cleanup_request_data(self, req_id: str) -> None:
        with self._req_lock:
            del self._req_failed[req_id]
            del self._req_err[req_id]
            del self._req_buf[req_id]
            del self._req_numlines[req_id]

    def _setup_request_data(self, req_id: str) -> None:
        """Setup empty buffers and other variables for a request."""
        with self._req_lock:
            self._req_buf[req_id] = deque()
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_err[req_id] = ""
            self._req_event[req_id] = threading.Event()

    def _get_data_buf(self, req_id: str) -> namedtuple:
        """Get the data buffer associated with a specific request."""
        with self._req_lock:
            buf = HistoryConn._databuf(
                failed=self._req_failed[req_id],
                err_msg=self._req_err[req_id],
                num_pts=self._req_numlines[req_id],
                raw_data=self._req_buf[req_id])
        self._cleanup_request_data(req_id)
        return buf

    def _read_ticks(self, req_id: str) -> np.array:
        """Get buffer for req_id and transform to a numpy array of ticks."""
        res = self._get_data_buf(req_id)
        if res.failed:
            return np.array([res.err_msg], dtype='object')
        else:
            data = np.empty(res.num_pts, HistoryConn.tick_type)
            line_num = 0
            while res.raw_data and (line_num < res.num_pts):
                dl = res.raw_data.popleft()
                (dt, tm) = fr.read_posix_ts_us(dl[1])
                data[line_num]['date'] = dt
                data[line_num]['time'] = tm
                data[line_num]['last'] = np.float64(dl[2])
                data[line_num]['last_sz'] = np.uint64(dl[3])
                data[line_num]['tot_vlm'] = np.uint64(dl[4])
                data[line_num]['bid'] = np.float64(dl[5])
                data[line_num]['ask'] = np.float64(dl[6])
                data[line_num]['tick_id'] = np.uint64(dl[7])
                data[line_num]['last_type'] = dl[8]
                data[line_num]['mkt_ctr'] = np.uint32(dl[9])

                cond_str = dl[10]
                num_cond = len(cond_str) / 2
                if num_cond > 0:
                    data[line_num]['cond1'] = np.uint8(int(cond_str[0:2], 16))
                else:
                    data[line_num]['cond1'] = 0

                if num_cond > 1:
                    data[line_num]['cond2'] = np.uint8(int(cond_str[2:4], 16))
                else:
                    data[line_num]['cond2'] = 0

                if num_cond > 2:
                    data[line_num]['cond3'] = np.uint8(int(cond_str[4:6], 16))
                else:
                    data[line_num]['cond3'] = 0

                if num_cond > 3:
                    data[line_num]['cond4'] = np.uint8(int(cond_str[6:8], 16))
                else:
                    data[line_num]['cond4'] = 0

                line_num += 1
                if line_num >= res.num_pts:
                    assert len(res.raw_data) == 0
                if len(res.raw_data) == 0:
                    assert line_num >= res.num_pts
            return data

    def request_ticks(self, ticker: str, max_ticks: int, ascend: bool = False,
                      timeout: int = None) -> np.array:
        """
        Request historical tickdata. Including upto the last second.

        :param ticker: Ticker symbol
        :param max_ticks: The most recent max_ticks trades.
        :param ascend: True means sorted oldest to latest, False opposite
        :param timeout: Wait for timeout seconds. Default None
        :return: A numpy array of dtype HistoryConn.tick_type

        HTX,[Symbol],[MaxDatapoints],[DataDirection],[RequestID],
        DatapointsPerSend]<CR><LF>

        """
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        pts_per_batch = min((max_ticks, 100))
        req_cmd = ("HTX,%s,%d,%d,%s,%d\r\n" % (
            ticker, max_ticks, ascend, req_id, pts_per_batch))
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_ticks(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            elif iqfeed_err == "Unauthorized user ID.":
                raise UnauthorizedError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    def request_ticks_for_days(self, ticker: str, num_days: int,
                               bgn_flt: datetime.time = None,
                               end_flt: datetime.time = None,
                               ascend: bool = False, max_ticks: int = None,
                               timeout: int = None) -> np.array:
        """
        Request tickdata for a certain number of days in the past.

        :param ticker: Ticker symbol
        :param num_days: Number of calendar days. 1 means today only.
        :param bgn_flt: Each day's data starting at bgn_flt
        :param end_flt: Each day's data no later than end_flt
        :param ascend: True means sorted oldest to latest, False opposite
        :param max_ticks: Only the most recent max_ticks trades. Default None
        :param timeout: Wait upto timeout seconds. Default None
        :return: A numpy array of dtype HistoryConn.tick_type

        HTD,[Symbol],[Days],[MaxDatapoints],[BeginFilterTime],[EndFilterTime],
        [DataDirection],[RequestID],[DatapointsPerSend]<CR><LF>

        """
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        bf_str = fr.time_to_hhmmss(bgn_flt)
        ef_str = fr.time_to_hhmmss(end_flt)
        mt_str = fr.blob_to_str(max_ticks)
        pts_per_batch = 100
        if max_ticks is not None:
            pts_per_batch = min((max_ticks, 100))
        req_cmd = ("HTD,%s,%d,%s,%s,%s,%d,%s,%d\r\n" % (
            ticker, num_days, mt_str, bf_str, ef_str, ascend, req_id,
            pts_per_batch))
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_ticks(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            elif iqfeed_err == "Unauthorized user ID.":
                raise UnauthorizedError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    def request_ticks_in_period(self, ticker: str, bgn_prd: datetime.datetime,
                                end_prd: datetime.datetime,
                                bgn_flt: datetime.time = None,
                                end_flt: datetime.time = None,
                                ascend: bool = False, max_ticks: int = None,
                                timeout: int = None) -> np.array:
        """
        Request tickdata in a certain period.

        :param ticker: Ticker symbol.
        :param bgn_prd: Start of the period.
        :param end_prd: End of the period.
        :param bgn_flt: Each day's data starting at bgn_flt
        :param end_flt: Each day's data no later than end_flt
        :param ascend: True means sorted oldest to latest, False opposite
        :param max_ticks: Only the most recent max_ticks trades. Default None
        :param timeout: Wait upto timeout seconds. Default None
        :return: A numpy array of dtype HistoryConn.tick_type

        HTT,[Symbol],[BeginDate BeginTime],[EndDate EndTime],[MaxDatapoints],
        [BeginFilterTime],[EndFilterTime],[DataDirection],[RequestID],
        [DatapointsPerSend]<CR><LF>

        """
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        bp_str = fr.datetime_to_yyyymmdd_hhmmss(bgn_prd)
        ep_str = fr.datetime_to_yyyymmdd_hhmmss(end_prd)
        bf_str = fr.time_to_hhmmss(bgn_flt)
        ef_str = fr.time_to_hhmmss(end_flt)
        mt_str = fr.blob_to_str(max_ticks)
        pts_per_batch = 100
        if max_ticks is not None:
            pts_per_batch = min((max_ticks, 100))
        req_cmd = ("HTT,%s,%s,%s,%s,%s,%s,%d,%s,%d\r\n" % (
            ticker, bp_str, ep_str, mt_str, bf_str, ef_str, ascend, req_id,
            pts_per_batch))
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_ticks(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            elif iqfeed_err == "Unauthorized user ID.":
                raise UnauthorizedError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    def _read_bars(self, req_id: str) -> np.array:
        """Get buffer for req_id and transform to a numpy array of bars."""
        res = self._get_data_buf(req_id)
        if res.failed:
            return np.array([res.err_msg], dtype='object')
        else:
            data = np.empty(res.num_pts, HistoryConn.bar_type)
            line_num = 0
            while res.raw_data and (line_num < res.num_pts):
                dl = res.raw_data.popleft()
                (dt, tm) = fr.read_posix_ts(dl[1])
                data[line_num]['date'] = dt
                data[line_num]['time'] = tm
                data[line_num]['high_p'] = np.float64(dl[2])
                data[line_num]['low_p'] = np.float64(dl[3])
                data[line_num]['open_p'] = np.float64(dl[4])
                data[line_num]['close_p'] = np.float64(dl[5])
                data[line_num]['tot_vlm'] = np.int64(dl[6])
                data[line_num]['prd_vlm'] = np.int64(dl[7])
                data[line_num]['num_trds'] = np.int64(dl[8])
                line_num += 1
                if line_num >= res.num_pts:
                    assert len(res.raw_data) == 0
                if len(res.raw_data) == 0:
                    assert line_num >= res.num_pts
            return data

    def request_bars(self, ticker: str, interval_len: int, interval_type: str,
                     max_bars: int, ascend: bool = False,
                     timeout: int = None) -> np.array:
        """
        Get max_bars number of bars of bar_data from IQFeed.

        :param ticker: Ticker symbol
        :param interval_len: Length of each bar interval in interval_type units
        :param interval_type: 's' = secs, 'v' = volume, 't' = ticks
        :param max_bars: Only the most recent max_bars bars. Default None
        :param ascend: True means oldest to latest, False opposite.
        :param timeout: Wait no more than timeout secs. Default None
        :return: A numpy array with dtype HistoryConn.bar_type

        If you use an interval type other than seconds, please make sure
        you understand what you are getting. The IQFeed docs
        don't explain. Support may help. Best is to get the tick-data and
        the bars and compare. In any event, bars other than
        seconds bars where interval_len % 60 == 0 are only
        available for the same period that tick-data is available for so
        you may be better off getting tick-data and creating your own bars.

        HIX,[Symbol],[Interval],[MaxDatapoints],[DataDirection],[RequestID],
        [DatapointsPerSend],[IntervalType]<CR><LF>

        """
        assert interval_type in ('s', 'v', 't')
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        bars_per_batch = min((100, max_bars))
        req_cmd = ("HIX,%s,%d,%d,%d,%s,%d,%s\r\n" % (
            ticker, interval_len, max_bars, ascend, req_id, bars_per_batch,
            interval_type))
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_bars(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            elif iqfeed_err == "Unauthorized user ID.":
                raise UnauthorizedError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    def request_bars_for_days(self, ticker: str, interval_len: int,
                              interval_type: str, days: int,
                              bgn_flt: datetime.time = None,
                              end_flt: datetime.time = None,
                              ascend: bool = False, max_bars: int = None,
                              timeout: int = None) -> np.array:
        """
        Get bars for the previous N days.

        :param ticker:  Ticker symbol
        :param interval_len: Length of each bar interval in interval_type units
        :param interval_type: 's' = secs, 'v' = volume, 't' = ticks
        :param days: Number of days to get bars for.
        :param bgn_flt: Each day's data starting at bgn_flt
        :param end_flt: Each day's data no later than end_flt
        :param ascend: True means latest to oldest, False opposite.
        :param max_bars: Only the most recent max_bars bars. Default None
        :param timeout: Wait no more than timeout secs. Default None
        :return: A numpy array with dtype HistoryConn.bar_type

        If you use an interval type other than seconds, please make sure
        you understand what you are getting. The IQFeed docs
        don't explain. Support may help. Best is to get the tick-data and
        the bars and compare. In any event, bars other than
        seconds bars where interval_len % 60 == 0 are only
        available for the same period that tick-data is available for so
        you may be better off getting tick-data and creating your own bars.

        HID,[Symbol],[Interval],[Days],[MaxDatapoints],[BeginFilterTime],
        [EndFilterTime],[DataDirection],[RequestID],[DatapointsPerSend],
        [IntervalType]<CR><LF>

        """
        assert interval_type in ('s', 'v', 't')
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        bf_str = fr.time_to_hhmmss(bgn_flt)
        ef_str = fr.time_to_hhmmss(end_flt)
        mb_str = fr.blob_to_str(max_bars)
        bars_per_batch = 100
        if max_bars is not None:
            bars_per_batch = min((100, max_bars))
        req_cmd = "HID,%s,%d,%d,%s,%s,%s,%d,%s,%d,%s\r\n" % (
            ticker, interval_len, days, mb_str, bf_str, ef_str, ascend, req_id,
            bars_per_batch, interval_type)
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_bars(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            elif iqfeed_err == "Unauthorized user ID.":
                raise UnauthorizedError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    def request_bars_in_period(self, ticker: str, interval_len: int,
                               interval_type: str, bgn_prd: datetime.datetime,
                               end_prd: datetime.datetime,
                               bgn_flt: datetime.time = None,
                               end_flt: datetime.time = None,
                               ascend: bool = False, max_bars: int = None,
                               timeout: int = None) -> np.array:
        """
        Get bars for a specific period.

        :param ticker:  Ticker symbol
        :param interval_len: Length of each bar interval in interval_type units
        :param interval_type: 's' = secs, 'v' = volume, 't' = ticks
        :param bgn_prd: Start of the period
        :param end_prd: End of the period
        :param bgn_flt: Each day's data starting at bgn_flt
        :param end_flt: Each day's data no later than end_flt
        :param ascend: True means oldest to latest, False opposite.
        :param max_bars: Only the most recent max_bars bars. Default None.
        :param timeout: Wait no more than timeout secs. Default None
        :return: A numpy array with dtype HistoryConn.bar_type

        If you use an interval type other than seconds, please make sure
        you understand what you are getting. The IQFeed docs
        don't explain. Support may help. Best is to get the tick-data and
        the bars and compare. In any event, bars other than
        seconds bars where interval_len % 60 == 0 are only
        available for the same period that tick-data is available for so
        you may be better off getting tick-data and creating your own bars.

        HIT,[Symbol],[Interval],[BeginDate BeginTime],[EndDate EndTime],
        [MaxDatapoints],[BeginFilterTime],[EndFilterTime],[DataDirection],
        [RequestID],[DatapointsPerSend],[IntervalType]<CR><LF>

        """
        assert interval_type in ('s', 'v', 't')
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        bp_str = fr.datetime_to_yyyymmdd_hhmmss(bgn_prd)
        ep_str = fr.datetime_to_yyyymmdd_hhmmss(end_prd)
        bf_str = fr.time_to_hhmmss(bgn_flt)
        ef_str = fr.time_to_hhmmss(end_flt)
        mb_str = fr.blob_to_str(max_bars)
        bars_per_batch = 100
        if max_bars is not None:
            bars_per_batch = min((100, max_bars))
        req_cmd = ("HIT,%s,%d,%s,%s,%s,%s,%s,%d,%s,%d,%s\r\n" % (
            ticker, interval_len, bp_str, ep_str, mb_str, bf_str, ef_str,
            ascend, req_id, bars_per_batch, interval_type))
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_bars(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            elif iqfeed_err == "Unauthorized user ID.":
                raise UnauthorizedError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    def _read_daily_data(self, req_id: str) -> np.array:
        """Get buffer for req_id and convert to a numpy array of daily data."""
        res = self._get_data_buf(req_id)
        if res.failed:
            return np.array([res.err_msg], dtype='object')
        else:
            data = np.empty(res.num_pts, HistoryConn.daily_type)
            line_num = 0
            while res.raw_data and (line_num < res.num_pts):
                dl = res.raw_data.popleft()
                data[line_num]['date'] = np.datetime64(dl[1], 'D')
                data[line_num]['high_p'] = np.float64(dl[2])
                data[line_num]['low_p'] = np.float64(dl[3])
                data[line_num]['open_p'] = np.float64(dl[4])
                data[line_num]['close_p'] = np.float64(dl[5])
                data[line_num]['prd_vlm'] = np.uint64(dl[6])
                data[line_num]['open_int'] = np.uint64(dl[7])
                line_num += 1
                if line_num >= res.num_pts:
                    assert len(res.raw_data) == 0
                if len(res.raw_data) == 0:
                    assert line_num >= res.num_pts
            return data

    def request_daily_data(self, ticker: str, num_days: int,
                           ascend: bool = False, timeout: int = None):
        """
        Request daily bars for the previous num_days.

        :param ticker: Symbol
        :param num_days: Number of days. 1 means today only.
        :param ascend: True means oldest data first, False opposite.
        :param timeout: Wait timeout seconds. Default None
        :return: A numpy array with dtype HistoryConn.daily_type

        HDX,[Symbol],[MaxDatapoints],[DataDirection],[RequestID],
        [DatapointsPerSend]<CR><LF>

        """
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        pts_per_batch = min((100, num_days))
        req_cmd = ("HDX,%s,%d,%d,%s,%d\r\n" % (
            ticker, num_days, ascend, req_id, pts_per_batch))
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_daily_data(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            elif iqfeed_err == "Unauthorized user ID.":
                raise UnauthorizedError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    def request_daily_data_for_dates(self, ticker: str, bgn_dt: datetime.date,
                                     end_dt: datetime.date,
                                     ascend: bool = False, max_days: int =
                                     None,
                                     timeout: int = None):
        """
        Request daily bars for a specific period.

        :param ticker: Symbol
        :param bgn_dt: Earliest Date
        :param end_dt: Latest DAte
        :param ascend: True means oldest data first, False opposite.
        :param max_days: Maximum number of days to get data for.
        :param timeout: Wait timeout seconds. Default None
        :return: A numpy array with dtype HistoryConn.daily_type

        HDT,[Symbol],[BeginDate],[EndDate],[MaxDatapoints],[DataDirection],
        [RequestID],[DatapointsPerSend]<CR><LF>

        """
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        bgn_str = fr.date_to_yyyymmdd(bgn_dt)
        end_str = fr.date_to_yyyymmdd(end_dt)
        md_str = fr.blob_to_str(max_days)
        pts_per_batch = 100
        if max_days is not None:
            pts_per_batch = min((100, max_days))
        req_cmd = ("HDT,%s,%s,%s,%s,%d,%s,%d\r\n" % (
            ticker, bgn_str, end_str, md_str, ascend, req_id, pts_per_batch))
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_daily_data(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            elif iqfeed_err == "Unauthorized user ID.":
                raise UnauthorizedError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    def request_weekly_data(self, ticker: str, num_weeks: int,
                            ascend: bool = False, timeout: int = None):
        """
        Request weekly bars for the last num_weeks.

        :param ticker: Symbol
        :param num_weeks: Number of weeks
        :param ascend: True means oldest data first, False opposite.
        :param timeout: Wait timeout seconds. Default None
        :return: A numpy array with dtype HistoryConn.daily_type

        HWX,[Symbol],[MaxDatapoints],[DataDirection],[RequestID],
        [DatapointsPerSend]<CR><LF>

        """
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        pts_per_batch = min((100, num_weeks))
        req_cmd = ("HWX,%s,%d,%d,%s,%d\r\n" % (
            ticker, num_weeks, ascend, req_id, pts_per_batch))
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_daily_data(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            elif iqfeed_err == "Unauthorized user ID.":
                raise UnauthorizedError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    def request_monthly_data(self, ticker: str, num_months: int,
                             ascend: bool = False, timeout: int = None):
        """
        Request monthly bars for the last num_months.

        :param ticker: Symbol
        :param num_months: Number of months.
        :param ascend: True means oldest data first, False opposite.
        :param timeout: Wait timeout seconds. Default None
        :return: A numpy array with dtype HistoryConn.daily_type

        HMX,[Symbol],[MaxDatapoints],[DataDirection],[RequestID],
        [DatapointsPerSend]<CR><LF>

        """
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        pts_per_batch = min((100, num_months))
        req_cmd = ("HMX,%s,%d,%d,%s,%d\r\n" % (
            ticker, num_months, ascend, req_id, pts_per_batch))
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_daily_data(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            elif iqfeed_err == "Unauthorized user ID.":
                raise UnauthorizedError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data


class TableConn(FeedConn):
    """
    TableConn is used to get type data from IQFeed's lookup socket.

    This class lets you query available market_types, security_types,
    trade_condition types, sic_codes and naic_codes from IQFeed.

    Since this data very rarely changes and is almost always needed, this
    class functions a little differently from other XXXConn classes. You
    construct this class and call update_tables. This starts the reader thread
    and gets data for all the type tables. Then you can query them to your
    heart's content and the cached values are returned. If you have reason
    to believe something has changed, or say you want to update your data
    once a day, call update_tables and then query the now updated values.

    This class derives from FeedConn and you can but do not have to
    receive basic IQFeed related messages by adding a listener. If things
    don't work as you expect, please do this and make sure you aren't
    receiving some messages that you are ignoring that are telling you
    why things aren't working.

    For more details see:
    www.iqfeed.net/dev/api/docs/SymbolLookupviaTCPIP.cfm

    """
    host = FeedConn.host
    port = FeedConn.lookup_port

    mkt_type = np.dtype(
            [('mkt_id', 'u8'), ('short_name', 'S16'), ('name', 'S128'),
             ('group_id', 'u8'), ('group', 'S128')])

    security_type = np.dtype(
            [('sec_type', 'u8'), ('short_name', 'S16'), ('name', 'S128')])

    tcond_type = np.dtype(
            [('tcond_id', 'u8'), ('short_name', 'S16'), ('name', 'S128')])

    sic_type = np.dtype([('sic', 'u8'), ('name', 'S128')])

    naic_type = np.dtype([('naic', 'u8'), ('name', 'S128')])

    def __init__(self, name: str = "TableConn", host: str = FeedConn.host,
                 port: int = port):
        super().__init__(name, host, port)

        self.markets = None
        self.security_types = None
        self.trade_conds = None
        self.sics = None
        self.naics = None

        self._current_deque = deque()
        self._current_event = threading.Event()
        self._lookup_done = False

        self._update_lock = threading.RLock()

    def _send_connect_message(self):
        # The lookup/history socket does not accept connect messages
        pass

    def _processing_function(self, fields: Sequence[str]):
        """Message processing function."""
        if fields[0].isdigit():
            return self._process_table_entry
        elif fields[0] == '!ENDMSG!':
            return self._process_table_end
        else:
            return super()._processing_function(fields)

    def _process_table_entry(self, fields: Sequence[str]) -> None:
        assert fields[0].isdigit()
        self._current_deque.append(fields)

    def _process_table_end(self, fields: Sequence[str]) -> None:
        assert fields[0] == "!ENDMSG!"
        self._current_event.set()

    def update_tables(self):
        """Update all the tables."""
        self.start_runner()
        with self._update_lock:
            self._update_markets()
            self._update_security_types()
            self._update_trade_conditions()
            self._update_sic_codes()
            self._update_naic_codes()
            self._lookup_done = True
        self.stop_runner()

    def get_markets(self):
        """Get the different markets."""
        if not self._lookup_done:
            raise RuntimeError("Update tables before requesting data")
        return self.markets

    def get_security_types(self):
        """Get security types (Stock/Option/Future etc)."""
        if not self._lookup_done:
            raise RuntimeError("Update tables before requesting data")
        return self.security_types

    def get_trade_conditions(self):
        """Get trade conditions."""
        if not self._lookup_done:
            raise RuntimeError("Update tables before requesting data")
        return self.trade_conds

    def get_sic_codes(self):
        """Get SIC codes (sector classification unrelated to NAIC codes)."""
        if not self._lookup_done:
            raise RuntimeError("Update tables before requesting data")
        return self.sics

    def get_naic_codes(self):
        """Get NAIC Codes (sector classification unrelated to SIC codes)."""
        if not self._lookup_done:
            raise RuntimeError("Update tables before requesting data")
        return self.naics

    def _update_markets(self):
        with self._update_lock:
            self._current_deque.clear()
            self._current_event.clear()
            self._send_cmd("SLM\r\n")
            self._current_event.wait(120)
            if self._current_event.is_set():
                num_pts = len(self._current_deque)
                self.markets = np.empty(num_pts, TableConn.mkt_type)
                line_num = 0
                while self._current_deque and (line_num < num_pts):
                    data_list = self._current_deque.popleft()
                    self.markets[line_num]['mkt_id'] = fr.read_uint64(
                            data_list[0])
                    self.markets[line_num]['short_name'] = data_list[1]
                    self.markets[line_num]['name'] = data_list[2]
                    self.markets[line_num]['group_id'] = fr.read_uint64(
                            data_list[3])
                    self.markets[line_num]['group'] = data_list[4]
                    line_num += 1
                    if line_num >= num_pts:
                        assert len(self._current_deque) == 0
                    if len(self._current_deque) == 0:
                        assert line_num >= num_pts
            else:
                raise RuntimeError("Update Market Types timed out")

    def _update_security_types(self):
        with self._update_lock:
            self._current_deque.clear()
            self._current_event.clear()
            self._send_cmd("SST\r\n")
            self._current_event.wait(120)
            if self._current_event.is_set():
                num_pts = len(self._current_deque)
                self.security_types = np.empty(num_pts, TableConn.security_type)
                line_num = 0
                while self._current_deque and (line_num < num_pts):
                    data_list = self._current_deque.popleft()
                    self.security_types[line_num]['sec_type'] = fr.read_uint64(
                            data_list[0])
                    self.security_types[line_num]['short_name'] = data_list[1]
                    self.security_types[line_num]['name'] = data_list[2]
                    line_num += 1
                    if line_num >= num_pts:
                        assert len(self._current_deque) == 0
                    if len(self._current_deque) == 0:
                        assert line_num >= num_pts
            else:
                raise RuntimeError("Update Security Types timed out")

    def _update_trade_conditions(self):
        with self._update_lock:
            self._current_deque.clear()
            self._current_event.clear()
            self._send_cmd("STC\r\n")
            self._current_event.wait(120)
            if self._current_event.is_set():
                num_pts = len(self._current_deque)
                self.trade_conds = np.empty(num_pts, TableConn.tcond_type)
                line_num = 0
                while self._current_deque and (line_num < num_pts):
                    data_list = self._current_deque.popleft()
                    self.trade_conds[line_num]['tcond_id'] = fr.read_uint64(
                            data_list[0])
                    self.trade_conds[line_num]['short_name'] = data_list[1]
                    self.trade_conds[line_num]['name'] = data_list[2]
                    line_num += 1
                    if line_num >= num_pts:
                        assert len(self._current_deque) == 0
                    if len(self._current_deque) == 0:
                        assert line_num >= num_pts
            else:
                raise RuntimeError("Update Trade Conditions timed out")

    def _update_sic_codes(self):
        with self._update_lock:
            self._current_deque.clear()
            self._current_event.clear()
            self._send_cmd("SSC\r\n")
            self._current_event.wait(120)
            if self._current_event.is_set():
                num_pts = len(self._current_deque)
                self.sics = np.empty(num_pts, TableConn.sic_type)
                line_num = 0
                while self._current_deque and (line_num < num_pts):
                    data_list = self._current_deque.popleft()
                    self.sics[line_num]['sic'] = fr.read_uint64(data_list[0])
                    self.sics[line_num]['name'] = ",".join(data_list[1:])
                    line_num += 1
                    if line_num >= num_pts:
                        assert len(self._current_deque) == 0
                    if len(self._current_deque) == 0:
                        assert line_num >= num_pts
            else:
                raise RuntimeError("Update SIC codes timed out")

    def _update_naic_codes(self):
        with self._update_lock:
            self._current_deque.clear()
            self._current_event.clear()
            self._send_cmd("SNC\r\n")
            self._current_event.wait(120)
            if self._current_event.is_set():
                num_pts = len(self._current_deque)
                self.naics = np.empty(num_pts, TableConn.naic_type)
                line_num = 0
                while self._current_deque and (line_num < num_pts):
                    data_list = self._current_deque.popleft()
                    self.naics[line_num]['naic'] = fr.read_uint64(data_list[0])
                    self.naics[line_num]['name'] = ",".join(data_list[1:])
                    line_num += 1
                    if line_num >= num_pts:
                        assert len(self._current_deque) == 0
                    if len(self._current_deque) == 0:
                        assert line_num >= num_pts
            else:
                raise RuntimeError("Update NAIC codes timed out")


class LookupConn(FeedConn):
    """
    LookupConn lets you look find symbols.

    You can find symbols of securities, options or futures
    based on the criteria you specify.

    Like HistoryConn the function called returns the data. Like HistoryConn
    you do receive messages from this class if you add a listener. These
    messages are about the connection itself and it's safe not to listen
    for them. If you are having trouble of some sort with this class, please
    first add a listener and see if one of the messages tells you something
    about why it's not working before assuming things don't work.

    """

    host = FeedConn.host
    port = FeedConn.lookup_port

    futures_month_letter_map = {1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K',
                                6: 'M', 7: 'N', 8: 'Q', 9: 'U', 10: 'V',
                                11: 'X', 12: 'Z'}
    futures_month_letters = (
            'F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z')

    call_month_letters = (
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L')
    call_month_letter_map = {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E',
                             6: 'F', 7: 'G', 8: 'H', 9: 'I', 10: 'J',
                             11: 'K', 12: 'L'}

    put_month_letters = (
            'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X')
    put_month_letter_map = {1: 'M', 2: 'N', 3: 'O', 4: 'P', 5: 'Q',
                            6: 'R', 7: 'S', 8: 'T', 9: 'U', 10: 'V',
                            11: 'W', 12: 'X'}

    asset_type = np.dtype(
            [('symbol', 'S128'), ('market', 'u1'), ('security_type', 'u1'),
             ('name', 'S128'), ('sector', 'u8')])

    _databuf = namedtuple("_databuf",
                          ['failed', 'err_msg', 'num_pts', 'raw_data'])

    def __init__(self, name: str = "SymbolSearchConn",
                 host: str = FeedConn.host, port: int = port):
        super().__init__(name, host, port)
        self._set_message_mappings()
        self._req_num = 0
        self._req_buf = {}
        self._req_numlines = {}
        self._req_event = {}
        self._req_failed = {}
        self._req_err = {}
        self._req_lock = threading.RLock()

    def _set_message_mappings(self) -> None:
        super()._set_message_mappings()
        self._pf_dict['L'] = self._process_lookup_datum

    def _send_connect_message(self):
        # The history/lookup socket does not accept connect messages
        pass

    def _process_lookup_datum(self, fields: Sequence[str]) -> None:
        req_id = fields[0]
        if 'E' == fields[1]:
            # Error
            self._req_failed[req_id] = True
            err_msg = "Unknown Error"
            if len(fields) > 2:
                if fields[2] != "":
                    err_msg = fields[2]
            self._req_err[req_id] = err_msg
        elif '!ENDMSG!' == fields[1]:
            self._req_event[req_id].set()
        else:
            self._req_buf[req_id].append(fields)
            self._req_numlines[req_id] += 1

    def _get_next_req_id(self) -> str:
        with self._req_lock:
            req_id = "L_%.10d" % self._req_num
            self._req_num += 1
            return req_id

    def _cleanup_request_data(self, req_id: str) -> None:
        with self._req_lock:
            del self._req_failed[req_id]
            del self._req_err[req_id]
            del self._req_buf[req_id]
            del self._req_numlines[req_id]

    def _setup_request_data(self, req_id: str) -> None:
        with self._req_lock:
            self._req_buf[req_id] = deque()
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_err[req_id] = ""
            self._req_event[req_id] = threading.Event()

    def _get_data_buf(self, req_id: str) -> namedtuple:
        """Get the data buffer for a specific request."""
        with self._req_lock:
            buf = LookupConn._databuf(
                    failed=self._req_failed[req_id],
                    err_msg=self._req_err[req_id],
                    num_pts=self._req_numlines[req_id],
                    raw_data=self._req_buf[req_id])
        self._cleanup_request_data(req_id)
        return buf

    def _read_symbols(self, req_id: str) -> np.array:
        """Get a data buffer and turn into np array of dtype asset_type."""
        res = self._get_data_buf(req_id)
        if res.failed:
            return np.array([res.err_msg], dtype='object')
        else:
            data = np.empty(res.num_pts, LookupConn.asset_type)
            line_num = 0
            while res.raw_data and (line_num < res.num_pts):
                dl = res.raw_data.popleft()
                data[line_num]['symbol'] = dl[1].strip()
                data[line_num]['market'] = fr.read_uint8(dl[2])
                data[line_num]['security_type'] = fr.read_uint8(dl[3])
                data[line_num]['name'] = dl[4].strip()
                data[line_num]['sector'] = 0
                line_num += 1
                if line_num >= res.num_pts:
                    assert len(res.raw_data) == 0
                if len(res.raw_data) == 0:
                    assert line_num >= res.num_pts
            return data

    def request_symbols_by_filter(self, search_term: str,
                                  search_field: str = 'd', filt_val: str =
                                  None,
                                  filt_type: str = None,
                                  timeout=None) -> np.array:
        """
        Search for symbols and return matches.

        :param search_term: 's': search symbols, 'd': search descriptions.
        :param search_field: What to search for.
        :param filt_val: 'e': Space delimited list of markets of security types.
        :param filt_type: Specific markets, 't': Specific security types.
        :param timeout: Must return before timeout or die, Default None
        :return: np.array of dtype LookupConn.asset_type

        SBF,[Field To Search],[Search String],[Filter Type],[Filter Value],
        [RequestID]<CR><LF>

        """
        assert search_field in ('d', 's')
        assert search_term is not None
        assert filt_type is None or filt_type in ('e', 't')

        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "SBF,%s,%s,%s,%s,%s\r\n" % (
            search_field, search_term, fr.blob_to_str(filt_type),
            fr.blob_to_str(filt_val), req_id)
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_symbols(req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def _read_symbols_with_sect(self, req_id: str) -> np.array:
        """Read symbols from buffer where sector field is not null."""
        res = self._get_data_buf(req_id)
        if res.failed:
            return np.array([res.err_msg], dtype='object')
        else:
            data = np.empty(res.num_pts, LookupConn.asset_type)
            line_num = 0
            while res.raw_data and (line_num < res.num_pts):
                dl = res.raw_data.popleft()
                data[line_num]['sector'] = fr.read_uint64(dl[1])
                data[line_num]['symbol'] = dl[2].strip()
                data[line_num]['market'] = fr.read_uint8(dl[3])
                data[line_num]['security_type'] = fr.read_uint8(dl[4])
                data[line_num]['name'] = dl[5].strip()
                line_num += 1
                if line_num >= res.num_pts:
                    assert len(res.raw_data) == 0
                if len(res.raw_data) == 0:
                    assert line_num >= res.num_pts
            return data

    def request_symbols_by_sic(self, sic: int, timeout=None) -> np.array:
        """
        Return symbols in a specific SIC sector.

        :param sic: SIC number for sector
        :param timeout: Wait timeout secs for data or die. Default None
        :return: np.array of dtype LookupConn.asset_type

        SBS,[Search String],[RequestID]<CR><LF>

        """
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "SBS,%d,%s\r\n" % (sic, req_id)
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_symbols_with_sect(req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_symbols_by_naic(self, naic: int, timeout=None) -> np.array:
        """
        Return symbols in a specific NAIC sector.

        :param naic: SIC number for sector
        :param timeout: Wait timeout secs for data or die. Default None
        :return: np.array of dtype LookupConn.asset_type

        SBN,[Search String],[RequestID]<CR><LF>

        """
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "SBS,%d,%s\r\n" % (naic, req_id)
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_symbols_with_sect(req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def _read_futures_chain(self, req_id: str) -> List[str]:
        """Read a buffer and return it as a futures chain."""
        res = self._get_data_buf(req_id)
        if res.failed:
            return ["!ERROR!", res.err_msg]
        else:
            assert res.num_pts == 1
            chain = res.raw_data[0][1:]
            if chain[-1] == "":
                chain = chain[:-1]
            return chain

    def request_futures_chain(self, symbol: str, month_codes: str = None,
                              years: str = None, near_months: int = None,
                              timeout: int = None) -> List[str]:
        """
        Request a futures chain
        :param symbol: Underlying symbol.
        :param month_codes: String containing month codes we want.
        :param years: Example: 2005 - 2014 would be "5678901234".
        :param near_months: Number of near months (ignore months and years).
        :param timeout: Die after timeout seconds, Default None.
        :return: List of futures tickers.

        CFU,[Symbol],[Month Codes],[Years],[Near Months],[RequestID]<CR><LF>

        """
        assert (symbol is not None) and (symbol != '')

        assert month_codes is None or near_months is None
        assert month_codes is not None or near_months is not None

        if month_codes is not None:
            # noinspection PyTypeChecker
            for month_code in month_codes:
                assert month_code in LookupConn.futures_month_letters

        if years is not None:
            assert years.isdigit()

        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "CFU,%s,%s,%s,%s,%s\r\n" % (
            symbol, fr.blob_to_str(month_codes), fr.blob_to_str(years),
            fr.blob_to_str(near_months), req_id)
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_futures_chain(req_id)
        if (len(data) == 2) and (data[0] == "!ERROR!"):
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[1]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_futures_spread_chain(
            self,
            symbol: str,
            month_codes: str = None,
            years: str = None,
            near_months: int = None,
            timeout: int = None) -> List[str]:
        """
        Request a chain of futures spreads
        :param symbol: Underlying symbol.
        :param month_codes: String containing month codes we want.
        :param years: Example: 2005 - 2014 would be "5678901234".
        :param near_months: Number of near months (ignore months and years).
        :param timeout: Die after timeout seconds, Default None.
        :return: List of futures spread tickers.

        CFS,[Symbol],[Month Codes],[Years],[Near Months],[RequestID]<CR><LF>

        """
        assert (symbol is not None) and (symbol != '')

        assert month_codes is None or near_months is None
        assert month_codes is not None or near_months is not None

        if month_codes is not None:
            # noinspection PyTypeChecker
            for month_code in month_codes:
                assert month_code in LookupConn.futures_month_letters

        if years is not None:
            assert years.isdigit()

        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "CFS,%s,%s,%s,%s,%s\r\n" % (
            symbol, fr.blob_to_str(month_codes), fr.blob_to_str(years),
            fr.blob_to_str(near_months), req_id)
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_futures_chain(req_id)
        if (len(data) == 2) and (data[0] == "!ERROR!"):
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[1]))
            raise RuntimeError(err_msg)
        else:
            return data

    def _read_option_chain(self, req_id: str):
        res = self._get_data_buf(req_id)
        if res.failed:
            return ["!ERROR!", res.err_msg]
        else:
            assert res.num_pts == 1
            symbols = res.raw_data[0][1:]
            cp_delim = symbols.index(':')
            call_symbols = symbols[:cp_delim]
            if len(call_symbols) > 0:
                if call_symbols[-1] == "":
                    call_symbols = call_symbols[:-1]
            put_symbols = symbols[cp_delim + 1:]
            if len(put_symbols) > 0:
                if put_symbols[-1] == "":
                    put_symbols = put_symbols[:-1]
            return {"c": call_symbols, "p": put_symbols}

    def request_futures_option_chain(self, symbol: str, opt_type: str = 'pc',
                                     month_codes: str = None, years: str =
                                     None,
                                     near_months: int = None,
                                     timeout: int = None) -> dict:
        """
        Request a chain of options on futures contracts.

        :param symbol: Underlying symbol of the futures contract.
        :param opt_type: 'p'=Puts, 'c'=Calls, 'pc'=Both
        :param month_codes: String of months you want
        :param years: Example: 2005 - 2014 would be "5678901234".
        :param near_months: Number of near months (ignore months and years)
        :param timeout: Die after timeout secs, Default None.
        :return: List of Options tickers
        CFO,[Symbol],[Puts/Calls],[Month Codes],[Years],[Near Months],
        [RequestID]<CR><LF>

        """
        assert (symbol is not None) and (symbol != '')

        assert opt_type is not None
        assert len(opt_type) in (1, 2)
        for op in opt_type:
            assert op in ('p', 'c')

        assert month_codes is None or near_months is None
        assert month_codes is not None or near_months is not None

        if month_codes is not None:
            valid_month_codes = ()
            if opt_type == 'p':
                valid_month_codes = LookupConn.put_month_letters
            elif opt_type == 'c':
                valid_month_codes = LookupConn.call_month_letters
            elif opt_type == 'cp' or opt_type == 'pc':
                valid_month_codes = (
                    LookupConn.call_month_letters +
                    LookupConn.put_month_letters)
            # noinspection PyTypeChecker
            for month_code in month_codes:
                assert month_code in valid_month_codes

        if years is not None:
            assert years.isdigit()

        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "CFO,%s,%s,%s,%s,%s,%s\r\n" % (
            symbol,
            opt_type,
            fr.blob_to_str(month_codes),
            fr.blob_to_str(years),
            fr.blob_to_str(near_months),
            req_id)
        logging.info("Req Futures Option Chain: %s" % req_cmd)
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_option_chain(req_id)
        if (type(data) == list) and (data[0] == "!ERROR!"):
            iqfeed_err = str(data[1])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == "!NO_DATA!":
                raise NoDataError(err_msg)
            elif iqfeed_err == "Unauthorized user ID.":
                raise UnauthorizedError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    def request_equity_option_chain(self, symbol: str, opt_type: str = 'pc',
                                    month_codes: str = None,
                                    near_months: int = None,
                                    include_binary: bool = True,
                                    filt_type: int = 0,
                                    filt_val_1: float = None,
                                    filt_val_2: float = None,
                                    timeout: int = None) -> dict:
        """
        Request a chain of options on an equity.

        :param symbol: Underlying symbol of the stock.
        :param opt_type: 'p'=Puts, 'c'=Calls, 'pc'=Both.
        :param month_codes: String of months you want.
        :param near_months: Number of near months (ignore months).
        :param include_binary: Include binary options.
        :param filt_type: 0=No filter 1=strike_range, 2=In/Out of money.
        :param filt_val_1: Lower strike or Num contracts in the money.
        :param filt_val_2: Upper string or Num Contracts out of the money.
        :param timeout: Die after timeout secs, Default None.
        :return: List of Options tickers.

        CEO,[Symbol],[Puts/Calls],[Month Codes],[Near Months],
        [BinaryOptions],[Filter Type],[Filter Value One],[Filter Value Two],
        [RequestID]<CR><LF>

        """
        assert (symbol is not None) and (symbol != '')

        assert opt_type is not None
        assert len(opt_type) in (1, 2)
        for op in opt_type:
            assert op in ('p', 'c')

        assert month_codes is None or near_months is None
        assert month_codes is not None or near_months is not None

        if month_codes is not None:
            valid_month_codes = ()
            if opt_type == 'p':
                valid_month_codes = LookupConn.put_month_letters
            elif opt_type == 'c':
                valid_month_codes = LookupConn.call_month_letters
            elif opt_type == 'cp' or opt_type == 'pc':
                valid_month_codes = (
                    LookupConn.call_month_letters +
                    LookupConn.put_month_letters)
            # noinspection PyTypeChecker
            for month_code in month_codes:
                assert month_code in valid_month_codes
        assert filt_type in (0, 1, 2)
        if filt_type != 0:
            assert filt_val_1 is not None and filt_val_1 > 0
            assert filt_val_2 is not None and filt_val_2 > 0
        if filt_type == 1:
            assert filt_val_1 < filt_val_2
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "CEO,%s,%s,%s,%s,%d,%d,%s,%s,%s\r\n" % (
            symbol, opt_type, fr.blob_to_str(month_codes),
            fr.blob_to_str(near_months), include_binary, filt_type,
            fr.blob_to_str(filt_val_1), fr.blob_to_str(filt_val_2), req_id)
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self._read_option_chain(req_id)
        if (type(data) == list) and (data[0] == "!ERROR!"):
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[1]))
            raise RuntimeError(err_msg)
        else:
            return data


class BarConn(FeedConn):
    """
    Let's you get live data as interval bar data.

    If you are using interval bars for trading, use this class if you want
    IQFeed to calculate the interval bars for you and send you interval bars
    instead of (or in addition to) receiving every tick. For example, you may
    want to get open, high, low, close data for each minute or every 50 trades.

    The length of the interval can be in time units, number of trades units
    or volume traded units as for bars from HistoryConn.

    If you want historical bars, use HistoryConn instead. This class
    allows you to get some history, for example if you want the past 5
    days's bars to fill in a data structure before you start getting live
    data updates. But if you want historical data for back-testing or some
    such, you are better off using HistoryConn instead.

    Since most historical data that IQFeed gives you is bar data, if you are
    just getting started, it may be a good idea to save some live tick-data and
    bar-data and compare them so you understand exactly how IQFeed is
    filtering ticks and generating it's bars. Different data providers tend to
    do this differently, dome better than others and the documentation usually
    doesn't get updated when things are changed.

    For more info, see:
    www.iqfeed.net/dev/api/docs/Derivatives_Overview.cfm
    and
    www.iqfeed.net/dev/api/docs/Derivatives_StreamingIntervalBars_TCPIP.cfm

    """
    host = FeedConn.host
    port = FeedConn.deriv_port

    interval_data_type = np.dtype(
            [('symbol', 'S64'), ('date', 'M8[D]'), ('time', 'u8'),
             ('open_p', 'f8'), ('high_p', 'f8'), ('low_p', 'f8'),
             ('close_p', 'f8'), ('tot_vlm', 'u8'), ('prd_vlm', 'u8'),
             ('num_trds', 'u8')])

    def __init__(self, name: str = "BarConn", host: str = host,
                 port: int = port):
        super().__init__(name, host, port)
        self._set_message_mappings()
        self._empty_interval_msg = np.zeros(1, dtype=BarConn.interval_data_type)

    def _set_message_mappings(self) -> None:
        super()._set_message_mappings()
        self._pf_dict['n'] = self._process_invalid_symbol
        self._pf_dict['B'] = self._process_bars
        self._sm_dict["REPLACED PREVIOUS WATCH"] = self._process_replaced_watch
        self._sm_dict[
            "SYMBOL LIMIT REACHED"] = self._process_symbol_limit_reached
        self._sm_dict["WATCHES"] = self._process_watch

    def _process_invalid_symbol(self, fields: Sequence[str]) -> None:
        """Called when a request is made with an invalid symbol."""
        assert len(fields) > 1
        assert fields[0] == 'n'
        bad_sym = fields[1]
        for listener in self._listeners:
            listener.process_invalid_symbol(bad_sym)

    def _process_replaced_watch(self, fields: Sequence[str]):
        """Called when a request supersedes an prior interval request."""
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'REPLACED PREVIOUS WATCH'
        symbol = fields[2]
        for listener in self._listeners:
            listener.process_replaced_previous_watch(symbol)

    def _process_symbol_limit_reached(self, fields: Sequence[str]) -> None:
        """Handle IQFeed telling us the symbol limit has been reached."""
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == "SYMBOL LIMIT REACHED"
        sym = fields[2]
        for listener in self._listeners:
            listener.process_symbol_limit_reached(sym)

    def _process_watch(self, fields: Sequence[str]) -> None:
        """Process a watches message."""
        assert len(fields) > 3
        assert fields[0] == 'S'
        assert fields[1] == 'WATCHES'
        symbol = fields[2]
        interval = fields[3]
        request_id = ""
        if len(fields) > 4:
            request_id = fields[4]
        for listener in self._listeners:
            listener.process_watch(symbol, interval, request_id)

    def _process_bars(self, fields: Sequence[str]):
        """Parse bar data and call appropriate callback."""
        assert len(fields) > 10
        assert fields[0][0] == "B" and fields[1][0] == "B"

        interval_data = self._empty_interval_msg
        interval_data['symbol'] = fields[2]
        interval_data['date'], interval_data['time'] = fr.read_posix_ts(
                fields[3])
        interval_data['open_p'] = np.float64(fields[4])
        interval_data['high_p'] = np.float64(fields[5])
        interval_data['low_p'] = np.float64(fields[6])
        interval_data['close_p'] = np.float64(fields[7])
        interval_data['tot_vlm'] = np.float64(fields[8])
        interval_data['prd_vlm'] = np.float64(fields[9])
        interval_data['num_trds'] = (
            np.float64(fields[10]) if fields[10] != "" else 0)

        bar_type = fields[1][1]
        if bar_type == 'U':
            for listener in self._listeners:
                listener.process_latest_bar_update(interval_data)
        elif bar_type == 'C':
            for listener in self._listeners:
                listener.process_live_bar(interval_data)
        elif bar_type == 'H':
            for listener in self._listeners:
                listener.process_history_bar(interval_data)
        else:
            raise UnexpectedField("Bad bar type in BarConn")

    def watch(self, symbol: str, interval_len: int, interval_type: str = None,
              bgn_flt: datetime.time = None, end_flt: datetime.time = None,
              update: int = None, bgn_bars: datetime.datetime = None,
              lookback_days: int = None, lookback_bars: int = None) -> None:
        """
        Request live interval (bar) data.

        :param symbol: Symbol for which you are requesting data.
        :param interval_len: Interval length in interval_type units
        :param interval_type: 's' = secs, 'v' = volume, 't' = ticks
        :param bgn_flt: Earliest time of day for which you want data
        :param end_flt: Latest time of day for which you want data
        :param update: Update the current bar every update secs.
        :param bgn_bars: Get back-fill bars starting at bgn_bars
        :param lookback_days: Get lookback_days of backfill data
        :param lookback_bars: Get lookback_bars of backfill data

        Only one of bgn_bars, lookback_days or lookback_bars should be set.

        Requests live interval data. You can also request some backfill data.
        When you call this function:
            1) The callback process_history_bar is called for backfill bars
            that go back either a) upto bgn_bars, b) upto lookback_days or
            c) upto lookback_bars.
            2) The callback process_latest_bar_update is called on every update
            to data for the current live bar.
            3) The callback process_live_bar is called every time we cross an
            interval boundary with data for the now complete bar.

        BW,[Symbol],[Interval],[BeginDate BeginTime],[MaxDaysOfDatapoints],
              [MaxDatapoints],[BeginFilterTime],[EndFilterTime],[RequestID],
              [Interval Type],[Reserved],[UpdateInterval]

        """
        assert interval_type in ('s', 'v', 't')
        bgn_bar_set = int(bgn_bars is not None)
        lookback_days_set = int(lookback_days is not None)
        lookback_bars_set = int(lookback_bars is not None)
        assert (bgn_bar_set + lookback_days_set + lookback_bars_set) < 2

        bgn_bar_str = fr.datetime_to_yyyymmdd_hhmmss(bgn_bars)
        lookback_days_str = fr.blob_to_str(lookback_days)
        lookback_bars_str = fr.blob_to_str(lookback_bars)
        bf_str = fr.time_to_hhmmss(bgn_flt)
        ef_str = fr.time_to_hhmmss(end_flt)
        update_str = fr.blob_to_str(update)

        request_id = "B-%s-%0.4d-%s" % (symbol, interval_len, interval_type)

        bar_cmd = "BW,%s,%s,%s,%s,%s,%s,%s,%s,%s,'',%s\r\n" % (
            symbol, interval_len, bgn_bar_str, lookback_days_str,
            lookback_bars_str,
            bf_str, ef_str, request_id, interval_type, update_str)
        self._send_cmd(bar_cmd)

    def unwatch(self, symbol: str):
        """Unwatch a specific symbol"""
        self._send_cmd("BR,%s" % symbol)

    def unwatch_all(self) -> None:
        """Unwatch all symbols."""
        self._send_cmd("S,UNWATCH ALL")

    def request_watches(self) -> None:
        """Request a list of all symbols we have subscribed."""
        self._send_cmd("S,REQUEST WATCHES\r\n")


class NewsConn(FeedConn):
    """
    NewsConn lets you do news lookups.

    If you want real time news headlines use QuoteConn.

    This class lets you get:
        a) News configuration so you know what news you can get.
        b) News headlines for a specific symbol or day etc
        c) Number of news stories for a specific symbol or day if you don't
        need the headlines but just the counts.
        c) The full story for a specific news headline.

    For more info, see:
    www.iqfeed.net/dev/api/docs/NewsLookupviaTCPIP.cfm

    """
    host = FeedConn.host
    port = FeedConn.lookup_port

    _databuf = namedtuple("_databuf",
                          ('failed', 'err_msg', 'num_pts', 'raw_data'))

    # Same as real-time news updates
    NewsMsg = namedtuple("NewsMsg",
                         ("story_id", "distributor", "symbol_list",
                          "story_date", "story_time", "headline"))

    NewsStoryMsg = namedtuple("NewsStoryMsg", ("story", "is_link"))

    NewsCountMsg = namedtuple("NewsCountMsg", ("symbol", "count"))

    def __init__(self, name: str = "NewsConn", host: str = FeedConn.host,
                 port: int = port):
        super().__init__(name, host, port)
        self._set_message_mappings()
        self._req_num = 0
        self._req_buf = {}
        self._req_numlines = {}
        self._req_event = {}
        self._req_failed = {}
        self._req_err = {}
        self._req_lock = threading.RLock()

    def _set_message_mappings(self) -> None:
        super()._set_message_mappings()
        self._pf_dict['N'] = self._process_news_datum

    def _send_connect_message(self):
        # The history/lookup socket does not accept connect messages
        pass

    def _process_news_datum(self, fields: Sequence[str]) -> None:
        req_id = fields[0]
        if 'E' == fields[1]:
            # Error
            self._req_failed[req_id] = True
            err_msg = "Unknown Error"
            if len(fields) > 2:
                if fields[2] != "":
                    err_msg = fields[2]
            self._req_err[req_id] = err_msg
        elif '!ENDMSG!' == fields[1]:
            self._req_event[req_id].set()
        else:
            self._req_buf[req_id].append(fields)
            self._req_numlines[req_id] += 1

    def _get_next_req_id(self) -> str:
        with self._req_lock:
            req_id = "N_%.10d" % self._req_num
            self._req_num += 1
            return req_id

    def _cleanup_request_data(self, req_id: str) -> None:
        with self._req_lock:
            del self._req_failed[req_id]
            del self._req_err[req_id]
            del self._req_buf[req_id]
            del self._req_numlines[req_id]

    def _setup_request_data(self, req_id: str) -> None:
        with self._req_lock:
            self._req_buf[req_id] = deque()
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_err[req_id] = ""
            self._req_event[req_id] = threading.Event()

    def _get_data_buf(self, req_id: str) -> namedtuple:
        with self._req_lock:
            buf = NewsConn._databuf(
                    failed=self._req_failed[req_id],
                    err_msg=self._req_err[req_id],
                    num_pts=self._req_numlines[req_id],
                    raw_data=self._req_buf[req_id])
        self._cleanup_request_data(req_id)
        return buf

    def _get_xml_message(self, req_id: str):
        """Convert a buffer into an XML ElementTree"""
        res = self._get_data_buf(req_id)
        if res.failed:
            return np.array([res.err_msg], dtype='object')
        else:
            raw_text = '\n'.join([''.join(line[1:]) for line in res.raw_data])
            return etree.fromstring(raw_text)

    def _create_config_structure(self, xml_data: etree.Element) -> dict:
        """Convert et.Element of configuration into nested list"""
        structure = xml_data.attrib
        structure["elem_type"] = xml_data.tag
        if len(xml_data) > 0:
            descendants = []
            for elem in xml_data:
                descendants.append(self._create_config_structure(elem))
            structure["sub_elems"] = descendants
        return structure

    def request_news_config(self, timeout: int = None) -> dict:
        """
        News Configuration request

        :param timeout: Die if o response in timeout secs
        :return: dict representing the configuration

        Returns the News configuration which tells you what news sources
        you are subscribed to.

        NCG,[XML/Text],[RequestID]<CR><LF>

        """
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)

        req_cmd = "NCG,x,%s\r\n" % req_id
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        xml_data = self._get_xml_message(req_id)
        if hasattr(xml_data, 'dtype'):
            if xml_data.dtype == object:
                err_msg = "Request: %s, Error: %s" % (req_cmd, str(xml_data[0]))
                raise RuntimeError(err_msg)
        return self._create_config_structure(xml_data)

    @staticmethod
    def _create_headline_list(xml_data: etree.Element) -> List[NewsMsg]:
        """Parse Headlines formatted as XML."""
        news_headlines = []
        for cur_headline in xml_data:
            story_id = None
            distributor = None
            symbol_list = []
            story_date = None
            story_time = None
            headline = None
            for item in cur_headline:
                if "id" == item.tag:
                    story_id = item.text
                elif "source" == item.tag:
                    distributor = item.text
                elif "symbols" == item.tag:
                    symbol_list = item.text.split(":")
                    if len(symbol_list) > 0:
                        symbol_list = [sym for sym in symbol_list if sym != '']
                elif "timestamp" == item.tag:
                    story_date, story_time = fr.read_hist_news_timestamp(
                            item.text)
                elif "text" == item.tag:
                    headline = item.text

            news_headlines.append(
                    NewsConn.NewsMsg(story_id=story_id,
                                     distributor=distributor,
                                     symbol_list=symbol_list,
                                     story_date=story_date,
                                     story_time=story_time,
                                     headline=headline))
        return news_headlines

    def request_news_headlines(self, sources: List[str] = None,
                               symbols: List[str] = None,
                               date: datetime.date = None, limit: int = 1000,
                               timeout: int = None) -> List[NewsMsg]:
        """
        Get all current news headlines.

        :param sources: Filter news sources to query. Default all sources.
        :param symbols: Filter symbols you want news for. Default all symbols.
        :param date: Filter News only for date. Default no date filter
        :param limit: Only limit stories. Default 1000
        :param timeout: Die if doesn't return in timeout secs.
        :return: A list of NewsMsg items. One for each story.

        Returns a list of NewsMsg. The elements of HeadlineMsg are:
          'distributor': News Source
          'story_id': ID of the story. Used to get full text
          'symbol_list': Symbols that are affected by the story
          'story_time': datetime the story went out
          'headline': The story's headline

        For the full text you need to use request_news_story or
        email_news_story.

        Note on passing in a date into this function:
            According to iqfeed docs news by date only works for
            "limited sources" All tests using date option thus far with iqfeed,
            failed to get any news. It's implemented here in case of better
            future support from DTN But if you use the date option, don't
            expect to get much/any news back.

        NHL,[Sources],[Symbols],[XML/Text],[Limit],[Date],[RequestID]<CR><LF>

        """
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)

        sources_str = ''
        if sources is not None:
            sources_str = ":".join(sources)

        symbols_str = ''
        if symbols is not None:
            symbols_str = ":".join(symbols)

        date_str = ''
        if date is not None:
            date_str = fr.date_to_yyyymmdd(date)

        req_cmd = "NHL,%s,%s,%s,%d,%s,%s\r\n" % (
            sources_str, symbols_str, 'x', limit, date_str, req_id)
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        xml_data = self._get_xml_message(req_id)
        if hasattr(xml_data, 'dtype'):
            if xml_data.dtype == object:
                err_msg = "Request: %s, Error: %s" % (req_cmd, str(xml_data[0]))
                raise RuntimeError(err_msg)
        return self._create_headline_list(xml_data)

    @staticmethod
    def _create_news_story(xml_data: etree.Element) -> NewsStoryMsg:
        """Convert news stories into NewsStoryMsgs."""
        is_link = 'N'
        story = None
        story_data = xml_data[0]
        for item in story_data:
            if "is_link" == item.tag:
                is_link = item.text
            elif 'story_text' == item.tag:
                story = item.text
        return NewsConn.NewsStoryMsg(is_link=is_link, story=story)

    def request_news_story(self, story_id: str,
                           timeout: int = None) -> NewsStoryMsg:
        """
        Request the news story corresponding to a story id.

        :param story_id: Story id
        :param timeout: Die if waiting > timeout secs.
        :return: Body of story as a NewsStoryMsg.

        NewsStoryMsg is a namedtuple which contains
        is_link: Is the story linked
        story: Text of the story

        NSY,[ID],[XML/Text/Email],[DeliverTo],[RequestID]<CR><LF>

        """
        assert story_id is not None
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)

        req_cmd = "NSY,%s,x,,%s\r\n" % (story_id, req_id)
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        xml_data = self._get_xml_message(req_id)
        if hasattr(xml_data, 'dtype'):
            if xml_data.dtype == object:
                err_msg = "Request: %s, Error: %s" % (req_cmd, str(xml_data[0]))
                raise RuntimeError(err_msg)
        return self._create_news_story(xml_data)

    def email_news_story(self, story_id: str, address: str) -> None:
        """
        Email the news story corresponding to a story id.

        :param story_id: Story id
        :param address: Email address you want the story emailed to

        The full text of the story is emailed to the specified address.

        NSY,[ID],[XML/Text/Email],[DeliverTo],[RequestID]<CR><LF>

        """
        assert story_id is not None
        assert address is not None

        req_cmd = "NSY,%s,e,%s,\r\n" % (story_id, address)
        self._send_cmd(req_cmd)

    @staticmethod
    def _create_story_counts(xml_data: etree.Element) -> List[NewsCountMsg]:
        """Parse story counts and return as NewsCountMsg."""
        story_counts = []
        for count_data in xml_data:
            story_counts.append(
                    NewsConn.NewsCountMsg(
                            symbol=count_data.attrib['Name'],
                            count=int(count_data.attrib['StoryCount'])))
        return story_counts

    def request_story_counts(self, symbols: List[str],
                             sources: List[str] = None,
                             bgn_dt: datetime.date = None,
                             end_dt: datetime.date = None,
                             timeout: int = None) -> List[NewsCountMsg]:
        """
        Request News Story Counts.

        :param symbols:  Request counts for these syms. List of String.
        :param sources:  Filter to sources. List of String
        :param bgn_dt:   Filter to after bgn_dt
        :param end_dt:   Filter to before bgn_dt
        :param timeout:  Die if can't complete in timeout secs
        :return: A list of NewsCountMsgs

        NSC,[Symbols],[XML/Text],[Sources],[DateRange],[RequestID]<CR><LF>

        """
        symbols_str = ":".join(symbols)
        sources_str = ""
        if sources is not None:
            sources_str = ":".join(sources)

        date_strings = []
        if bgn_dt is not None:
            date_strings.append(fr.date_to_yyyymmdd(bgn_dt))
        if end_dt is not None:
            date_strings.append(fr.date_to_yyyymmdd(end_dt))
        date_range_str = "-".join(date_strings)

        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)

        req_cmd = "NSC,%s,x,%s,%s,%s\r\n" % (
            symbols_str, sources_str, date_range_str, req_id)
        self._send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        xml_data = self._get_xml_message(req_id)
        if hasattr(xml_data, 'dtype'):
            if xml_data.dtype == object:
                err_msg = "Request: %s, Error: %s" % (req_cmd, str(xml_data[0]))
                raise RuntimeError(err_msg)
        return self._create_story_counts(xml_data)
