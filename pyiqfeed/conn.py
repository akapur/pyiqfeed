import datetime
import itertools
import select
import socket
import threading
import time
from collections import deque, namedtuple
from typing import Sequence, List, Callable, Tuple
from .exceptions import NoDataError

import numpy as np


def blob_to_str(val) -> str:
    if val is None:
        return ""
    else:
        return str(val)


def read_market_open(field: str) -> bool:
    bool(int(field)) if field != "" else False


def read_short_restricted(field: str) -> bool:
    if field != "":
        if field == 'Y':
            return True
        if field == 'N':
            return False
        else:
            raise RuntimeError(
                "Unknown Value in Short Restricted Field: %s" % field)
    else:
        return False


def read_tick_direction(field: str) -> np.int8:
    if field != "":
        field_as_int = int(field)
        if field_as_int == 173:
            return 1
        if field_as_int == 175:
            return -1
        if field_as_int == 183:
            return 0
        else:
            raise RuntimeError(
                "Unknown value in Tick Direction Field: %s" % field)
    else:
        return 0


def read_int(field: str) -> int:
    return int(field) if field != "" else 0


def read_hex(field: str) -> int:
    return int(field, 16) if field != "" else 0


def read_uint8(field: str) -> np.uint8:
    return np.uint8(field) if field != "" else 0


def read_uint16(field: str) -> np.uint16:
    return np.uint16(field) if field != "" else 0


def read_uint64(field: str) -> np.uint64:
    return np.uint64(field) if field != "" else 0


def read_float(field: str) -> float:
    return float(field) if field != "" else float('nan')


def read_float64(field: str) -> np.float64:
    return np.float64(field) if field != "" else np.nan


def read_split_string(split_str: str) -> tuple:
    split_fld_0, split_fld_1 = ("", "")
    if split_str != "":
        (split_fld_0, split_fld_1) = split_str.split(' ')
    split_factor = read_float64(split_fld_0)
    split_date = read_mmddccyy(split_fld_1)
    split_data = (split_factor, split_date)
    return split_data


def read_hhmmss(field: str) -> int:
    if field != "":
        hour = int(field[0:2])
        minute = int(field[3:5])
        second = int(field[6:8])
        msecs_since_midnight = 1000000 * ((3600*hour) + (60*minute) + second)
        return msecs_since_midnight
    else:
        return 0


def read_hhmmssmil(field: str) -> int:
    if field != "":
        hour = int(field[0:2])
        minute = int(field[3:5])
        second = int(field[6:8])
        msecs = int(field[9:])
        msecs_since_midnight = (1000000 *
                                ((3600*hour) + (60*minute) + second)) + msecs
        return msecs_since_midnight
    else:
        return 0


# noinspection PyUnresolvedReferences
def read_mmddccyy(field: str) -> np.datetime64:
    if field != "":
        month = int(field[0:2])
        day = int(field[3:5])
        year = int(field[6:10])
        return np.datetime64(
            datetime.date(year=year, month=month, day=day), 'D')
    else:
        return np.datetime64(datetime.date(year=1, month=1, day=1), 'D')


# noinspection PyUnresolvedReferences,PyUnresolvedReferences
def read_ccyymmdd(field: str) -> np.datetime64:
    if field != "":
        year = int(field[0:4])
        month = int(field[4:6])
        day = int(field[6:8])
        return np.datetime64(
            datetime.date(year=year, month=month, day=day), 'D')
    else:
        return np.datetime64(datetime.date(year=1, month=1, day=1), 'D')


# noinspection PyUnresolvedReferences
def read_dtn_timestamp(dt_tm: str) -> Tuple[np.datetime64, int]:
    if dt_tm != "":
        (date_str, time_str) = dt_tm.split(' ')
        dt = read_ccyymmdd(date_str)
        tm = read_hhmmss(time_str)
        return dt, tm
    else:
        return np.datetime64(datetime.date(year=1, month=1, day=1), 'D'), 0


# noinspection PyUnresolvedReferences
def read_yyyymmdd_hhmmss(dt_tm: str) -> Tuple[datetime.date, int]:
    if dt_tm != "":
        (date_str, time_str) = dt_tm.split(' ')
        year = read_int(date_str[0:4])
        month = read_int(date_str[4:6])
        day = read_int(date_str[6:8])
        dt = np.datetime64(datetime.date(year=year, month=month, day=day), 'D')

        hour = read_int(time_str[0:2])
        minute = read_int(time_str[2:4])
        second = read_int(time_str[4:6])
        msecs_since_midnight = 1000000 * ((3600*hour) + (60*minute) + second)
        return dt, msecs_since_midnight
    else:
        return np.datetime64(datetime.date(year=1, month=1, day=1), 'D'), 0


# noinspection PyUnresolvedReferences
def read_posix_ts_mil(dt_tm_str: str) -> Tuple[np.datetime64, int]:
    if dt_tm_str != "":
        (date_str, time_str) = dt_tm_str.split(" ")
        dt = np.datetime64(date_str, 'D')
        tm = read_hhmmssmil(time_str)
        return dt, tm
    else:
        return np.datetime64(datetime.date(year=1, month=1, day=1), 'D'), 0


# noinspection PyUnresolvedReferences
def read_posix_ts(dt_tm_str: str) -> Tuple[np.datetime64, int]:
    if dt_tm_str != "":
        (date_str, time_str) = dt_tm_str.split(" ")
        dt = np.datetime64(date_str, 'D')
        tm = read_hhmmss(time_str)
        return dt, tm
    else:
        return np.datetime64(datetime.date(year=1, month=1, day=1), 'D'), 0


def str_or_blank(val) -> str:
    if val is not None:
        return str(val)
    else:
        return ""


def ms_since_midnight_to_time(ms: int) -> datetime.time:
    assert ms >= 0
    assert ms <= 86400000000
    secs_since_midnight = np.floor(ms/1000000.0)
    hour = np.floor(secs_since_midnight/3600)
    minute = np.floor((secs_since_midnight-(hour*3600))/60)
    second = secs_since_midnight - (hour*3600) - (minute*60)
    return datetime.time(hour=int(hour), minute=int(minute), second=int(second))


def time_to_hhmmss(tm: datetime.time) -> str:
    if tm is not None:
        return "%.2d%.2d%.2d" % (tm.hour, tm.minute, tm.second)
    else:
        return ""


# noinspection PyUnresolvedReferences
def datetime64_to_date(dt64: np.datetime64) -> datetime.date:
    return dt64.astype(datetime.date)


def date_to_yyyymmdd(dt: datetime.date) -> str:
    if dt is not None:
        return "%.4d%.2d%.2d" % (dt.year, dt.month, dt.day)
    else:
        return ""


# noinspection PyUnresolvedReferences
def date_ms_to_datetime(dt64: np.datetime64, tm_int: int) -> datetime.datetime:
    dt = datetime64_to_date(dt64)
    tm = ms_since_midnight_to_time(tm_int)
    return datetime.datetime(year=dt.year, month=dt.month, day=dt.day,
                             hour=tm.hour, minute=tm.minute, second=tm.second)


def datetime_to_yyyymmdd_hhmmss(dt_tm: datetime.datetime) -> str:
    if dt_tm is not None:
        # noinspection PyPep8
        return "%.4d%.2d%.2d %.2d%.2d%.2d" % (dt_tm.year, dt_tm.month, dt_tm.day,
                                              dt_tm.hour, dt_tm.minute,
                                              dt_tm.second)
    else:
        return ""


class FeedConn:

    host = "127.0.0.1"
    quote_port = 5009
    lookup_port = 9100
    depth_port = 9200
    admin_conn = 9300
    deriv_port = 9400
    port = quote_port
    protocol = "5.2"

    def __init__(self, name: str, host: str, port: int):
        self._stop = False
        self._started = False
        self._connected = False
        self._reconnect_failed = False
        self._pf_dict = {}
        self._sm_dict = {}
        self._listeners = []
        self._buf_lock = threading.RLock()
        self._send_lock = threading.RLock()
        self._recv_buf = ""
        self._host = host
        self._port = port
        self._name = name
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._read_thread = threading.Thread(group=None, target=self,
                                             name="%s-reader" % self._name,
                                             args=(),
                                             kwargs={}, daemon=True)
        self._set_message_mappings()
        self.connect(host, port)

    def __del__(self):
        self.disconnect()

    def send_cmd(self, cmd: str) -> None:
        with self._send_lock:
            self._sock.sendall(cmd.encode(encoding='utf-8', errors='strict'))

    def connect(self, host, port) -> None:
        self._host = host
        self._port = port
        self._sock.connect((host, port))
        self._set_protocol(FeedConn.protocol)
        self._set_client_name(self._name)
        self._send_connect_message()

    def disconnect(self) -> None:
        self.stop_runner()
        self._sock.close()

    def start_runner(self) -> None:
        self._stop = False
        if not self._started:
            self._read_thread.start()
            self._started = True

    def stop_runner(self) -> None:
        self._stop = True
        if self._started:
            self._read_thread.join(30)
            self._started = False

    def running(self) -> bool:
        return self._read_thread.is_alive()

    def connected(self) -> bool:
        return self._connected

    def reconnect_failed(self) -> bool:
        return self._reconnect_failed

    def __call__(self):
        try:
            while not self._stop:
                if self.read_messages():
                    self.process_messages()
        finally:
            self._started = False

    def _set_message_mappings(self) -> None:
        self._pf_dict['E'] = self.process_error
        self._pf_dict['T'] = self.process_timestamp
        self._pf_dict['S'] = self.process_system_message

        self._sm_dict["SERVER DISCONNECTED"] = self.process_server_disconnected
        self._sm_dict["SERVER CONNECTED"] = self.process_server_connected
        self._sm_dict["SERVER RECONNECT FAILED"] = self.process_reconnect_failed
        self._sm_dict["CURRENT PROTOCOL"] = self.process_current_protocol
        self._sm_dict["STATS"] = self.process_conn_stats

    def read_messages(self) -> bool:
        ready_list = select.select([self._sock], [], [self._sock], 5)
        if ready_list[2]:
            raise RuntimeError(
                "There was a problem with the socket for QuoteReader: %s," %
                self._name)
        if ready_list[0]:
            with self._buf_lock:
                data_recvd = self._sock.recv(16384).decode()
                self._recv_buf += data_recvd
                return True
        return False

    def next_message(self) -> str:
        with self._buf_lock:
            next_delim = self._recv_buf.find('\n')
            if next_delim != -1:
                message = self._recv_buf[:next_delim].strip()
                self._recv_buf = self._recv_buf[(next_delim + 1):]
                return message
            else:
                return ""

    def process_messages(self) -> None:
        with self._buf_lock:
            message = self.next_message()
            while "" != message:
                fields = message.split(',')
                handle_func = self.processing_function(fields)
                handle_func(fields)
                message = self.next_message()

    def processing_function(self, fields) -> Callable[[Sequence[str]], None]:
        pf = self._pf_dict.get(fields[0][0])
        if pf is not None:
            return pf
        else:
            return self.process_unregistered_message

    # noinspection PyMethodMayBeStatic
    def process_unregistered_message(self, fields: Sequence[str]) -> None:
        raise RuntimeError("Unexpected message received: %s", ",".join(fields))

    def process_system_message(self, fields: Sequence[str]) -> None:
        assert len(fields) > 1
        assert fields[0] == "S"
        processing_func = self.system_processing_function(fields)
        processing_func(fields)

    def system_processing_function(self, fields) -> Callable[[Sequence[str]],
                                                             None]:
        assert len(fields) > 1
        assert fields[0] == "S"
        spf = self._sm_dict.get(fields[1])
        if spf is not None:
            return spf
        else:
            return self.process_unregistered_system_message

    # noinspection PyMethodMayBeStatic
    def process_unregistered_system_message(self,
                                            fields: Sequence[str]) -> None:
        raise RuntimeError("Unexpected system message received: %s", ",".join(
            fields))

    # noinspection PyMethodMayBeStatic
    def process_current_protocol(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == "S"
        assert fields[1] == "CURRENT PROTOCOL"
        protocol = fields[2]
        if protocol != FeedConn.protocol:
            raise RuntimeError("Desired Protocol %s, Server Says Protocol %s" %
                               (FeedConn.protocol, protocol))

    def process_server_disconnected(self, fields: Sequence[str]) -> None:
        assert len(fields) > 1
        assert fields[0] == "S"
        assert fields[1] == "SERVER DISCONNECTED"
        self._connected = False
        for listener in self._listeners:
            listener.feed_is_stale()

    def process_server_connected(self, fields: Sequence[str]) -> None:
        assert len(fields) > 1
        assert fields[0] == "S"
        assert fields[1] == "SERVER CONNECTED"
        self._connected = True
        for listener in self._listeners:
            listener.feed_is_fresh()

    def process_reconnect_failed(self, fields: Sequence[str]) -> None:
        assert len(fields) > 1
        assert fields[0] == "S"
        assert fields[1] == "SERVER RECONNECT FAILED"
        self._reconnect_failed = True
        self._connected = False
        for listener in self._listeners:
            listener.feed_is_stale()
            listener.feed_has_error()

    def process_conn_stats(self, fields: Sequence[str]) -> None:
        assert len(fields) > 20
        assert fields[0] == "S"
        assert fields[1] == "STATS"
        conn_stats = {"server_ip": fields[2], "server_port": int(fields[3]),
                      "max_sym": int(fields[4]), "num_sym": int(fields[5]),
                      "num_clients": int(fields[6]),
                      "secs_since_update": int(fields[7]),
                      "num_recon": int(fields[8]),
                      "num_fail_recon": int(fields[9]),
                      "conn_tm":  time.strptime(fields[10], "%b %d %I:%M%p"),
                      "mkt_tm": time.strptime(fields[11], "%b %d %I:%M%p"),
                      "status": (fields[12] == "Connected"),
                      "feed_version": fields[13], "login": fields[14],
                      "kbs_recv": float(fields[15]),
                      "kbps_recv": float(fields[16]),
                      "avg_kbps_recv": float(fields[17]),
                      "kbs_sent": float(fields[18]),
                      "kbps_sent": float(fields[19]),
                      "avg_kbps_sent": float(fields[20])}
        for listener in self._listeners:
            listener.process_conn_stats(conn_stats)

    def process_timestamp(self, fields: Sequence[str]) -> None:
        # T,[YYYYMMDD HH:MM:SS]
        assert fields[0] == "T"
        assert len(fields) > 1
        dt_tm_tuple = read_dtn_timestamp(fields[1])
        for listener in self._listeners:
            listener.process_timestamp(dt_tm_tuple)

    def process_error(self, fields: Sequence[str]) -> None:
        assert fields[0] == "E"
        assert len(fields) > 1
        for listener in self._listeners:
            listener.process_error(fields)

    def add_listener(self, listener) -> None:
        if listener not in self._listeners:
            self._listeners.append(listener)

    def remove_listener(self, listener) -> None:
        if listener in self._listeners:
            self._listeners.remove(listener)

    def _set_protocol(self, protocol) -> None:
        self.send_cmd("S,SET PROTOCOL,%s\r\n" % protocol)

    def _send_connect_message(self) -> None:
        msg = "S,CONNECT\r\n"
        self.send_cmd(msg)

    def send_disconnect_message(self) -> None:
        self.send_cmd("S,DISCONNECT\r\n")

    def _set_client_name(self, name) -> None:
        self._name = name
        msg = "S,SET CLIENT NAME,%s\r\n" % name
        self.send_cmd(msg)


class QuoteConn(FeedConn):
    port = 5009

    regional_type = np.dtype([('Symbol', 'S64'),
                              ('Regional Bid', 'f8'),
                              ('Regional BidSize', 'u8'),
                              ('Regional BidTime', 'u8'),
                              ('Regional Ask', 'f8'),
                              ('Regional AskSize', 'u8'),
                              ('Regional AskTime', 'u8'),
                              ('Market Center', 'u1'),
                              ('Fraction Display Code', 'u1'),
                              ('Decimal Precision', 'u2')])

    fundamental_fields = ["Symbol", "Exchange ID", "PE", "Average Volume",
                          "52 Week High", "52 Week Low",
                          "Calendar Year High", "Calendar Year Low",
                          "Dividend Yield", "Dividend Amount", "Dividend Rate",
                          "Pay Date", "Ex-dividend Date",
                          "(Reserved)", "(Reserved)", "(Reserved)",
                          "Short Interest", "(Reserved)",
                          "Current Year EPS", "Next Year EPS",
                          "Five-year Growth Percentage", "Fiscal Year End",
                          "(Reserved)", "Company Name",
                          "Root Option Symbol", "Percent Held By Institutions",
                          "Beta", "Leaps",
                          "Current Assets", "Current Liabilities",
                          "Balance Sheet Date", "Long-term Debt",
                          "Common Shares Outstanding", "(Reserved)",
                          "Split Factor 1", "Split Factor 2",
                          "(Reserved)", "Market Center", "Format Code",
                          "Precision", "SIC",
                          "Historical Volatility", "Security Type",
                          "Listed Market",
                          "52 Week High Date", "52 Week Low Date",
                          "Calendar Year High Date", "Calendar Year Low Date",
                          "Year End Close", "Maturity Date", "Coupon Rate",
                          "Expiration Date",
                          "Strike Price", "NAICS", "Exchange Root"]

    fundamental_type = [('Symbol', 'S128'),
                        ('PE', 'f8'),
                        ('Average Volume', 'f8'),
                        ('52 Week High', 'f8'), ('52 Week Low', 'f8'),
                        ('Calendar Year High', 'f8'),
                        ('Calendar Year Low', 'f8'),
                        ('Dividend Yield', 'f8'), ('Dividend Amount', 'f8'),
                        ('Dividend Rate', 'f8'),
                        ('Pay Date', 'M8[D]'), ('Ex-dividend Date', 'M8[D]'),
                        ('Short Interest', 'i8'),
                        ('Current Year EPS', 'f8'), ('Next Year EPS', 'f8'),
                        ('Five-year Growth Percentage', 'f8'),
                        ('Fiscal Year End', 'u1'),
                        ('Company Name', 'S256'),
                        ('Root Option Symbol', 'S256'),
                        ('Percent Held By Institutions', 'f8'),
                        ('Beta', 'f8'), ('Leaps', 'S128'),
                        ('Current Assets', 'f8'),
                        ('Current Liabilities', 'f8'),
                        ('Balance Sheet Date', 'M8[D]'),
                        ('Long-term Debt', 'f8'),
                        ('Common Shares Outstanding', 'f8'),
                        ('Split Factor 1 Date', 'M8[D]'),
                        ('Split Factor 1', 'f8'),
                        ('Split Factor 2 Date', 'M8[D]'),
                        ('Split Factor 2', 'f8'),
                        ('Format Code', 'u1'), ('Precision', 'u1'),
                        ('SIC', 'u8'),
                        ('Historical Volatility', 'f8'),
                        ('Security Type', 'u1'), ('Listed Market', 'u1'),
                        ('52 Week High Date', 'M8[D]'),
                        ('52 Week Low Date', 'M8[D]'),
                        ('Calendar Year High Date', 'M8[D]'),
                        ('Calendar Year Low Date', 'M8[D]'),
                        ('Year End Close', 'f8'), ('Maturity Date', 'M8[D]'),
                        ('Coupon Rate', 'f8'), ('Expiration Date', 'M8[D]'),
                        ('Strike Price', 'f8'),
                        ('NAICS', 'u8'),
                        ('Exchange Root', 'S128')]

    # noinspection PyPep8
    quote_msg_map = {'Symbol': ('Symbol', 'S128', lambda x: x),
                     '7 Day Yield': ('7 Day Yield', 'f8', read_float64),
                     'Ask': ('Ask', 'f8', read_float64),
                     'Ask Change': ('Ask Change', 'f8', read_float64),
                     'Ask Market Center':
                         ('Ask Market Center', 'u1', read_uint8),
                     'Ask Size': ('Ask Size', 'u8', read_uint64),
                     'Ask Time': ('Ask Time', 'u8', read_hhmmssmil),
                     'Available Regions':
                         ('Available Regions', 'S128', lambda x: x),
                         # TODO: Parse
                     'Average Maturity':
                         ('Average Maturity', 'f8', read_float64),
                     'Bid': ('Bid', 'f8', read_float64),
                     'Bid Change': ('Bid Change', 'f8', read_float64),
                     'Bid Market Center':
                         ('Bid Market Center', 'u1', read_uint8),
                     'Bid Size': ('Bid Size', 'u8', read_uint64),
                     'Bid Time': ('Bid Time', 'u8', read_hhmmssmil),
                     'Change': ('Change', 'f8', read_float64),
                     'Change From Open':
                         ('Change From Open', 'f8', read_float64),
                     'Close': ('Close', 'f8', read_float64),
                     'Close Range 1': ('Close Range 1', 'f8', read_float64),
                     'Close Range 2': ('Close Range 2', 'f8', read_float64),
                     'Days to Expiration':
                         ('Days to Expiration', 'u2', read_uint16),
                     'Decimal Precision':
                         ('Decimal Precision', 'u1', read_uint8),
                     'Delay': ('Delay', 'u1', read_uint8),
                     'Exchange ID': ('Exchange ID', 'u1', read_hex),
                     'Extended Trade': ('Extended Price',  'f8', read_float64),
                     'Extended Trade Date':
                         ('Extended Trade Date', 'M8[D]', read_mmddccyy),
                     'Extended Trade Market Center':
                         ('Extended Trade Market Center', 'u1', read_uint8),
                     'Extended Trade Size':
                         ('Extended Trade Size', 'u8', read_uint64),
                     'Extended Trade Time':
                         ('Extended Trade Time', 'u8', read_hhmmssmil),
                     'Extended Trading Change':
                         ('Extended Trading Change', 'f8', read_float64),
                     'Extended Trading Difference':
                         ('Extended Trading Difference', 'f8', read_float64),
                     'Financial Status Indicator':
                         ('Financial Status Indicator', 'S1', lambda x: x),
                         # TODO: Parse
                     'Fraction Display Code':
                         ('Fraction Display Code', 'u1', read_uint8),
                     'High': ('High', 'f8', read_float64),
                     'Last': ('Last', 'f8', read_float64),
                     'Last Date': ('Last Date', 'M8[D]', read_mmddccyy),
                     'Last Market Center':
                         ('Last Market Center', 'u1', read_uint8),
                     'Last Size': ('Last Size', 'u8', read_uint64),
                     'Last Time': ('Last Time', 'u8', read_hhmmssmil),
                     'Low': ('Low', 'f8', read_float64),
                     'Market Capitalization':
                         ('Market Capitalization', 'f8', read_float64),
                     'Market Open': ('Market Open', 'b1', read_market_open),
                     'Message Contents':
                         ('Message Contents', 'S9', lambda x: x),  # TODO: Parse
                     'Most Recent Trade':
                         ('Most Recent Trade', 'f8', read_float64),
                     'Most Recent Trade Conditions':
                         ('Most Recent Trade Conditions', 'S16', lambda x: x),
                         # TODO: Parse
                     'Most Recent Trade Date':
                         ('Most Recent Trade Date', 'M8[D]', read_mmddccyy),
                     'Most Recent Trade Market Center':
                         ('Most Recent Trade Market Center', 'u1', read_uint8),
                     'Most Recent Trade Size':
                         ('Most Recent Trade Size', 'u8', read_uint64),
                     'Most Recent Trade Time':
                         ('Most Recent Trade Time', 'u8', read_hhmmssmil),
                     'Net Asset Value': ('Net Asset Value', 'f8', read_float64),
                     'Number of Trades Today':
                         ('Number of Trades Today', 'u8', read_uint64),
                     'Open': ('Open', 'f8', read_float64),
                     'Open Interest': ('Open Interest', 'u8', read_uint64),
                     'Open Range 1': ('Open Range 1', 'f8', read_float64),
                     'Open Range 2': ('Open Range 2', 'f8', read_float64),
                     'Percent Change': ('Percent Change', 'f8', read_float64),
                     'Percent Off Average Volume':
                         ('Percent Off Average Volume', 'f8', read_float64),
                     'Previous Day Volume':
                         ('Previous Day Volume', 'u8', read_uint64),
                     'Price-Earnings Ratio':
                         ('Price-Earnings Ratio', 'f8', read_float64),
                     'Range': ('Range', 'f8', read_float64),
                     'Restricted Code':
                         ('Restricted Code', 'b1', read_short_restricted),
                     'Settle': ('Settle', 'f8', read_float64),
                     'Settlement Date':
                         ('Settlement Date', 'M8[D]', read_mmddccyy),
                     'Spread': ('Spread', 'f8', read_float64),
                     'Tick': ('Tick', 'i8', read_tick_direction),
                     'TickID': ('TickId', 'u8', read_uint64),
                     'Total Volume': ('Total Volume', 'u8', read_uint64),
                     'Volatility': ('Volatility', 'f8', read_float64),
                     'VWAP': ('VWAP', 'f8', read_float64)}

    def __init__(self, name: str = "QuoteConn", host: str = FeedConn.host,
                 port: int = port):
        super().__init__(name, host, port)
        self._current_update_fields = []
        self._update_names = []
        self._update_dtype = []
        self._update_reader = []
        self._set_message_mappings()
        self._current_update_fields = ["Symbol",
                                       "Most Recent Trade",
                                       "Most Recent Trade Size",
                                       "Most Recent Trade Time",
                                       "Most Recent Trade Market Center",
                                       "Total Volume",
                                       "Bid", "Bid Size", "Ask", "Ask Size",
                                       "Open", "High", "Low", "Close",
                                       "Message Contents",
                                       "Most Recent Trade Conditions"]
        self._num_update_fields = len(self._current_update_fields)
        self._set_current_update_structs(self._current_update_fields)
        self.request_fundamental_fieldnames()
        self.request_all_update_fieldnames()
        self.request_current_update_fieldnames()

    def _set_message_mappings(self) -> None:
        super()._set_message_mappings()
        self._pf_dict['n'] = self.process_invalid_symbol
        self._pf_dict['N'] = self.process_news
        self._pf_dict['R'] = self.process_regional_quote
        self._pf_dict['P'] = self.process_summary
        self._pf_dict['Q'] = self.process_update
        self._pf_dict['F'] = self.process_fundamentals

        self._sm_dict["KEY"] = self.process_auth_key
        self._sm_dict["KEYOK"] = self.process_keyok
        self._sm_dict["CUST"] = self.process_customer_info
        self._sm_dict["SYMBOL LIMIT REACHED"] =\
            self.process_symbol_limit_reached
        self._sm_dict["IP"] = self.process_ip_addresses_used
        self._sm_dict["FUNDAMENTAL FIELDNAMES"] =\
            self.process_fundamental_fieldnames
        self._sm_dict["UPDATE FIELDNAMES"] =\
            self.process_update_fieldnames
        self._sm_dict["CURRENT UPDATE FIELDNAMES"] =\
            self.process_current_update_fieldnames

    def process_invalid_symbol(self, fields: Sequence[str]) -> None:
        assert len(fields) > 1
        assert fields[0] == 'n'
        bad_sym = fields[1]
        for listener in self._listeners:
            listener.process_invalid_symbol(bad_sym)

    def process_news(self, fields: Sequence[str]):
        assert len(fields) > 5
        assert fields[0] == "N"
        distributor = fields[1]
        story_id = fields[2]
        symbol_list = fields[3].split(":")
        story_time = read_yyyymmdd_hhmmss(fields[4])
        headline = fields[5]
        news_dict = {"distributor": distributor,
                     "story_id": story_id,
                     "symbol_list": symbol_list,
                     "story_time": story_time,
                     "headline": headline}
        for listener in self._listeners:
            listener.process_news(news_dict)

    def process_regional_quote(self, fields: Sequence[str]):
        assert len(fields) > 11
        assert fields[0] == "R"
        rgn_quote = np.empty(shape=1, dtype=QuoteConn.regional_type)
        rgn_quote["Symbol"] = fields[1]
        rgn_quote["Regional Bid"] = read_float64(fields[3])
        rgn_quote["Regional BidSize"] = read_uint64(fields[4])
        rgn_quote["Regional BidTime"] = read_hhmmss(fields[5])
        rgn_quote["Regional Ask"] = read_float64(fields[6])
        rgn_quote["Regional AskSize"] = read_uint64(fields[7])
        rgn_quote["Regional AskTime"] = read_hhmmss(fields[8])
        rgn_quote["Fraction Display Code"] = read_uint8(fields[9])
        rgn_quote["Decimal Precision"] = read_uint8(fields[10])
        rgn_quote["Market Center"] = read_uint8(fields[11])
        for listener in self._listeners:
            listener.process_regional_rgn_quote(rgn_quote)

    def process_summary(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == "P"
        update = self.create_update(fields)
        for listener in self._listeners:
            listener.process_summary(update)

    def process_update(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == "Q"
        update = self.create_update(fields)
        for listener in self._listeners:
            listener.process_update(update)

    def create_update(self, fields: Sequence[str]) -> np.array:
        update = np.empty(1, self._update_dtype)
        for field_num, field in enumerate(fields[1:]):
            if field_num >= self._num_update_fields and field == "":
                break
            update[self._update_names[field_num]] =\
                self._update_reader[field_num](field)
        return update

    def process_fundamentals(self, fields: Sequence[str]):
        assert len(fields) > 55
        assert fields[0] == 'F'
        msg = np.zeros(1, dtype=QuoteConn.fundamental_type)

        msg['Symbol'] = fields[1]
        msg['PE'] = read_float64(fields[3])
        msg['Average Volume'] = read_uint64(fields[4])
        msg['52 Week High'] = read_float64(fields[5])
        msg['52 Week Low'] = read_float64(fields[6])
        msg['Calendar Year High'] = read_float64(fields[7])
        msg['Calendar Year Low'] = read_float64(fields[8])
        msg['Dividend Yield'] = read_float64(fields[9])
        msg['Dividend Amount'] = read_float64(fields[10])
        msg['Dividend Rate'] = read_float64(fields[11])
        msg['Pay Date'] = read_mmddccyy(fields[12])
        msg['Ex-dividend Date'] = read_mmddccyy(fields[13])
        msg['Short Interest'] = read_uint64(fields[17])
        msg['Current Year EPS'] = read_float64(fields[19])
        msg['Next Year EPS'] = read_float64(fields[20])
        msg['Five-year Growth Percentage'] = read_float64(fields[21])
        msg['Fiscal Year End'] = read_uint8(fields[22])
        msg['Company Name'] = fields[24]
        msg['Root Option Symbol'] = fields[25]    # TODO:Parse
        msg['Percent Held By Institutions'] = read_float64(fields[26])
        msg['Beta'] = read_float64(fields[27])
        msg['Leaps'] = fields[28]  # TODO: Parse
        msg['Current Assets'] = read_float64(fields[29])
        msg['Current Liabilities'] = read_float64(fields[30])
        msg['Balance Sheet Date'] = read_mmddccyy(fields[31])
        msg['Long-term Debt'] = read_float64(fields[32])
        msg['Common Shares Outstanding'] = read_float64(fields[33])
        (fact, dt) = read_split_string(fields[35])
        msg['Split Factor 1 Date'] = dt
        msg['Split Factor 1'] = fact
        (fact, dt) = read_split_string(fields[36])
        msg['Split Factor 2 Date'] = dt
        msg['Split Factor 2'] = fact
        msg['Format Code'] = read_uint8(fields[39])
        msg['Precision'] = read_uint8(fields[40])
        msg['SIC'] = read_uint64(fields[41])
        msg['Historical Volatility'] = read_float64(fields[42])
        msg['Security Type'] = read_int(fields[43])
        msg['Listed Market'] = read_uint8(fields[44])
        msg['52 Week High Date'] = read_mmddccyy(fields[45])
        msg['52 Week Low Date'] = read_mmddccyy(fields[46])
        msg['Calendar Year High Date'] = read_mmddccyy(fields[47])
        msg['Calendar Year Low Date'] = read_mmddccyy(fields[48])
        msg['Year End Close'] = read_float64(fields[49])
        msg['Maturity Date'] = read_mmddccyy(fields[50])
        msg['Coupon Rate'] = read_float64(fields[51])
        msg['Expiration Date'] = read_mmddccyy(fields[52])
        msg['Strike Price'] = read_float64(fields[53])
        msg['NAICS'] = read_uint8(fields[54])
        msg['Exchange Root'] = fields[55]
        for listener in self._listeners:
            listener.process_fundamentals(msg)

    def process_auth_key(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == "S"
        assert fields[1] == "KEY"
        auth_key = fields[2]
        for listener in self._listeners:
            listener.process_auth_key(auth_key)

    def process_keyok(self, fields: Sequence[str]) -> None:
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == "KEYOK"
        for listener in self._listeners:
            listener.process_keyok()

    def process_customer_info(self, fields: Sequence[str]) -> None:
        assert len(fields) > 11
        assert fields[0] == 'S'
        assert fields[1] == "CUST"
        msg_dict = {"svc_t": (fields[2] == "real_time"),
                    "ip_add": fields[3], "port": int(fields[4]),
                    "token": fields[5], "version": fields[6],
                    "rt_exchs": fields[8].split(" "),
                    "max_sym": int(fields[10]),
                    "flags": fields[11]}
        for listener in self._listeners:
            listener.process_customer_info(msg_dict)

    def process_symbol_limit_reached(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == "SYMBOL LIMIT REACHED"
        sym = fields[2]
        for listener in self._listeners:
            listener.process_symbol_limit_reached(sym)

    def process_ip_addresses_used(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'IP'
        ip = fields[2]
        for listener in self._listeners:
            listener.process_ip_addresses_used(ip)

    # noinspection PyMethodMayBeStatic
    def process_fundamental_fieldnames(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'FUNDAMENTAL FIELDNAMES'
        for field in fields[2:]:
            if field not in QuoteConn.fundamental_fields:
                raise RuntimeError(
                    "%s not found in QuoteConn.dtn_fundamental_fields" % field)
        for field in QuoteConn.fundamental_fields:
            if field not in fields[2:]:
                raise RuntimeError(
                    "%s not found in FUNDAMENTAL FIELDNAMES message" % field)

    # noinspection PyMethodMayBeStatic
    def process_update_fieldnames(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'UPDATE FIELDNAMES'
        for field in fields[2:]:
            if field not in QuoteConn.quote_msg_map:
                raise RuntimeError(
                    "%s not found in QuoteConn.dtn_update_map" % field)
        for field in QuoteConn.quote_msg_map:
            if field not in fields[2:]:
                raise RuntimeError(
                    "%s not found in UPDATE FIELDNAMES message" % field)

    def process_current_update_fieldnames(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'CURRENT UPDATE FIELDNAMES'
        self._set_current_update_structs(fields[2:])

    def _set_current_update_structs(self, fields):
        num_update_fields = len(fields)
        new_update_fields = list(itertools.repeat("", num_update_fields))
        new_update_names = new_update_fields
        new_update_dtypes = list(itertools.repeat(("no_name", 'i8'),
                                                  num_update_fields))
        new_update_reader = list(itertools.repeat(
            lambda x: x, num_update_fields))
        for field_num, field in enumerate(fields):
            if field not in QuoteConn.quote_msg_map:
                raise RuntimeError("%s not in QuoteConn.dtn_update_map" % field)
            new_update_fields[field_num] = field
            dtn_update_tup = QuoteConn.quote_msg_map[field]
            new_update_names[field_num] = dtn_update_tup[0]
            new_update_dtypes[field_num] = (dtn_update_tup[0],
                                            dtn_update_tup[1])
            new_update_reader[field_num] = dtn_update_tup[2]
        self._current_update_fields = new_update_fields
        self._update_names = new_update_names
        self._update_dtype = new_update_dtypes
        self._update_reader = new_update_reader
        self._num_update_fields = len(new_update_fields)

    def req_timestamp(self) -> None:
        self.send_cmd("T\r\n")

    def timestamp_on(self) -> None:
        self.send_cmd("S,TIMESTAMPSON\r\n")

    def timestamp_off(self) -> None:
        self.send_cmd("S,TIMESTAMPSOFF\r\n")

    def trades_watch(self, symbol: str) -> None:
        self.send_cmd("t%s\r\n" % symbol)

    def watch(self, symbol: str) -> None:
        self.send_cmd("w%s\r\n" % symbol)

    def unwatch(self, symbol: str) -> None:
        self.send_cmd("r%s\r\n" % symbol)

    def watch_regional(self, symbol: str) -> None:
        self.send_cmd("S,REGON,%s\r\n" % symbol)

    def unwatch_regional(self, symbol: str) -> None:
        self.send_cmd("S,REGOFF,%s\r\n" % symbol)

    def refresh(self, symbol: str) -> None:
        self.send_cmd("f%s\r\n" % symbol)

    def request_watches(self) -> None:
        self.send_cmd("S,REQUEST WATCHES\r\n")

    def unwatch_all(self) -> None:
        self.send_cmd("S,UNWATCH ALL")

    def news_on(self) -> None:
        self.send_cmd("S,NEWSON\r\n")

    def news_off(self) -> None:
        self.send_cmd("S,NEWSOFF\r\n")

    def request_stats(self) -> None:
        self.send_cmd("S,REQUEST STATS\r\n")

    def request_fundamental_fieldnames(self) -> None:
        self.send_cmd("S,REQUEST FUNDAMENTAL FIELDNAMES\r\n")

    def request_all_update_fieldnames(self) -> None:
        self.send_cmd("S,REQUEST ALL UPDATE FIELDNAMES\r\n")

    def request_current_update_fieldnames(self) -> None:
        self.send_cmd("S,REQUEST CURRENT UPDATE FIELDNAMES\r\n")

    def select_update_fieldnames(self, field_names: List[str]) -> None:
        symbol_field = "Symbol"
        if symbol_field not in field_names:
            field_names.insert(0, symbol_field)
        else:
            symbol_idx = field_names.index("Symbol")
            if symbol_idx != 0:
                field_names[0], field_names[symbol_idx] =\
                    field_names[symbol_idx], field_names[0]
        self.send_cmd("S,SELECT UPDATE FIELDS,%s\r\n" % ",".join(field_names))

    def set_log_levels(self, log_levels: Sequence[str]) -> None:
        self.send_cmd("S,SET LOG LEVELS,%s\r\n" % ",".join(log_levels))


class AdminConn(FeedConn):
    port = 9300
    host = "127.0.0.1"

    def __init__(self, name: str ="AdminConn",
                 host: str = host, port: int = port):
        super().__init__(name, host, port)
        self._set_message_mappings()

    def _set_message_mappings(self) -> None:
        super()._set_message_mappings()
        self._sm_dict["REGISTER CLIENT APP COMPLETED"] =\
            self.process_register_client_app_completed
        self._sm_dict["REMOVE CLIENT APP COMPLETED"] =\
            self.process_remove_client_app_completed
        self._sm_dict["CURRENT LOGINID"] = self.process_current_login
        self._sm_dict["CURRENT PASSWORD"] = self.process_current_password
        self._sm_dict["LOGIN INFO SAVED"] = self.process_login_info_saved
        self._sm_dict["LOGIN INFO NOT SAVED"] =\
            self.process_login_info_not_saved
        self._sm_dict["AUTOCONNECT ON"] = self.process_autoconnect_on
        self._sm_dict["AUTOCONNECT OFF"] = self.process_autoconnect_off
        self._sm_dict["CLIENTSTATS"] = self.process_client_stats

    def process_register_client_app_completed(self,
                                              fields: Sequence[str]) -> None:
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == 'REGISTER CLIENT APP COMPLETED'
        for listener in self._listeners:
            listener.process_register_client_app_completed()

    def process_remove_client_app_completed(self,
                                            fields: Sequence[str]) -> None:
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == 'REMOVE CLIENT APP COMPLETED'
        for listener in self._listeners:
            listener.process_remove_client_app_completed()

    def process_current_login(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'CURRENT LOGINID'
        login_id = fields[2]
        for listener in self._listeners:
            listener.process_current_login(login_id)

    def process_current_password(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'CURRENT PASSWORD'
        password = fields[2]
        for listener in self._listeners:
            listener.process_current_password(password)

    def process_login_info_saved(self, fields: Sequence[str]) -> None:
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == 'LOGIN INFO SAVED'
        for listener in self._listeners:
            listener.process_login_info_saved()

    def process_login_info_not_saved(self, fields: Sequence[str]) -> None:
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == 'LOGIN INFO NOT SAVED'
        for listener in self._listeners:
            listener.process_login_info_not_saved()

    def process_autoconnect_on(self, fields: Sequence[str]) -> None:
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == 'AUTOCONNECT ON'
        for listener in self._listeners:
            listener.process_autoconnect_on()

    def process_autoconnect_off(self, fields: Sequence[str]) -> None:
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == 'AUTOCONNECT OFF'
        for listener in self._listeners:
            listener.process_autoconnect_off()

    def process_client_stats(self, fields: Sequence[str]) -> None:
        assert len(fields) > 10
        assert fields[0] == 'S'
        assert fields[1] == 'CLIENTSTATS'

        type_int = read_int(fields[2])
        client_id = read_int(fields[3])
        client_name = fields[4]
        (start_date, start_tm) = read_yyyymmdd_hhmmss(fields[5])
        num_sym = read_int(fields[6])
        num_reg_sym = read_int(fields[7])
        kb_sent = read_float(fields[8])
        kb_recvd = read_float(fields[9])
        kb_queued = read_float(fields[10])

        client_type = "Unknown"
        if 0 == type_int:
            client_type = "Admin"
        elif 1 == type_int:
            client_type = "Quote"
        elif 2 == type_int:
            client_type = "Depth"
        elif 3 == type_int:
            client_type = "Lookup"

        client_stats_dict = {"client_type": client_type, "client_id": client_id,
                             "client_name": client_name,
                             "start_dt": start_date, "start_tm": start_tm,
                             "kb_sent": kb_sent, "kb_recvd": kb_recvd,
                             "kb_queued": kb_queued}
        if 1 == type_int:
            client_stats_dict["num_quote_subs"] = num_sym
            client_stats_dict["num_reg_subs"] = num_reg_sym
        elif 2 == type_int:
            client_stats_dict["num_depth_subs"] = num_sym
        for listener in self._listeners:
            listener.process_client_stats(client_stats_dict)

    def register_client_app(self, product: str) -> None:
        self.send_cmd("S,REGISTER CLIENT APP,%s\r\n" % product)

    def remove_client_app(self, product: str) -> None:
        self.send_cmd("S REMOVE CLIENT APP,%s\r\n" % product)

    def set_login(self, login: str) -> None:
        self.send_cmd("S,SET LOGINID,%s\r\n" % login)

    def set_password(self, password: str) -> None:
        self.send_cmd("S,SET PASSWORD,%s\r\n" % password)

    def set_autoconnect(self, autoconnect: bool) -> None:
        if autoconnect:
            self.send_cmd("S,SET AUTOCONNECT,On\r\n")
        else:
            self.send_cmd("S,SET AUTOCONNECT,Off\r\n")

    def client_stats_on(self) -> None:
        self.send_cmd("S,CLIENTSTATS ON\r\n")

    def client_stats_off(self) -> None:
        self.send_cmd("S,CLIENTSTATS OFF\r\n")

    def set_admin_variables(self,
                            product: str,
                            login: str,
                            password: str,
                            autoconnect: bool = True) -> None:
        self.register_client_app(product)
        self.set_login(login)
        self.set_password(password)
        self.set_autoconnect(autoconnect)

    def set_admin_variables_from_dict(self, avd: dict) -> None:
        self.set_admin_variables(product=avd["product"],
                                 login=avd["login"],
                                 password=avd["password"],
                                 autoconnect=avd["autoconnect"])


class HistoryConn(FeedConn):
    port = 9100

    tick_type = np.dtype([('tick_id', 'u8'),
                          ('date', 'M8[D]'),
                          ('time', 'u8'),
                          ('last', 'f8'),
                          ('last_sz', 'u8'),
                          ('last_type', 'S1'),
                          ('mkt_ctr', 'u4'),
                          ('tot_vlm', 'u8'),
                          ('bid', 'f8'),
                          ('ask', 'f8'),
                          ('cond1', 'u1'),
                          ('cond2', 'u1'),
                          ('cond3', 'u1'),
                          ('cond4', 'u1')])

    bar_type = np.dtype([('date', 'M8[D]'),
                         ('time', 'u8'),
                         ('open_p', 'f8'),
                         ('high_p', 'f8'),
                         ('low_p', 'f8'),
                         ('close_p', 'f8'),
                         ('tot_vlm', 'u8'),
                         ('prd_vlm', 'u8'),
                         ('num_trds', 'u8')])

    daily_type = np.dtype([('date', 'M8[D]'),
                           ('open_p', 'f8'),
                           ('high_p', 'f8'),
                           ('low_p', 'f8'),
                           ('close_p', 'f8'),
                           ('prd_vlm', 'u8'),
                           ('open_int', 'u8')])

    _databuf = namedtuple("_databuf",
                          ['failed', 'err_msg', 'num_pts', 'raw_data'])

    def __init__(self, name: str = "HistoryConn",
                 host: str = FeedConn.host, port: int = port):
        super().__init__(name, host, port)
        self._set_message_mappings()
        self._req_num = 0
        self._req_buf = {}
        self._req_numlines = {}
        self._req_event = {}
        self._req_failed = {}
        self._req_err = {}
        self._buf_lock = threading.RLock()

    def _set_message_mappings(self) -> None:
        super()._set_message_mappings()
        self._pf_dict['H'] = self.process_datum

    def _send_connect_message(self):
        # The history socket does not accept connect messages
        pass

    def process_datum(self, fields: Sequence[str]) -> None:
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
        with self._buf_lock:
            req_id = "H_%.10d" % self._req_num
            self._req_num += 1
            return req_id

    def _cleanup_request_data(self, req_id: str) -> None:
        with self._buf_lock:
            del self._req_failed[req_id]
            del self._req_err[req_id]
            del self._req_buf[req_id]
            del self._req_numlines[req_id]

    def _setup_request_data(self, req_id: str) -> None:
        with self._buf_lock:
            self._req_buf[req_id] = deque()
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_err[req_id] = ""
            self._req_event[req_id] = threading.Event()

    def get_data_buf(self, req_id: str) -> namedtuple:
        with self._buf_lock:
            buf = HistoryConn._databuf(
                failed=self._req_failed[req_id],
                err_msg=self._req_err[req_id],
                num_pts=self._req_numlines[req_id],
                raw_data=self._req_buf[req_id]
            )
        self._cleanup_request_data(req_id)
        return buf

    def read_ticks(self, req_id: str) -> np.array:
        res = self.get_data_buf(req_id)
        if res.failed:
            return np.array([res.err_msg], dtype='object')
        else:
            data = np.empty(res.num_pts, HistoryConn.tick_type)
            line_num = 0
            while res.raw_data and (line_num < res.num_pts):
                dl = res.raw_data.popleft()
                (dt, tm) = read_posix_ts_mil(dl[1])
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

    def request_ticks(self, ticker: str, max_ticks: int, ascend: bool=False,
                      timeout: int=None) -> np.array:
        # HTX,[Symbol],[MaxDatapoints],[DataDirection],[RequestID],
        # DatapointsPerSend]<CR><LF>
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "HTX,%s,%d,%d,%s,\r\n" % (ticker, max_ticks, ascend, req_id)
        self.send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self.read_ticks(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    def request_ticks_for_days(self, ticker: str, num_days: int,
                               bgn_flt: datetime.time=None,
                               end_flt: datetime.time=None,
                               ascend: bool=False, max_ticks: int=None,
                               timeout: int=None) -> np.array:
        # HTD,[Symbol],[Days],[MaxDatapoints],[BeginFilterTime],[EndFilterTime],
        # [DataDirection],[RequestID],[DatapointsPerSend]<CR><LF>
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        bf_str = time_to_hhmmss(bgn_flt)
        ef_str = time_to_hhmmss(end_flt)
        mt_str = blob_to_str(max_ticks)
        req_cmd = "HTD,%s,%d,%s,%s,%s,%d,%s,\r\n" % (ticker, num_days, mt_str,
                                                     bf_str, ef_str, ascend,
                                                     req_id)
        self.send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self.read_ticks(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    def request_ticks_in_period(self, ticker: str,
                                bgn_prd: datetime.datetime,
                                end_prd: datetime.datetime,
                                bgn_flt: datetime.time=None,
                                end_flt: datetime.time=None, ascend: bool=False,
                                max_ticks: int=None,
                                timeout: int=None) -> np.array:
        # HTT,[Symbol],[BeginDate BeginTime],[EndDate EndTime],[MaxDatapoints],
        # [BeginFilterTime],[EndFilterTime],[DataDirection],[RequestID],
        # [DatapointsPerSend]<CR><LF>
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        bp_str = datetime_to_yyyymmdd_hhmmss(bgn_prd)
        ep_str = datetime_to_yyyymmdd_hhmmss(end_prd)
        bf_str = time_to_hhmmss(bgn_flt)
        ef_str = time_to_hhmmss(end_flt)
        mt_str = blob_to_str(max_ticks)
        req_cmd = "HTT,%s,%s,%s,%s,%s,%s,%d,%s,\r\n" % (ticker, bp_str, ep_str,
                                                        mt_str, bf_str, ef_str,
                                                        ascend, req_id)
        self.send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self.read_ticks(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    def read_bars(self, req_id: str) -> np.array:
        res = self.get_data_buf(req_id)
        if res.failed:
            return np.array([res.err_msg], dtype='object')
        else:
            data = np.empty(res.num_pts, HistoryConn.bar_type)
            line_num = 0
            while res.raw_data and (line_num < res.num_pts):
                dl = res.raw_data.popleft()
                (dt, tm) = read_posix_ts(dl[1])
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
                     max_bars: int, ascend: bool=False,
                     timeout: int=None) -> np.array:
        # HIX,[Symbol],[Interval],[MaxDatapoints],[DataDirection],[RequestID],
        # [DatapointsPerSend],[IntervalType]<CR><LF>
        assert interval_type in ('s', 'v', 't')
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "HIX,%s,%d,%d,%d,%s,,%s\r\n" % (ticker, interval_len,
                                                  max_bars, ascend, req_id,
                                                  interval_type)
        self.send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self.read_bars(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    def request_bars_for_days(self, ticker: str, interval_len: int,
                              interval_type: str, days: int,
                              bgn_flt: datetime.time = None,
                              end_flt: datetime.time = None,
                              ascend: bool=False,
                              max_bars: int=None,
                              timeout: int=None) -> np.array:
        # HID,[Symbol],[Interval],[Days],[MaxDatapoints],[BeginFilterTime],
        # [EndFilterTime],[DataDirection],[RequestID],[DatapointsPerSend],
        # [IntervalType]<CR><LF>
        assert interval_type in ('s', 'v', 't')
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        bf_str = time_to_hhmmss(bgn_flt)
        ef_str = time_to_hhmmss(end_flt)
        mb_str = blob_to_str(max_bars)
        req_cmd = "HID,%s,%d,%d,%s,%s,%s,%d,%s,,%s\r\n" % (
                    ticker, interval_len, days, mb_str, bf_str, ef_str, ascend,
                    req_id, interval_type)
        self.send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self.read_bars(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    def request_bars_in_period(self, ticker: str,
                               interval_len: int,
                               interval_type: str,
                               bgn_prd: datetime.datetime,
                               end_prd: datetime.datetime,
                               bgn_flt: datetime.time=None,
                               end_flt: datetime.time=None,
                               ascend: bool=False,
                               max_bars: int=None,
                               timeout: int=None) -> np.array:
        # HIT,[Symbol],[Interval],[BeginDate BeginTime],[EndDate EndTime],
        # [MaxDatapoints],[BeginFilterTime],[EndFilterTime],[DataDirection],
        # [RequestID],[DatapointsPerSend],[IntervalType]<CR><LF>
        assert interval_type in ('s', 'v', 't')
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        bp_str = datetime_to_yyyymmdd_hhmmss(bgn_prd)
        ep_str = datetime_to_yyyymmdd_hhmmss(end_prd)
        bf_str = time_to_hhmmss(bgn_flt)
        ef_str = time_to_hhmmss(end_flt)
        mb_str = blob_to_str(max_bars)

        req_cmd = "HIT,%s,%d,%s,%s,%s,%s,%s,%d,%s,,%s\r\n" % (
                   ticker, interval_len, bp_str, ep_str, mb_str,
                   bf_str, ef_str, ascend, req_id, interval_type)
        self.send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self.read_bars(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    # noinspection PyUnresolvedReferences
    def read_days(self, req_id: str) -> np.array:
        res = self.get_data_buf(req_id)
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

    def request_daily_data(self, ticker: str, num_days: int, ascend: bool=False,
                           timeout: int=None):
        # HDX,[Symbol],[MaxDatapoints],[DataDirection],[RequestID],
        # [DatapointsPerSend]<CR><LF>
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "HDX,%s,%d,%d,%s,\r\n" % (ticker, num_days, ascend, req_id)
        self.send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self.read_days(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    def request_daily_data_for_dates(self, ticker: str,
                                     bgn_dt: datetime.date,
                                     end_dt: datetime.date,
                                     ascend: bool=False,
                                     max_days: int=None,
                                     timeout: int=None):
        # HDT,[Symbol],[BeginDate],[EndDate],[MaxDatapoints],[DataDirection],
        # [RequestID],[DatapointsPerSend]<CR><LF>
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        bgn_str = date_to_yyyymmdd(bgn_dt)
        end_str = date_to_yyyymmdd(end_dt)
        md_str = blob_to_str(max_days)
        req_cmd = "HDT,%s,%s,%s,%s,%d,%s,\r\n" % (ticker, bgn_str, end_str,
                                                  md_str, ascend, req_id)
        self.send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self.read_days(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    def request_weekly_data(self, ticker: str, num_weeks: int,
                            ascend: bool=False, timeout: int=None):
        # HWX,[Symbol],[MaxDatapoints],[DataDirection],[RequestID],
        # [DatapointsPerSend]<CR><LF>
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "HWX,%s,%d,%d,%s,\r\n" % (ticker, num_weeks, ascend, req_id)
        self.send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self.read_days(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data

    def request_monthly_data(self, ticker: str, num_months: int,
                             ascend: bool=False, timeout: int=None):
        # HMX,[Symbol],[MaxDatapoints],[DataDirection],[RequestID],
        # [DatapointsPerSend]<CR><LF>
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "HMX,%s,%d,%d,%s,\r\n" % (ticker, num_months, ascend, req_id)
        self.send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self.read_days(req_id)
        if data.dtype == object:
            iqfeed_err = str(data[0])
            err_msg = "Request: %s, Error: %s" % (req_cmd, iqfeed_err)
            if iqfeed_err == '!NO_DATA!':
                raise NoDataError(err_msg)
            else:
                raise RuntimeError(err_msg)
        else:
            return data


class TableConn(FeedConn):
    port = 9100

    mkt_type = np.dtype([('mkt_id', 'u8'),
                         ('short_name', 'S16'),
                         ('name', 'S128'),
                         ('group_id', 'u8'),
                         ('group', 'S128')])

    security_type = np.dtype([('sec_type', 'u8'),
                              ('short_name', 'S16'),
                              ('name', 'S128')])

    tcond_type = np.dtype([('tcond_id', 'u8'),
                           ('short_name', 'S16'),
                           ('name', 'S128')])

    sic_type = np.dtype([('sic', 'u8'),
                         ('name', 'S128')])

    naic_type = np.dtype([('naic', 'u8'),
                          ('name', 'S128')])

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

    def processing_function(self, fields: Sequence[str]) -> Callable[
                                                        [Sequence[str]], None]:
        if fields[0].isdigit():
            return self.process_table_entry
        elif fields[0] == '!ENDMSG!':
            return self.process_table_end
        else:
            return super().processing_function(fields)

    def process_table_entry(self, fields: Sequence[str]) -> None:
        assert fields[0].isdigit()
        self._current_deque.append(fields)

    def process_table_end(self, fields: Sequence[str]) -> None:
        assert fields[0] == "!ENDMSG!"
        self._current_event.set()

    def update_tables(self):
        self.start_runner()
        with self._update_lock:
            self.update_markets()
            self.update_security_types()
            self.update_trade_conditions()
            self.update_sic_codes()
            self.update_naic_codes()
            self._lookup_done = True
        self.stop_runner()

    def get_markets(self):
        if not self._lookup_done:
            raise RuntimeError("Update tables before requesting data")
        return self.markets

    def get_security_types(self):
        if not self._lookup_done:
            raise RuntimeError("Update tables before requesting data")
        return self.security_types

    def get_trade_conditions(self):
        if not self._lookup_done:
            raise RuntimeError("Update tables before requesting data")
        return self.trade_conds

    def get_sic_codes(self):
        if not self._lookup_done:
            raise RuntimeError("Update tables before requesting data")
        return self.sics

    def get_naic_codes(self):
        if not self._lookup_done:
            raise RuntimeError("Update tables before requesting data")
        return self.naics

    def update_markets(self):
        with self._update_lock:
            self._current_deque.clear()
            self._current_event.clear()
            self.send_cmd("SLM\r\n")
            self._current_event.wait(120)
            if self._current_event.is_set():
                num_pts = len(self._current_deque)
                self.markets = np.empty(num_pts, TableConn.mkt_type)
                line_num = 0
                while self._current_deque and (line_num < num_pts):
                    data_list = self._current_deque.popleft()
                    self.markets[line_num]['mkt_id'] = read_uint64(data_list[0])
                    self.markets[line_num]['short_name'] = data_list[1]
                    self.markets[line_num]['name'] = data_list[2]
                    self.markets[line_num]['group_id'] =\
                        read_uint64(data_list[3])
                    self.markets[line_num]['group'] = data_list[4]
                    line_num += 1
                    if line_num >= num_pts:
                        assert len(self._current_deque) == 0
                    if len(self._current_deque) == 0:
                        assert line_num >= num_pts
            else:
                raise RuntimeError("Update Market Types timed out")

    def update_security_types(self):
        with self._update_lock:
            self._current_deque.clear()
            self._current_event.clear()
            self.send_cmd("SST\r\n")
            self._current_event.wait(120)
            if self._current_event.is_set():
                num_pts = len(self._current_deque)
                self.security_types = np.empty(num_pts, TableConn.security_type)
                line_num = 0
                while self._current_deque and (line_num < num_pts):
                    data_list = self._current_deque.popleft()
                    self.security_types[line_num]['sec_type'] =\
                        read_uint64(data_list[0])
                    self.security_types[line_num]['short_name'] = data_list[1]
                    self.security_types[line_num]['name'] = data_list[2]
                    line_num += 1
                    if line_num >= num_pts:
                        assert len(self._current_deque) == 0
                    if len(self._current_deque) == 0:
                        assert line_num >= num_pts
            else:
                raise RuntimeError("Update Security Types timed out")

    def update_trade_conditions(self):
        with self._update_lock:
            self._current_deque.clear()
            self._current_event.clear()
            self.send_cmd("STC\r\n")
            self._current_event.wait(120)
            if self._current_event.is_set():
                num_pts = len(self._current_deque)
                self.trade_conds = np.empty(num_pts, TableConn.tcond_type)
                line_num = 0
                while self._current_deque and (line_num < num_pts):
                    data_list = self._current_deque.popleft()
                    self.trade_conds[line_num]['tcond_id'] =\
                        read_uint64(data_list[0])
                    self.trade_conds[line_num]['short_name'] = data_list[1]
                    self.trade_conds[line_num]['name'] = data_list[2]
                    line_num += 1
                    if line_num >= num_pts:
                        assert len(self._current_deque) == 0
                    if len(self._current_deque) == 0:
                        assert line_num >= num_pts
            else:
                raise RuntimeError("Update Trade Conditions timed out")

    def update_sic_codes(self):
        with self._update_lock:
            self._current_deque.clear()
            self._current_event.clear()
            self.send_cmd("SSC\r\n")
            self._current_event.wait(120)
            if self._current_event.is_set():
                num_pts = len(self._current_deque)
                self.sics = np.empty(num_pts, TableConn.sic_type)
                line_num = 0
                while self._current_deque and (line_num < num_pts):
                    data_list = self._current_deque.popleft()
                    self.sics[line_num]['sic'] = read_uint64(data_list[0])
                    self.sics[line_num]['name'] = ",".join(data_list[1:])
                    line_num += 1
                    if line_num >= num_pts:
                        assert len(self._current_deque) == 0
                    if len(self._current_deque) == 0:
                        assert line_num >= num_pts
            else:
                raise RuntimeError("Update SIC codes timed out")

    def update_naic_codes(self):
        with self._update_lock:
            self._current_deque.clear()
            self._current_event.clear()
            self.send_cmd("SNC\r\n")
            self._current_event.wait(120)
            if self._current_event.is_set():
                num_pts = len(self._current_deque)
                self.naics = np.empty(num_pts, TableConn.naic_type)
                line_num = 0
                while self._current_deque and (line_num < num_pts):
                    data_list = self._current_deque.popleft()
                    self.naics[line_num]['naic'] = read_uint64(data_list[0])
                    self.naics[line_num]['name'] = ",".join(data_list[1:])
                    line_num += 1
                    if line_num >= num_pts:
                        assert len(self._current_deque) == 0
                    if len(self._current_deque) == 0:
                        assert line_num >= num_pts
            else:
                raise RuntimeError("Update NAIC codes timed out")


class LookupConn(FeedConn):
    port = 9100

    futures_month_letter_map = {1: 'F', 2: 'G', 3: 'H',
                                4: 'J', 5: 'K', 6: 'M',
                                7: 'N', 8: 'Q', 9: 'U',
                                10: 'V', 11: 'X', 12: 'Z'}
    futures_month_letters = ('F', 'G', 'H', 'J', 'K', 'M',
                             'N', 'Q', 'U', 'V', 'X', 'Z')

    equity_call_month_letters = ('A', 'B', 'C', 'D', 'E', 'F',
                                 'G', 'H', 'I', 'J', 'K', 'L')
    equity_call_month_letter_map = {1: 'A', 2: 'B', 3: 'C',
                                    4: 'D', 5: 'E', 6: 'F',
                                    7: 'G', 8: 'H', 9: 'I',
                                    10: 'J', 11: 'K', 12: 'L'}
    equity_put_month_letters = ('M', 'N', 'O', 'P', 'Q', 'R',
                                'S', 'T', 'U', 'V', 'W', 'X')
    equity_put_month_letter_map = {1: 'M', 2: 'N', 3: 'O',
                                   4: 'P', 5: 'Q', 6: 'R',
                                   7: 'S', 8: 'T', 9: 'U',
                                   10: 'V', 11: 'W', 12: 'X'}

    asset_type = np.dtype([('symbol', 'S128'),
                           ('market', 'u1'),
                           ('security_type', 'u1'),
                           ('name', 'S128'),
                           ('sector', 'u8')])

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
        self._buf_lock = threading.RLock()

    def _set_message_mappings(self) -> None:
        super()._set_message_mappings()
        self._pf_dict['L'] = self.process_lookup_datum

    def _send_connect_message(self):
        # The history/lookup socket does not accept connect messages
        pass

    def process_lookup_datum(self, fields: Sequence[str]) -> None:
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
        with self._buf_lock:
            req_id = "L_%.10d" % self._req_num
            self._req_num += 1
            return req_id

    def _cleanup_request_data(self, req_id: str) -> None:
        with self._buf_lock:
            del self._req_failed[req_id]
            del self._req_err[req_id]
            del self._req_buf[req_id]
            del self._req_numlines[req_id]

    def _setup_request_data(self, req_id: str) -> None:
        with self._buf_lock:
            self._req_buf[req_id] = deque()
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_err[req_id] = ""
            self._req_event[req_id] = threading.Event()

    def get_data_buf(self, req_id: str) -> namedtuple:
        with self._buf_lock:
            buf = LookupConn._databuf(
                failed=self._req_failed[req_id],
                err_msg=self._req_err[req_id],
                num_pts=self._req_numlines[req_id],
                raw_data=self._req_buf[req_id]
            )
        self._cleanup_request_data(req_id)
        return buf

    def read_symbols(self, req_id: str) -> np.array:
        res = self.get_data_buf(req_id)
        if res.failed:
            return np.array([res.err_msg], dtype='object')
        else:
            data = np.empty(res.num_pts, LookupConn.asset_type)
            line_num = 0
            while res.raw_data and (line_num < res.num_pts):
                dl = res.raw_data.popleft()
                data[line_num]['symbol'] = dl[1].strip()
                data[line_num]['market'] = read_uint8(dl[2])
                data[line_num]['security_type'] = read_uint8(dl[3])
                data[line_num]['name'] = dl[4].strip()
                data[line_num]['sector'] = 0
                line_num += 1
                if line_num >= res.num_pts:
                    assert len(res.raw_data) == 0
                if len(res.raw_data) == 0:
                    assert line_num >= res.num_pts
            return data

    def request_symbols_by_filter(self, search_term: str, search_field: str='d',
                                  filt_val: str=None, filt_type: str=None,
                                  timeout=None) -> np.array:
        # SBF,[Field To Search],[Search String],[Filter Type],[Filter Value],
        # [RequestID]<CR><LF>
        assert search_field in ('d', 's')
        assert search_term is not None
        assert filt_type is None or filt_type in ('e', 't')

        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "SBF,%s,%s,%s,%s,%s\r\n" % (search_field, search_term,
                                              blob_to_str(filt_type),
                                              blob_to_str(filt_val), req_id)
        self.send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self.read_symbols(req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def read_symbols_with_sect(self, req_id: str) -> np.array:
        res = self.get_data_buf(req_id)
        if res.failed:
            return np.array([res.err_msg], dtype='object')
        else:
            data = np.empty(res.num_pts, LookupConn.asset_type)
            line_num = 0
            while res.raw_data and (line_num < res.num_pts):
                dl = res.raw_data.popleft()
                data[line_num]['sector'] = read_uint64(dl[1])
                data[line_num]['symbol'] = dl[2].strip()
                data[line_num]['market'] = read_uint8(dl[3])
                data[line_num]['security_type'] = read_uint8(dl[4])
                data[line_num]['name'] = dl[5].strip()
                line_num += 1
                if line_num >= res.num_pts:
                    assert len(res.raw_data) == 0
                if len(res.raw_data) == 0:
                    assert line_num >= res.num_pts
            return data

    def request_symbols_by_sic(self, sic: int, timeout=None) -> np.array:
        # SBS,[Search String],[RequestID]<CR><LF>
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "SBS,%d,%s\r\n" % (sic, req_id)
        self.send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self.read_symbols_with_sect(req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_symbols_by_naic(self, naic: int, timeout=None) -> np.array:
        # SBN,[Search String],[RequestID]<CR><LF>
        req_id = self._get_next_req_id()
        self._setup_request_data(req_id)
        req_cmd = "SBS,%d,%s\r\n" % (naic, req_id)
        self.send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self.read_symbols_with_sect(req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def read_futures_chain(self, req_id: str) -> List[str]:
        res = self.get_data_buf(req_id)
        if res.failed:
            return ["!ERROR!", res.err_msg]
        else:
            assert res.num_pts == 1
            chain = res.raw_data[0][1:]
            if chain[-1] == "":
                chain = chain[:-1]
            return chain

    def request_futures_chain(self, symbol: str,
                              month_codes: str = None,
                              years: str = None,
                              near_months: int = None,
                              timeout: int = None) -> List[str]:
        # CFU,[Symbol],[Month Codes],[Years],[Near Months],[RequestID]<CR><LF>
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
        req_cmd = "CFU,%s,%s,%s,%s,%s\r\n" % (symbol, blob_to_str(month_codes),
                                              blob_to_str(years),
                                              blob_to_str(near_months), req_id)
        self.send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self.read_futures_chain(req_id)
        if (len(data) == 2) and (data[0] == "!ERROR!"):
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[1]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_futures_spread_chain(self, symbol: str,
                                     month_codes: str = None,
                                     years: str = None,
                                     near_months: int = None,
                                     timeout: int = None) -> List[str]:
        # CFS,[Symbol],[Month Codes],[Years],[Near Months],[RequestID]<CR><LF>
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
        req_cmd = "CFS,%s,%s,%s,%s,%s\r\n" % (symbol,
                                              blob_to_str(month_codes),
                                              blob_to_str(years),
                                              blob_to_str(near_months),
                                              req_id)
        self.send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self.read_futures_chain(req_id)
        if (len(data) == 2) and (data[0] == "!ERROR!"):
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[1]))
            raise RuntimeError(err_msg)
        else:
            return data

    def read_option_chain(self, req_id: str) -> dict:
        res = self.get_data_buf(req_id)
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
            put_symbols = symbols[cp_delim+1:]
            if len(put_symbols) > 0:
                if put_symbols[-1] == "":
                    put_symbols = put_symbols[:-1]
            return {"c": call_symbols, "p": put_symbols}

    def request_futures_option_chain(self, symbol: str,
                                     opt_type: str = 'pc',
                                     month_codes: str = None,
                                     years: str = None,
                                     near_months: int = None,
                                     timeout: int = None) -> dict:
        # CFO,[Symbol],[Puts/Calls],[Month Codes],[Years],[Near Months],
        # [RequestID]<CR><LF>
        assert (symbol is not None) and (symbol != '')

        assert opt_type is not None
        assert len(opt_type) in (1, 2)
        for op in opt_type:
            assert op in ('p', 'c')

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
        req_cmd = "CFO,%s,%s,%s,%s,%s,%s\r\n" % (symbol,
                                                 opt_type,
                                                 blob_to_str(month_codes),
                                                 blob_to_str(years),
                                                 blob_to_str(near_months),
                                                 req_id)
        self.send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self.read_option_chain(req_id)
        if (type(data) == list) and (data[0] == "!ERROR!"):
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[1]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_equity_option_chain(self, symbol: str,
                                    opt_type: str = 'pc',
                                    month_codes: str = None,
                                    near_months: int = None,
                                    include_binary: bool = True,
                                    filt_type: int = 0,
                                    filt_val_1: float = None,
                                    filt_val_2: float = None,
                                    timeout: int = None) -> List[str]:
        # CEO,[Symbol],[Puts/Calls],[Month Codes],[Near Months],
        # [BinaryOptions],[Filter Type],[Filter Value One],[Filter Value Two],
        # [RequestID]<CR><LF>
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
                valid_month_codes = LookupConn.equity_put_month_letters
            elif opt_type == 'c':
                valid_month_codes = LookupConn.equity_call_month_letters
            elif opt_type == 'cp' or opt_type == 'pc':
                valid_month_codes = (LookupConn.equity_call_month_letters +
                                     LookupConn.equity_put_month_letters)
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
        req_cmd =\
            "CEO,%s,%s,%s,%s,%d,%d,%s,%s,%s\r\n" % (symbol, opt_type,
                                                    blob_to_str(month_codes),
                                                    blob_to_str(near_months),
                                                    include_binary,
                                                    filt_type,
                                                    blob_to_str(filt_val_1),
                                                    blob_to_str(filt_val_2),
                                                    req_id)
        self.send_cmd(req_cmd)
        self._req_event[req_id].wait(timeout=timeout)
        data = self.read_option_chain(req_id)
        if (type(data) == list) and (data[0] == "!ERROR!"):
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[1]))
            raise RuntimeError(err_msg)
        else:
            return data
