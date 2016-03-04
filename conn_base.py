import select
import socket
import threading
import datetime
import time
import itertools
import numpy as np
from typing import Sequence, Callable


def read_market_open(field: str) -> bool:
    bool(int(field)) if field != "" else False


def read_short_restricted(field: str) -> bool:
    if field != "":
        if field == 'Y':
            return True
        if field == 'N':
            return False
        else:
            raise RuntimeError("Unknown Value in Short Restricted Field: %s" % field)
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
            raise RuntimeError("Unknown value in Tick Direction Field: %s" % field)
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


# def read_hhmmss(hhmmss: str) -> datetime.time:
#     if hhmmss != "":
#         tm = time.strptime(hhmmss, "%H:%M:%S")
#         return datetime.time(tm.tm_hour, tm.tm_min, tm.tm_sec)
#     else:
#         return None
#
#
# def read_mm_dd_yyyy(date_str: str) -> datetime.date:
#     if date_str != "":
#         timestamp = time.strptime(date_str, "%m/%d/%Y")
#         return datetime.date.fromtimestamp(timestamp)
#     else:
#         return None
#
#
# def read_yyyymmdd_hhmmss(dttm: str) -> datetime.datetime:
#     if dttm != "":
#         return time.strptime(dttm, "%Y%m%d %H%M%S")
#     else:
#         return None
#
#
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
        msecs_since_midnight = (1000000 * ((3600*hour) + (60*minute) + second)) + msecs
        return msecs_since_midnight
    else:
        return 0


def read_mmddccyy(field: str) -> np.datetime64:
    if field != "":
        month = int(field[0:2])
        day = int(field[3:5])
        year = int(field[6:10])
        return np.datetime64(datetime.date(year=year, month=month, day=day), 'D')
    else:
        return np.datetime64(datetime.date(year=1, month=1, day=1), 'D')


def str_or_blank(val) -> str:
    if val is not None:
        return str(val)
    else:
        return ""


# def time_to_hhmmss(tm: datetime.time) -> str:
#     if tm is not None:
#         return "%.2d%.2d%.2d" % (tm.hour, tm.minute, tm.second)
#     else:
#         return ""
#
#
# def date_to_yyyymmdd(dt: datetime.date) -> str:
#     if dt is not None:
#         return "%.4d%.2d%.2d" % (dt.year, dt.month, dt.day)
#     else:
#         return ""
#
#
# def datetime_to_yyyymmdd_hhmmss(dt_tm: datetime.datetime) -> str:
#     if dt_tm is not None:
#         return "%.4d%.2d%.2d %.2d%.2d%.2d" % (dt_tm.year, dt_tm.month, dt_tm.day, dt_tm.hour, dt_tm.minute, dt_tm.second)
#     else:
#         return ""

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
        self._read_thread = threading.Thread(group=None, target=self, name="%s-reader" % self._name, args=(),
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
        self.set_protocol(FeedConn.protocol)
        self.set_client_name(self._name)
        self.send_connect_message()

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
                self.read_messages()
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

    def read_messages(self) -> None:
        ready_list = select.select([self._sock], [], [self._sock], 5)
        if ready_list[2]:
            raise RuntimeError("There was a problem with the socket for QuoteReader: %s," % self._name)
        if ready_list[0]:
            with self._buf_lock:
                data_recvd = self._sock.recv(16384).decode()
                self._recv_buf += data_recvd

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
                print("Message: %s" % message)
                fields = message.split(',')
                handle_func = self.processing_function(fields)
                handle_func(fields)
                message = self.next_message()

    def processing_function(self, fields) -> None:
        pf = self._pf_dict.get(fields[0])
        if pf is not None:
            return pf
        else:
            return self.process_unregistered_message

    def process_unregistered_message(self, fields: Sequence[str]) -> None:
        raise RuntimeError("Unexpected message received: %s", ",".join(fields))

    def process_system_message(self, fields: Sequence[str]) -> None:
        assert len(fields) > 1
        assert fields[0] == "S"
        processing_func = self.system_processing_function(fields)
        processing_func(fields)

    def system_processing_function(self, fields) -> Callable[[Sequence[str]], None]:
        assert len(fields) > 1
        assert fields[0] == "S"
        spf = self._sm_dict.get(fields[1])
        if spf is not None:
            return spf
        else:
            return self.process_unregistered_system_message(self, fields)

    def process_unregistered_system_message(self, fields: Sequence[str]) -> None:
        raise RuntimeError("Unexpected system message received: %s", ",".join(fields))

    def process_current_protocol(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == "S"
        assert fields[1] == "CURRENT PROTOCOL"
        protocol = fields[2]
        if protocol != FeedConn.protocol:
            raise RuntimeError("Desired Protocol %s, Server Says Protocol %s" % (FeedConn.protocol, protocol))

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
                      "num_recon": int(fields[8]), "num_fail_recon": int(fields[9]),
                      "conn_tm":  time.strptime(fields[10], "%b %d %I:%M%p"),
                      "mkt_tm": time.strptime(fields[11], "%b %d %I:%M%p"),
                      "status": (fields[12] == "Connected"),
                      "feed_version": fields[13], "login": fields[14],
                      "kbs_recv": float(fields[15]), "kbps_recv": float(fields[16]),
                      "avg_kbps_recv": float(fields[17]),
                      "kbs_sent": float(fields[18]), "kbps_sent": float(fields[19]),
                      "avg_kbps_sent": float(fields[20])}
        for listener in self._listeners:
            listener.process_conn_stats(conn_stats)

    def process_timestamp(self, fields: Sequence[str]) -> None:
        assert fields[0] == "T"
        assert len(fields) > 1
        time_val = time.strptime("%s EST" % fields[1], "%Y%m%d %H:%M:%S %Z")
        for listener in self._listeners:
            listener.process_timestamp(time_val)

    def process_error(self, fields: Sequence[str]) -> None:
        assert fields[0] == "E"
        assert len(fields) > 1
        print("process_error: %s" % ",".join(fields))
        for listener in self._listeners:
            listener.process_error(fields)

    def add_listener(self, listener) -> None:
        if listener not in self._listeners:
            self._listeners.append(listener)

    def remove_listener(self, listener) -> None:
        if listener in self._listeners:
            self._listeners.remove(listener)

    def set_protocol(self, protocol) -> None:
        self.send_cmd("S,SET PROTOCOL,%s\r\n" % protocol)

    def send_connect_message(self) -> None:
        msg = "S,CONNECT\r\n"
        self.send_cmd(msg)

    def send_disconnect_message(self) -> None:
        self.send_cmd("S,DISCONNECT\r\n")

    def set_client_name(self, name) -> None:
        self._name = name
        msg = "S,SET CLIENT NAME,%s\r\n" % name
        self.send_cmd(msg)


class QuoteConn(FeedConn):
    port = 5009

    regional_type = np.dtype([('Symbol', 'S64'),
                              ('Regional Bid', 'f8'), ('Regional BidSize', 'u8'), ('Regional BidTime', 'u8'),
                              ('Regional Ask', 'f8'), ('Regional AskSize', 'u8'), ('Regional AskTime', 'u8'),
                              ('Market Center', 'u1'),
                              ('Fraction Display Code', 'u1'), ('Decimal Precision', 'u2')])

    fundamental_fields = ["Symbol", "Exchange ID", "PE", "Average Volume",
                          "52 Week High", "52 Week Low",
                          "Calendar Year High", "Calendar Year Low",
                          "Dividend Yield", "Dividend Amount", "Dividend Rate",
                          "Pay Date", "Ex-dividend Date",
                          "(Reserved)", "(Reserved)", "(Reserved)", "Short Interest", "(Reserved)",
                          "Current Year EPS", "Next Year EPS",
                          "Five-year Growth Percentage", "Fiscal Year End", "(Reserved)", "Company Name",
                          "Root Option Symbol", "Percent Held By Institutions", "Beta", "Leaps",
                          "Current Assets", "Current Liabilities", "Balance Sheet Date", "Long-term Debt",
                          "Common Shares Outstanding", "(Reserved)", "Split Factor 1", "Split Factor 2",
                          "(Reserved)", "Market Center", "Format Code", "Precision", "SIC",
                          "Historical Volatility", "Security Type", "Listed Market",
                          "52 Week High Date", "52 Week Low Date",
                          "Calendar Year High Date", "Calendar Year Low Date",
                          "Year End Close", "Maturity Date", "Coupon Rate", "Expiration Date",
                          "Strike Price", "NAICS", "Exchange Root"]

    fundamental_type = [('Symbol', 'S128'),
                        ('PE', 'f8'),
                        ('Average Volume', 'f8'),
                        ('52 Week High', 'f8'), ('52 Week Low', 'f8'),
                        ('Calendar Year High', 'f8'), ('Calendar Year Low', 'f8'),
                        ('Dividend Yield', 'f8'), ('Dividend Amount', 'f8'), ('Dividend Rate', 'f8'),
                        ('Pay Date', 'M8[D]'), ('Ex-dividend Date', 'M8[D]'),
                        ('Short Interest', 'i8'),
                        ('Current Year EPS', 'f8'), ('Next Year EPS', 'f8'),
                        ('Five-year Growth Percentage', 'f8'), ('Fiscal Year End', 'u1'),
                        ('Company Name', 'S256'),
                        ('Root Option Symbol', 'S256'),
                        ('Percent Held By Institutions', 'f8'),
                        ('Beta', 'f8'), ('Leaps', 'S128'),
                        ('Current Assets', 'f8'), ('Current Liabilities', 'f8'),
                        ('Balance Sheet Date', 'M8[D]'), ('Long-term Debt', 'f8'),
                        ('Common Shares Outstanding', 'f8'),
                        ('Split Factor 1 Date', 'M8[D]'), ('Split Factor 1', 'f8'),
                        ('Split Factor 2 Date', 'M8[D]'), ('Split Factor 2', 'f8'),
                        ('Format Code', 'u1'), ('Precision', 'u1'),
                        ('SIC', 'u8'),
                        ('Historical Volatility', 'f8'),
                        ('Security Type', 'u1'), ('Listed Market', 'u1'),
                        ('52 Week High Date', 'M8[D]'), ('52 Week Low Date', 'M8[D]'),
                        ('Calendar Year High Date', 'M8[D]'), ('Calendar Year Low Date', 'M8[D]'),
                        ('Year End Close', 'f8'), ('Maturity Date', 'M8[D]'),
                        ('Coupon Rate', 'f8'), ('Expiration Date', 'M8[D]'),
                        ('Strike Price', 'f8'),
                        ('NAICS', 'u8'),
                        ('Exchange Root', 'S128')]

    quote_msg_map = {'Symbol': ('Symbol', 'S128', lambda x: x),
                     '7 Day Yield': ('7 Day Yield', 'f8', read_float64),
                     'Ask': ('Ask', 'f8', read_float64),
                     'Ask Change': ('Ask Change', 'f8', read_float64),
                     'Ask Market Center': ('Ask Market Center', 'u1', read_uint8),
                     'Ask Size': ('Ask Size', 'u8', read_uint64),
                     'Ask Time': ('Ask Time', 'u8', read_hhmmssmil),
                     'Available Regions': ('Available Regions', 'S128', lambda x: x),  # TODO: Parse
                     'Average Maturity': ('Average Maturity', 'f8', read_float64),
                     'Bid': ('Bid', 'f8', read_float64),
                     'Bid Change': ('Bid Change', 'f8', read_float64),
                     'Bid Market Center': ('Bid Market Center', 'u1', read_uint8),
                     'Bid Size': ('Bid Size', 'u8', read_uint64),
                     'Bid Time': ('Bid Time', 'u8', read_hhmmssmil),
                     'Change': ('Change', 'f8', read_float64),
                     'Change From Open': ('Change From Open', 'f8', read_float64),
                     'Close': ('Close', 'f8', read_float64),
                     'Close Range 1': ('Close Range 1', 'f8', read_float64),
                     'Close Range 2': ('Close Range 2', 'f8', read_float64),
                     'Days to Expiration': ('Days to Expiration', 'u2', read_uint16),
                     'Decimal Precision': ('Decimal Precision', 'u1', read_uint8),
                     'Delay': ('Delay', 'u1', read_uint8),
                     'Exchange ID': ('Exchange ID', 'u1', read_hex),
                     'Extended Trade': ('Extended Price',  'f8', read_float64),
                     'Extended Trade Date': ('Extended Trade Date', 'M8[D]', read_mmddccyy),
                     'Extended Trade Market Center': ('Extended Trade Market Center', 'u1', read_uint8),
                     'Extended Trade Size': ('Extended Trade Size', 'u8', read_uint64),
                     'Extended Trade Time': ('Extended Trade Time', 'u8', read_hhmmssmil),
                     'Extended Trading Change': ('Extended Trading Change', 'f8', read_float64),
                     'Extended Trading Difference': ('Extended Trading Difference', 'f8', read_float64),
                     'Financial Status Indicator': ('Financial Status Indicator', 'S1', lambda x: x),  # TODO: Parse
                     'Fraction Display Code': ('Fraction Display Code', 'u1', read_uint8),
                     'High': ('High', 'f8', read_float64),
                     'Last': ('Last', 'f8', read_float64),
                     'Last Date': ('Last Date', 'M8[D]', read_mmddccyy),
                     'Last Market Center': ('Last Market Center', 'u1', read_uint8),
                     'Last Size': ('Last Size', 'u8', read_uint64),
                     'Last Time': ('Last Time', 'u8', read_hhmmssmil),
                     'Low': ('Low', 'f8', read_float64),
                     'Market Capitalization': ('Market Capitalization', 'f8', read_float64),
                     'Market Open': ('Market Open', 'b1', read_market_open),
                     'Message Contents': ('Message Contents', 'S9', lambda x: x),  # TODO: Parse
                     'Most Recent Trade': ('Most Recent Trade', 'f8', read_float64),
                     'Most Recent Trade Conditions': ('Most Recent Trade Conditions', 'S16', lambda x: x),  # TODO: Parse
                     'Most Recent Trade Date': ('Most Recent Trade Date', 'M8[D]', read_mmddccyy),
                     'Most Recent Trade Market Center': ('Most Recent Trade Market Center', 'u1', read_uint8),
                     'Most Recent Trade Size': ('Most Recent Trade Size', 'u8', read_uint64),
                     'Most Recent Trade Time': ('Most Recent Trade Time', 'u8', read_hhmmssmil),
                     'Net Asset Value': ('Net Asset Value', 'f8', read_float64),
                     'Number of Trades Today': ('Number of Trades Today', 'u8', read_uint64),
                     'Open': ('Open', 'f8', read_float64),
                     'Open Interest': ('Open Interest', 'u8', read_uint64),
                     'Open Range 1': ('Open Range 1', 'f8', read_float64),
                     'Open Range 2': ('Open Range 2', 'f8', read_float64),
                     'Percent Change': ('Percent Change', 'f8', read_float64),
                     'Percent Off Average Volume': ('Percent Off Average Volume', 'f8', read_float64),
                     'Previous Day Volume': ('Previous Day Volume', 'u8', read_uint64),
                     'Price-Earnings Ratio': ('Price-Earnings Ratio', 'f8', read_float64),
                     'Range': ('Range', 'f8', read_float64),
                     'Restricted Code': ('Restricted Code', 'b1', read_short_restricted),
                     'Settle': ('Settle', 'f8', read_float64),
                     'Settlement Date': ('Settlement Date', 'M8[D]', read_mmddccyy),
                     'Spread': ('Spread', 'f8', read_float64),
                     'Tick': ('Tick', 'i8', read_tick_direction),
                     'TickID': ('TickId', 'u8', read_uint64),
                     'Total Volume': ('Total Volume', 'u8', read_uint64),
                     'Volatility': ('Volatility', 'f8', read_float64),
                     'VWAP': ('VWAP', 'f8', read_float64)}

    def __init__(self, name:str = "QuoteConn", host: str = FeedConn.host, port: int = port):
        super().__init__(name, host, port)
        self._current_update_fields = []
        self._update_names = []
        self._update_dtype = []
        self._update_reader = []
        self._set_message_mappings()
        self._current_update_fields = ["Symbol",
                                       "Most Recent Trade", "Most Recent Trade Size", "Most Recent Trade Time",
                                       "Most Recent Trade Market Center", "Total Volume",
                                       "Bid", "Bid Size", "Ask", "Ask Size",
                                       "Open", "High", "Low", "Close",
                                       "Message Contents", "Most Recent Trade Conditions"]
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
        self._sm_dict["SYMBOL LIMIT REACHED"] = self.process_symbol_limit_reached
        self._sm_dict["IP"] = self.process_ip_addresses_used
        self._sm_dict["FUNDAMENTAL FIELDNAMES"] = self.process_fundamental_fieldnames
        self._sm_dict["UPDATE FIELDNAMES"] = self.process_update_fieldnames
        self._sm_dict["CURRENT UPDATE FIELDNAMES"] = self.process_current_update_fieldnames

    def process_invalid_symbol(self, fields: Sequence[str]) -> None:
        assert len(fields) > 1
        assert fields[0] == 'n'
        bad_sym = fields[1]
        for listener in self._listeners:
            listener.process_bad_symbol_error(bad_sym)

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
        rgn_quote = np.empty(shape=(1), dtype=QuoteConn.regional_type)
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
            listener.process_update(update)

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
            update[self._update_names[field_num]] = self._update_reader[field_num](field)
        print("Quote Update:")
        print(update)
        print(update.dtype)
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
        msg['Leaps'] = fields[28] # TODO: Parse
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
        msg['Listed Market'] =  read_uint8(fields[44])
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
        print("Fundamental Message:")
        print(msg)
        print(msg.dtype)
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

    def process_fundamental_fieldnames(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'FUNDAMENTAL FIELDNAMES'
        for field in fields[2:]:
            if field not in QuoteConn.fundamental_fields:
                raise RuntimeError("Protocol Conflict: %s not found in QuoteConn.dtn_fundamental_fields" % field)
        for field in QuoteConn.fundamental_fields:
            if field not in fields[2:]:
                raise RuntimeError("%s not found in FUNDAMENTAL FIELDNAMES message" % field)

    def process_update_fieldnames(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'UPDATE FIELDNAMES'
        # Raise exception instead of printing after debugging
        for field in fields[2:]:
            if field not in QuoteConn.quote_msg_map:
                raise RuntimeError("Protocol Conflict: %s not found in QuoteConn.dtn_update_map" % field)
        for field in QuoteConn.quote_msg_map:
            if field not in fields[2:]:
                raise RuntimeError("Protocol Conflict: %s not found in UPDATE FIELDNAMES message" % field)

    def process_current_update_fieldnames(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'CURRENT UPDATE FIELDNAMES'
        self._set_current_update_structs(fields[2:])

    def _set_current_update_structs(self, fields):
        num_update_fields = len(fields)
        new_update_fields = list(itertools.repeat("", num_update_fields))
        new_update_names = new_update_fields
        new_update_dtypes = list(itertools.repeat(("no_name", 'i8'), num_update_fields))
        new_update_reader = list(itertools.repeat(lambda x: x, num_update_fields))
        for field_num, field in enumerate(fields):
            if field not in QuoteConn.quote_msg_map:
                raise RuntimeError("Protocol Conflict: %s not in QuoteConn.dtn_update_map" % field)
            new_update_fields[field_num] = field
            dtn_update_tup = QuoteConn.quote_msg_map[field]
            new_update_names[field_num] = dtn_update_tup[0]
            new_update_dtypes[field_num] = (dtn_update_tup[0], dtn_update_tup[1])
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

    def select_update_fieldnames(self, field_names: Sequence[str]) -> None:
        symbol_field = "Symbol"
        if symbol_field not in field_names:
            field_names.insert(0, symbol_field)
        else:
            symbol_idx = field_names.index("Symbol")
            if symbol_idx != 0:
                field_names[0], field_names[symbol_idx] = field_names[symbol_idx] , field_names[0]
        self.send_cmd("S,SELECT UPDATE FIELDS,%s\r\n" % ",".join(field_names))

    def set_log_levels(self, log_levels: Sequence[str]) -> None:
        self.send_cmd("S,SET LOG LEVELS,%s\r\n" % ",".join(log_levels))

if __name__ == "__main__":
    from service import FeedService
    from passwords import dtn_login, dtn_password, dtn_product_id

    svc = FeedService(product=dtn_product_id, version="Debugging", login=dtn_login, password=dtn_password,
                      autoconnect=True, savelogininfo=True)
    svc.launch()

    conn = QuoteConn(name="RunningInIDE")
    conn.start_runner()

    conn.request_all_update_fieldnames()
    conn.request_current_update_fieldnames()
    conn.request_fundamental_fieldnames()
    all_fields = sorted(list(QuoteConn.quote_msg_map.keys()))
    conn.select_update_fieldnames(all_fields)
    conn.watch_regional("@VXH16")
    time.sleep(30)
    conn.stop_runner()


