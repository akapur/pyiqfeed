import select
import socket
import threading
import datetime
import time
import numpy as np
from typing import Sequence, Callable


def read_float(float_str: str) -> float:
    if float_str != "":
        return float(float_str)
    else:
        return None


def read_int(int_str: str) -> int:
    if int_str != "":
        return int(int_str)
    else:
        return None


def read_hhmmss(hhmmss: str) -> datetime.time:
    if hhmmss != "":
        tm = time.strptime(hhmmss, "%H:%M:%S")
        return datetime.time(tm.tm_hour, tm.tm_min, tm.tm_sec)
    else:
        return None


def read_mm_dd_yyyy(date_str: str) -> datetime.date:
    if date_str != "":
        timestamp = time.strptime(date_str, "%m/%d/%Y")
        return datetime.date.fromtimestamp(timestamp)
    else:
        return None


def read_yyyymmdd_hhmmss(dttm: str) -> datetime.datetime:
    if dttm != "":
        return time.strptime(dttm, "%Y%m%d %H%M%S")
    else:
        return None


def read_split_string(split_str: str) -> tuple:
    if split_str != "":
        (split_factor, split_date) = split_str.split(' ')
        split_factor = read_float(split_factor)
        split_date = read_mm_dd_yyyy(split_date)
        split_data = (split_factor, split_date)
    else:
        split_data = (None, None)
    return split_data


def int_to_str(val: int) -> str:
    if val is not None:
        return str(val)
    else:
        return ""


def time_to_hhmmss(tm: datetime.time) -> str:
    if tm is not None:
        return "%.2d%.2d%.2d" % (tm.hour, tm.minute, tm.second)
    else:
        return ""


def date_to_yyyymmdd(dt: datetime.date) -> str:
    if dt is not None:
        return "%.4d%.2d%.2d" % (dt.year, dt.month, dt.day)
    else:
        return ""


def datetime_to_yyyymmdd_hhmmss(dt_tm: datetime.datetime) -> str:
    if dt_tm is not None:
        return "%.4d%.2d%.2d %.2d%.2d%.2d" % (dt_tm.year, dt_tm.month, dt_tm.day, dt_tm.hour, dt_tm.minute, dt_tm.second)
    else:
        return ""


def hh_mm_ss_to_secs_since_midnight(tm_str: str) -> int:
    hour = int(tm_str[0:2])
    minute = int(tm_str[3:5])
    second = int(tm_str[6:8])
    secs_since_midnight = (3600*hour) + (60 * minute) + second
    return secs_since_midnight


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

    def start_runner(self):
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

    def iqfeed_protocol(self):
        return FeedConn.protocol

    def connected(self):
        return self._connected

    def reconnect_failed(self):
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
                fields = message.split(',')
                handle_func = self.processing_function(fields)
                handle_func(fields)
                message = self.next_message()

    def processing_function(self, fields) -> Callable[Sequence[str], type(None)]:
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

    def system_processing_function(self, fields):
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

    regional_type = np.dtype([('ticker', 'S64'),
                              ('rgn_bid', 'f8'), ('rgn_bid_sz', 'u8'), ('rgn_bid_tm', 'u4'),
                              ('rgn_ask', 'f8'), ('rgn_ask_sz', 'u8'), ('rgn_ask_tm', 'u4'),
                              ('mkt_center', 'u1'),
                              ('display_type', 'u1'), ('precision', 'u2')])

    def __init__(self, name:str = "QuoteConn", host: str = FeedConn.host, port: int = port):
        super().__init__(name, host, port)
        self._set_price_update_metadata()
        self._set_message_mappings()

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

        self._update_fields = []
        self._current_update_fields = []
        self._fundamental_fields = []
        self._update_names = []
        self._update_dtype = []
        self._set_price_update_metadata()

    def _set_price_update_metadata(self):
        self._fundamental_fields = ["F", "Symbol", "Exchange ID", "PE", "Average Volume", "52 Week High", "52 Week Low",
                                    "Calendar year high", "Calendar year low",
                                    "Dividend yield", "Dividend amount", "Dividend rate", "Pay date", "Ex-dividend date",
                                    "(Reserved)", "(Reserved)", "(Reserved)", "Short Interest", "(Reserved)",
                                    "Current year earnings per share", "Next year earnings per share",
                                    "Five-year growth percentage", "Fiscal year end", "(Reserved)", "Company name",
                                    "Root Option symbol", "Percent held by institutions", "Beta", "Leaps",
                                    "Current assets", "Current liabilities", "Balance sheet date", "Long-term debt",
                                    "Common shares outstanding", "(Reserved)", "Split factor 1", "Split factor 2",
                                    "(Reserved)", "(Reserved)", "Format Code", "Precision", "SIC",
                                    "Historical Volatility", "Security Type", "Listed Market",
                                    "52 Week High Date", "52 Week Low Date",
                                    "Calendar Year High Date", "Calencar Year Low Date",
                                    "Year End Close", "Maturity Date", "Coupon Rate", "Expiration Date",
                                    "Strike Price", "NAICS", "Exchange Root"]

        self._update_fields = ["7 Day Yield",
                               "Ask", "Ask Change", "Ask Market Center", "Ask Size", "Ask Time",
                               "Available Regions", "Average Maturity",
                               "Bid", "Bid Change", "Bid Market Center", "Bid Size", "Bid Time",
                               "Chnage", "Change From Open", "Close", "Close Range 1", "Close Range 2",
                               "Days to Expiration", "Decimal Precision", "Delay", "Exchange ID",
                               "Extended Trade", "Extended Trade Date", "Extended Trade Market Center",
                               "Extended Trade Size", "Extended Trade Time", "Extended Trading Change",
                               "Extended Trading Difference",
                               "Financial Status Indicator",
                               "Fraction Display Code",
                               "High",
                               "Last", "Last Date", "Last Market Center", "Last Size", "Last Time", "Last Trade Date",
                               "Low",
                               "Market Capitalization",
                               "Market Open", "Message Contents",
                               "Most Recent Trade", "Most Recent Trade Conditions", "Most Recent Trade Date",
                               "Most Recent Trade Market Center", "Most Recent Trade Size", "Most Recent Trade Time",
                               "Net Asset Value", "Number of Trades Today", "Open", "Open Range 1", "Open Range 2",
                               "Percent Change", "Percent Off Average Volume", "Previous Day Volume",
                               "Price-Earnings Ratio", "Range", "Restricted Code", "Settle", "Settlement Date", "Spread",
                               "Symbol", "Tick", "TickID", "Total Volume", "Type", "Volatility", "VWAP"]

        self._current_update_fields = ["Type", "Symbol", "Most Recent Trade", "Most Recent Trade Size",
                                       "Most Recent Trade Time", "Most Recent Trade Market Center","Total Volume",
                                       "Bid", "Bid Size", "Ask", "Ask Size", "Open", "High", "Low", "Close",
                                       "Message Contents", "Most Recent Trade Conditions"]


    def process_invalid_symbol(self, fields: Sequence[str]) -> None:
        assert len(fields) > 1
        assert fields[0] == 'n'
        bad_sym = fields[1]
        print("process_invalid_symbol: %s" % bad_sym)
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
        quote = np.empty(shape=(1), dtype=QuoteConn.rgnl_dtype)
        quote["ticker"] = fields[1]
        quote["rgn_bid"] = float(fields[3])
        quote["rgn_bid_sz"] = int(fields[4])
        quote["rgn_bid_tm"] = hh_mm_ss_to_secs_since_midnight(fields[5])
        quote["rgn_ask"] = float(fields[6])
        quote["rgn_ask_sz"] = int(fields[7])
        quote["rgn_ask_tm"] = hh_mm_ss_to_secs_since_midnight(fields[8])
        quote["mkt_center"] = int(fields[9])
        quote["display_type"] = int(fields[10])
        quote["precision"] = int(fields[11])
        for listener in self._listeners:
            listener.process_regional_quote(quote)

    def process_summary(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == "P"
        print("Default process_summary: %s" % ",".join(fields))
        update_dict = self.create_update_dict(fields[1:])
        for listener in self._listeners:
            listener.process_summary(update_dict)

    def process_update(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == "Q"
        print("Default process_update: %s" % ",".join(fields))
        update_dict = self.create_update_dict(fields[1:])
        for listener in self._listeners:
            listener.process_update(update_dict)

    def create_update_dict(self, fields: Sequence[str]) -> dict:
        update_dict = {}
        if len(self._current_update_fields) > 0:
            for i in range(len(fields)):
                update_dict[self._current_update_fields[i]] = fields[i]
        return update_dict

    def process_fundamentals(self, fields: Sequence[str]):
        assert len(fields) > 55
        assert fields[0] == 'F'
        print("process_fundamentals: %s" % ",".join(fields))

        symbol = fields[1]
        pe = read_float(fields[3])
        avg_vlm = read_int(fields[4])
        hi_52wk = read_float(fields[5])
        lo_52wk = read_float(fields[6])
        hi_cal_yr = read_float(fields[7])
        lo_cal_yr = read_float(fields[8])

        div_yld = read_float(fields[9])
        div_amt = read_float(fields[10])
        div_rate = read_float(fields[11])
        pay_dt = read_mm_dd_yyyy(fields[12])
        ex_div_dt = read_mm_dd_yyyy(fields[13])

        short_int = read_int(fields[17])

        eps_cur_yr = read_float(fields[19])
        eps_next_yr = read_float(fields[20])
        growth_5_yr = read_float(fields[21])
        yr_end = read_int(fields[22])

        name = fields[24]
        root_opt_syms = fields[25].split(" ")
        inst_hld_pcnt = read_float(fields[26])
        beta = read_float(fields[27])
        leaps = fields[28].split(" ")

        current_assets = read_float(fields[29])
        current_liabs = read_float(fields[30])
        balance_sheet_dt = read_mm_dd_yyyy(fields[31])
        long_term_debt = read_float(fields[32])
        shares_outstanding = read_float(fields[33])

        (split_factor_1, split_date_1) = read_split_string(fields[35])
        (split_factor_2, split_date_2) = read_split_string(fields[36])

        display_format = read_int(fields[39])
        precision = read_int(fields[40])

        sic = read_int(fields[41])
        hist_vol = read_float(fields[42])
        sec_type = read_int(fields[43])
        listed_mkt =  read_int(fields[44])
        hi_dt_52wk = read_mm_dd_yyyy(fields[45])
        lo_dt_52wk = read_mm_dd_yyyy(fields[46])
        hi_dt_cal_yr = read_mm_dd_yyyy(fields[47])
        lo_dt_cal_yr = read_mm_dd_yyyy(fields[48])
        yr_end_close = read_float(fields[49])
        mat_dt = read_mm_dd_yyyy(fields[50])
        coupon_rate = read_float(fields[51])
        exp_dt = read_mm_dd_yyyy(fields[52])
        strike_price = read_float(fields[53])
        naics = read_int(fields[54])
        root_sym = fields[55]

        fund_msg = {"symbol": symbol, "name": name, "root_symbol": root_sym,
                    "root_opt_syms": root_opt_syms, "leaps": leaps,
                    "sec_type": sec_type, "listed_mkt": listed_mkt, "exchange_root": exchange_root,
                    "strike_price": strike_price, "exp_dt": exp_dt,
                    "coupon_rate": coupon_rate, "mat_dt": mat_dt,

                    "avg_vlm": avg_vlm,
                    "hi_52_wk": hi_52wk, "hi_dt_52_wk": hi_dt_52wk,
                    "lo_52_wk": lo_52wk, "lo_dt_52_wk": lo_dt_52wk,
                    "hi_cal_yr": hi_cal_yr, "hi_dt_cal_yr": hi_dt_cal_yr,
                    "lo_cal_yr": lo_cal_yr, "lo_dt_cal_yr": lo_dt_cal_yr,
                    "yr_end_close": yr_end_close,

                    "beta": beta, "hist_vol": hist_vol,
                    "short_int": short_int, "shares_outstanding": shares_outstanding, "inst_held_pcnt": inst_hld_pcnt,

                    "pe": pe, "eps_cur_yr": eps_cur_yr, "eps_next_yr": eps_next_yr, "growth_5_yr": growth_5_yr,
                    "year_end_month": yr_end,

                    "div_yld": div_yld, "div_amt": div_amt, "div_rate": div_rate, "pay_dt": pay_dt, "ex_div_dt": ex_div_dt,

                    "current_assets": current_assets, "current_liabs": current_liabs,
                    "balance_sheet_dt": balance_sheet_dt,
                    "long_term_debt": long_term_debt,

                    "split_factor_1": split_factor_1, "split_date_1": split_date_1,
                    "split_factor_2": split_factor_2, "split_date_2": split_date_2,

                    "display_format": display_format, "precision": precision,

                    "sic": sic, "naics": naics}

        for listener in self._listeners:
            listener.process_fundamentals(fund_msg)

    def process_auth_key(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2:
        assert fields[0] == "S"
        assert fields[1] = "KEY"
        auth_key = fields[2]
        print("process_auth_key: %s" % auth_key)
        for listener in self._listeners:
            listener.process_auth_key(auth_key)

    def process_keyok(self, fields: Sequence[str]) -> None:
        assert len(fields) > 1
        assert fields[0] == 'S'
        assert fields[1] == "KEYOK"
        print("process_keyok")
        for listener in self._listeners:
            listener.process_keyok()

    def process_customer_info(self, fields: Sequence[str]) -> None:
        assert len(fields) > 11:
        assert fields[0] == 'S'
        assert fields[1] == "CUST"
        print("process_customer_info: %s" % ",".join(fields))
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
        print("process_symbol_limit_reached: %s" % sym)
        for listener in self._listeners:
            listener.process_symbol_limit_reached(sym)

    def process_ip_addresses_used(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'IP'
        ip = fields[2]
        print("process_ip_addresses_used: %s" % ip)
        for listener in self._listeners:
            listener.process_ip_addresses_used(ip)

    def process_fundamental_fieldnames(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'FUNDAMENTAL FIELDNAMES'

        # Remove this after debugging
        for field in fields[2:]:
            if field not in self._fundamental_fields:
                print("%s not found in self._fundamental_fieldnames" % field)
        for field in self._fundamental_fields:
            if field not in fields[2:]:
                print("%s not found in FUNDAMENTAL FIELDNAMES message" % field)
        # Remove this after debugging

        self._fundamental_fields = fields[2:]
        print("process_fundamental_fieldnames: %s" % ",".join(self._fundamental_fields))
        for listener in self._listeners:
            listener.process_fundamental_fieldnames(self._fundamental_fields)

    def process_update_fieldnames(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'UPDATE FIELDNAMES'

        # Remove this after debugging
        for field in fields[2:]:
            if field not in self._update_fields:
                print("%s not found in self._update_fieldnames" % field)
        for field in self._update_fields:
            if field not in fields[2:]:
                print("%s not found in UPDATE FIELDNAMES message" % field)
        # Remove this after debugging

        self._update_fields = fields[2:]
        print("process_update_fieldnames: %s" % ",".join(self._update_fields))
        for listener in self._listeners:
            listener.process_update_fieldnames(self._update_fields)

    def process_current_update_fieldnames(self, fields: Sequence[str]) -> None:
        assert len(fields) > 2
        assert fields[0] == 'S'
        assert fields[1] == 'CURRENT UPDATE FIELDNAMES'
        for field in fields[2:]:
            if field not in self._update_fields:
                raise RuntimeError("Got an %s as an update field. Field not in self._update_fieldnames" % field)

        # Update dtype string, field reading functions etc here.
        self._current_update_fields = fields[2:]
        print("process_current_update_fieldnames: %s" % ",".join(self._current_update_fields))
        for listener in self._listeners:
            listener.process_current_update_fieldnames(self._current_update_fields)

    def watch(self, symbol: str) -> None:
        self.send_cmd("w%s\r\n" % symbol)

    def trades_watch(self, symbol: str) -> None:
        self.send_cmd("t%s\r\n" % symbol)

    def unwatch(self, symbol: str) -> None:
        self.send_cmd("r%s\r\n" % symbol)

    def refresh(self, symbol: str) -> None:
        self.send_cmd("f%s\r\n" % symbol)

    def req_timestamp(self) -> None:
        self.send_cmd("T\r\n")

    def timestamp_on(self) -> None:
        self.send_cmd("S,TIMESTAMPSON\r\n")

    def timestamp_off(self) -> None:
        self.send_cmd("S,TIMESTAMPSOFF\r\n")

    def regional_on(self, symbol: str) -> None:
        self.send_cmd("S,REGON,%s\r\n" % symbol)

    def regional_off(self, symbol: str) -> None:
        self.send_cmd("S,REGOFF,%s\r\n" % symbol)

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
        self.send_cmd("S,SELECT UPDATE FIELDS,%s\r\n" % ",".join(field_names))

    def set_log_levels(self, log_levels: Sequence[str]) -> None:
        self.send_cmd("S,SET LOG LEVELS,%s\r\n" % ",".join(log_levels))

    def request_watches(self) -> None:
        self.send_cmd("S,REQUEST WATCHES\r\n")

    def unwatch_all(self) -> None:
        self.send_cmd("S,UNWATCH ALL")


if __name__ == "__main__":
    from service import FeedService
    from passwords import dtn_login, dtn_password, dtn_product_id

    svc = FeedService(product=dtn_product_id, version="Debugging", login=dtn_login, password=dtn_password)
    svc.launch()

    conn = QuoteConn(name="RunningInIDE")
    conn.start_runner()

    conn.request_all_update_fieldnames()
    conn.request_current_update_fieldnames()
    conn.request_fundamental_fieldnames()

    conn.stop_runner()

    


