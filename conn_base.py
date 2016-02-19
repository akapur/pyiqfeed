import select
import socket
import threading
import datetime
import time
from typing import Sequence


def read_float(float_str: str) -> float:
    if float_str != "":
        return float(str)
    else:
        return None


def read_int(int_str: str) -> int:
    if int_str != "":
        return int(str)
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


def time_to_hhmmss(time: datetime.time) -> str:
    if time is not None:
        return "%.2d%.2d%.2d" % (flt.hour, flt.minute, flt.second)
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


class FeedConn:

    host = "127.0.0.1"
    lookup_port = 9100
    depth_port = 9200
    deriv_port = 9400
    protocol = "5.2"

    def __init__(self, name: str, host: str, port: int):
        self._stop = False
        self._started = False
        self._pf_dict = {}
        self._sm_dict = {}
        self._listeners = []
        self._buf_lock = threading.RLock()
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
        self._sock.sendall(cmd.encode(encoding='utf-8', errors='strict'))

    def connect(self, host, port) -> None:
        self._host = host
        self._port = port
        self._sock.connect((host, port))
        self.set_protocol(FeedConn.protocol)
        self.send_connect_message()
        self.set_client_name(self._name)

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

    def running(self):
        return self._started

    def __call__(self):
        try:
            while not self._stop:
                self.read_messages()
                self.process_messages()
        finally:
            self._started = False

    def _set_message_mappings(self):
        self._pf_dict['E'] = self.process_error
        self._pf_dict['T'] = self.process_timestamp
        self._pf_dict['S'] = self.process_system_message

        self._sm_dict["SERVER DISCONNECTED"] = self.process_server_disconnected
        self._sm_dict["SERVER CONNECTED"] = self.process_server_connected
        self._sm_dict["SERVER RECONNECT FAILED"] = self.process_reconnect_failed
        self._sm_dict["CURRENT PROTOCOL"] = self.process_current_protocol
        self._sm_dict["STATS"] = self.process_conn_stats

    def read_messages(self):
        ready_list = select.select([self._sock], [], [self._sock], 30)
        if ready_list[2]:
            raise RuntimeError("There was a problem with the socket for QuoteReader: %s," % self._name)
        if ready_list[0]:
            with self._buf_lock:
                self._recv_buf += self._sock.recv(16384).decode()

    @property
    def recvd_messages(self):
        with self._buf_lock:
            next_delim = self._recv_buf.find('\n')
            if next_delim != -1:
                message = self._recv_buf[:next_delim].strip()
                self._recv_buf = self._recv_buf[(next_delim + 1):]
                yield message

    def process_messages(self):
        with self._buf_lock:
            for message in self.recvd_messages:
                dispatch_func = self._pf_dict[message[0]]
                dispatch_func(message)

    def process_system_message(self, msg: str) -> None:
        assert msg[0:2] == "S,"
        has_param = False
        param_str = ""
        msg_name_delim = msg.find(',')
        if (-1 != msg_name_delim) and (len(msg) > (msg_name_delim + 2)):
            has_param = True
            param_str = msg[(1 + msg_name_delim):]
        if -1 == msg_name_delim:
            msg_name_delim = len(msg)
        msg_name = msg[2:msg_name_delim]
        processing_func = self._sm_dict[msg_name]
        if has_param:
            processing_func(param_str)
        else:
            processing_func()

    def process_current_protocol(self, protocol: str) -> None:
        if protocol != FeedConn.protocol:
            raise RuntimeError("Desired Protocol %s, Server Says Protocol %s" % (FeedConn.protocol, protocol))
        for listener in self._listeners:
            listener.process_current_protocol(protocol)

    def process_server_disconnected(self) -> None:
        for listener in self._listeners:
            listener.process_server_disconnected()

    def process_server_connected(self) -> None:
        for listener in self._listeners:
            listener.process_server_connected()

    def process_reconnect_failed(self) -> None:
        for listener in self._listeners:
            listener.process_reconnect_failed()

    def process_conn_stats(self, msg: str) -> None:
        (server_ip, server_port_str, max_sym_str, num_sym_str, num_clients_str,
         secs_since_update_str, num_reconn_str, num_fail_conn_str,
         conn_tm_str, mkt_tm_str, status_str, feed_version, login,
         kbs_recv_str, kbps_recv_str, avg_kbps_recv_str,
         kbs_sent_str, kbps_sent_str, avg_kbps_sent_str) = msg[7:].split(",")
        conn_tm = time.strptime("%s EST" % conn_tm_str, "%b %d %I:%M%p %Z")
        mkt_tm = time.strptime("%s EST" % mkt_tm_str, "%b %d %I:%M%p %Z")
        status = False
        if status_str == "Connected":
            status = True
        conn_stats = {"server_ip": server_ip, "server_port": int(server_port_str),
                      "max_sym": int(max_sym_str), "num_sym": int(num_sym_str),
                      "num_clients": int(num_clients_str),
                      "secs_since_update": int(secs_since_update_str),
                      "num_recon": int(num_reconn_str), "num_fail_recon": int(num_fail_conn_str),
                      "conn_tm": conn_tm, "mkt_tm": mkt_tm, "status": status,
                      "feed_version": feed_version, "login": login,
                      "kbs_recv": float(kbs_recv_str), "kbps_recv": float(kbps_recv_str),
                      "avg_kbps_recv": float(avg_kbps_recv_str),
                      "kbs_sent": float(kbs_sent_str), "kbps_sent": float(kbps_sent_str),
                      "avg_kbps_sent": float(avg_kbps_sent_str)}
        for listener in self._listeners:
            listener.process_conn_stats(conn_stats)

    def process_timestamp(self, msg: str) -> None:
        assert msg[0:2] == 'T,'
        time_val = time.strptime("%s EST" % msg[2:], "%Y%m%d %H:%M:%S %Z")
        for listener in self._listeners:
            listener.process_timestamp(time_val)

    def process_error(self, msg: str) -> None:
        assert msg[0:2] == 'E,'
        for listener in self._listeners:
            listener.process_error(msg[2:])

    def add_listener(self, listener) -> None:
        if listener not in self._listeners:
            self._listeners.append(listener)

    def remove_listener(self, listener) -> None:
        if listener in self._listeners:
            self._listeners.remove(listener)

    def set_protocol(self, protocol) -> None:
        self.send_cmd("S,SET PROTOCOL,%s\r\n" % protocol)

    def send_connect_message(self) -> None:
        self.send_cmd("S,CONNECT\r\n")

    def send_disconnect_message(self) -> None:
        self.send_cmd("S,DISCONNECT\r\n")

    def set_client_name(self, name) -> None:
        self._name = name
        self.send_cmd("S,SET CLIENT NAME,%s\r\n" % name)


class QuoteConn(FeedConn):
    port = 5009

    def __init__(self, name, host, port):
        super().__init__(name, host, port)
        self._fundamental_fields = []
        self._update_fields = []
        self._all_update_fields = []
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
        self._sm_dict["UPDATE FIELDNAMES"] = self.process_all_update_fieldnames
        self._sm_dict["CURRENT UPDATE FIELDNAMES"] = self.process_current_update_fieldnames

    def process_invalid_symbol(self, msg: str) -> None:
        assert msg[0:2] == "n,"
        for listener in self._listeners:
            listener.process_no_symbol_error(msg[2:])

    def process_news(self, msg):
        assert msg[0:2] == "N,"
        (N, distributor, story_id, symbol_list, story_time, headline) = msg.split(',')
        story_time = read_yyyymmdd_hhmmss(story_time)
        news_dict = {"distributor": distributor,
                     "story_id": story_id,
                     "symbol_list": symbol_list,
                     "story_time": story_time,
                     "headline": headline}
        for listener in self._listeners:
            listener.process_news(news_dict)

    def process_regional_quote(self, msg):
        assert msg[0:2] == "R,"
        (R, sym, exch_dep,
         bid_reg, bid_sz_reg, bid_tm_reg, ask_reg, ask_sz_reg, ask_tm_reg,
         display_format, precision, mkt_center) = msg.split(",")
        bid_reg = read_float(bid_reg)
        bid_sz_reg = read_int(bid_sz_reg)
        bid_tm_reg = read_hhmmss(bid_tm_reg)
        ask_reg = read_float(ask_reg)
        ask_sz_reg = read_int(ask_sz_reg)
        ask_tm_reg = read_hhmmss(ask_tm_reg)
        reg_dict = {"symbol": sym,
                    "bid": bid_reg, "bid_sz": bid_sz_reg, "bid_tm": bid_tm_reg,
                    "ask": ask_reg, "ask_sz": ask_sz_reg, "ask_tm": ask_tm_reg,
                    "mkt_center": mkt_center,
                    "disp_fmt": display_format, "precision": precision}
        for listener in self._listeners:
            listener.process_regional_quote(reg_dict)

    def process_summary(self, msg: str) -> None:
        assert msg[0:2] == "P,"
        update_dict = self.create_update_dict(msg)
        for listener in self._listeners:
            listener.process_summary(update_dict)

    def process_update(self, msg: str) -> None:
        assert msg[0:2] == "Q,"
        update_dict = self.create_update_dict(msg)
        for listener in self._listeners:
            listener.process_update(update_dict)

    def create_update_dict(self, msg: str) -> dict:
        msg_list = msg[2:].split(',')
        update_dict = {}
        if len(self._update_fields) > 0:
            for i in range(len(msg_list)):
                update_dict[self._update_fields[i]] = msg_list[i]
        return update_dict

    def process_fundamentals(self, msg):
        assert msg[0:2] == "F,"
        (f,
         symbol, exch_id_dep,
         pe, avg_vlm, hi_52wk, lo_52wk, hi_cal_yr, lo_cal_yr,
         div_yld, div_amt, div_rate, pay_dt, ex_div_dt,
         res_15, res_16, res_17,
         short_int,
         res_19,
         eps_cur_yr, eps_next_yr, growth_5_yr, yr_end,
         res_24,
         name, root_opt_syms, inst_hld_pcnt, beta, leaps,
         current_assets, current_liabs, balance_sheet_dt, long_term_debt, shares_outstanding,
         res_35,
         split1, split2,
         res_38, res_39,
         display_format, precision,
         sic,
         hist_vol, sec_type, listed_mkt,
         hi_dt_52wk, lo_dt_52wk, hi_dt_cal_yr, lo_dt_cal_yr, yr_end_close, mat_dt,
         coupon_rate, exp_dt, strike_price,
         naics, exchange_root) = msg.split(',')

        pe = read_float(pe)
        avg_vlm = read_int(avg_vlm)
        hi_52wk = read_float(hi_52wk)
        lo_52wk = read_float(lo_52wk)
        hi_cal_yr = read_float(hi_cal_yr)
        lo_cal_yr = read_float(lo_cal_yr)

        div_yld = read_float(div_yld)
        div_amt = read_float(div_amt)
        div_rate = read_float(div_rate)
        pay_dt = read_mm_dd_yyyy(pay_dt)
        ex_div_dt = read_mm_dd_yyyy(ex_div_dt)

        short_int = read_int(short_int)
        eps_cur_yr = read_float(eps_cur_yr)
        eps_next_yr = read_float(eps_next_yr)
        growth_5_yr = read_float(growth_5_yr)
        yr_end = read_int(yr_end)

        inst_hld_pcnt = read_float(inst_hld_pcnt)
        beta = read_float(beta)

        current_assets = read_float(current_assets)
        current_liabs = read_float(current_liabs)
        balance_sheet_dt = read_mm_dd_yyyy(balance_sheet_dt)
        long_term_debt = read_float(long_term_debt)
        shares_outstanding = read_float(shares_outstanding)

        (split_factor_1, split_date_1) = read_split_string(split1)
        (split_factor_2, split_date_2) = read_split_string(split2)

        precision = read_int(precision)

        sic = read_int(sic)
        hist_vol = read_float(hist_vol)
        hi_dt_52wk = read_mm_dd_yyyy(hi_dt_52wk)
        lo_dt_52wk = read_mm_dd_yyyy(lo_dt_52wk)
        hi_dt_cal_yr = read_mm_dd_yyyy(hi_dt_cal_yr)
        lo_dt_cal_yr = read_mm_dd_yyyy(lo_dt_cal_yr)
        yr_end_close = read_float(yr_end_close)
        mat_dt = read_mm_dd_yyyy(mat_dt)
        coupon_rate = read_float(coupon_rate)
        exp_dt = read_mm_dd_yyyy(exp_dt)
        strike_price = read_float(strike_price)
        naics = read_int(naics)

        fund_msg = {"symbol": symbol, "name": name,
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

    def process_auth_key(self, auth_key) -> None:
        for listener in self._listeners:
            listener.process_auth_key(auth_key)

    def process_keyok(self) -> None:
        for listener in self._listeners:
            listener.process_keyok()

    def process_customer_info(self, msg: str) -> None:
        (svc_t_str, ip_add, port_str, token, version, dep_1, rt_exchanges, dep_2, max_sym_str, flags, dep_3,
         dep_4) = msg.split(",")
        svc_t = (svc_t_str == "real_time")
        msg_dict = {"svc_t": svc_t,
                    "ip_add": ip_add, "port": int(port_str),
                    "token": token, "version": version,
                    "rt_exch": rt_exchanges, "max_sym": int(max_sym_str),
                    "flags": flags}
        for listener in self._listeners:
            listener.process_customer_info(msg_dict)

    def process_symbol_limit_reached(self, sym: str) -> None:
        for listener in self._listeners:
            listener.process_symbol_limit_reached(sym)

    def process_ip_addresses_used(self, addresses: str) -> None:
        for listener in self._listeners:
            listener.process_ip_addresses_used(addresses)

    def process_fundamental_fieldnames(self, msg: str) -> None:
        fields = msg.split(',')
        self._fundamental_fields = fields
        for listener in self._listeners:
            listener.process_fundamental_fieldnames(fields)

    def process_all_update_fieldnames(self, msg: str) -> None:
        fields = msg.split(',')
        self._all_update_fields = fields
        for listener in self._listeners:
            listener.process_all_update_fieldnames(fields)

    def process_current_update_fieldnames(self, msg: str) -> None:
        fields = msg.split(',')
        for listener in self._listeners:
            listener.process_current_update_fieldnames(fields)
        self._update_fields = fields

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
