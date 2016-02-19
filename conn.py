import numpy as np
import io
from iqfeed.conn_base import *


class AdminConn(FeedConn):
    port = 9300
    host = "127.0.0.1"

    def __init__(self, name:str ="AdminConn", host: str = host, port: int = port):
        super().__init__(name, host, port)
        self._set_message_mappings()

    def _set_message_mappings(self) -> None:
        super()._set_message_mappings()
        self._sm_dict["REGISTER CLIENT APP COMPLETED"] = self.process_register_client_app_completed
        self._sm_dict["REMOVE CLIENT APP COMPLETED"] = self.process_remove_client_app_completed
        self._sm_dict["CURRENT LOGINID"] = self.process_current_login
        self._sm_dict["CURRENT PASSWORD"] = self.process_current_password
        self._sm_dict["LOGIN INFO SAVED"] = self.process_login_info_saved
        self._sm_dict["LOGIN INFO NOT SAVED"] = self.process_login_info_not_saved
        self._sm_dict["AUTOCONNECT ON"] = self.process_autoconnect_on
        self._sm_dict["AUTOCONNECT OFF"] = self.process_autoconnect_off
        self._sm_dict["CLIENTSTATS"] = self.process_client_stats

    def process_register_client_app_completed(self) -> None:
        for listener in self._listeners:
            listener.process_register_client_app_completed()

    def process_remove_client_app_completed(self) -> None:
        for listener in self._listeners:
            listener.process_remove_client_app_completed()

    def process_current_login(self, login) -> None:
        for listener in self._listeners:
            listener.process_current_login(login)

    def process_current_password(self, password) -> None:
        for listener in self._listeners:
            listener.process_current_password(password)

    def process_login_info_saved(self) -> None:
        for listener in self._listeners:
            listener.process_login_info_saved()

    def process_login_info_not_saved(self) -> None:
        for listener in self._listeners:
            listener.process_login_info_not_saved()

    def process_autoconnect_on(self) -> None:
        for listener in self._listeners:
            listener.process_autoconnect_on()

    def process_autoconnect_off(self) -> None:
        for listener in self._listeners:
            listener.process_autoconnect_off()

    def process_client_stats(self, msg: str) -> None:
        [type_int_str, client_id_str, client_name, start_tm_str,
         num_syms_str, num_reg_syms_str,
         kb_sent_str, kb_recvd_str, kb_queued_str , junk] = msg.split(',')

        type_int = read_int(type_int_str)
        client_id = read_int(client_id_str)
        start_tm = read_yyyymmdd_hhmmss(start_tm_str)
        num_syms = read_int(num_syms_str)
        num_reg_syms = read_int(num_reg_syms_str)
        kb_sent = read_float(kb_sent_str)
        kb_recvd = read_float(kb_recvd_str)
        kb_queued = read_float(kb_queued_str)

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
                             "start_tm": start_tm,
                             "kb_sent": kb_sent, "kb_recvd": kb_recvd, "kb_queued": kb_queued}
        if 1 == type_int:
            client_stats_dict["num_quote_subs"] = num_syms
            client_stats_dict["num_reg_subs"] = num_reg_syms
        elif 2 == type_int:
            client_stats_dict["num_depth_subs"] = num_syms
        if client_stats_dict["kb_queued"] > 0.01:
            print(client_stats_dict)

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


class HistoryConn(FeedConn):
    port = 9100

    tick_dtype = np.dtype([('tick_id', 'u8'),
                           ('time', 'datetime64[us]'),
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

    bar_dtype = np.dtype([('time', 'datetime64[s]'),
                          ('open_p', 'f8'),
                          ('high_p', 'f8'),
                          ('low_p', 'f8'),
                          ('close_p', 'f8'),
                          ('tot_vlm', 'u8'),
                          ('prd_vlm', 'u8'),
                          ('num_trds', 'u8')])

    daily_data_dtype = np.dtype([('date', 'datetime64[D]'),
                                 ('open_p', 'f8'),
                                 ('high_p', 'f8'),
                                 ('low_p', 'f8'),
                                 ('close_p', 'f8'),
                                 ('prd_vlm', 'u8'),
                                 ('open_int', 'u8')])

    def __init__(self, name: str = "HistoryConn", host: str = FeedConn.host, port: int = port):
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
        self._pf_dict['H'] = self.process_hist_datum

    def send_connect_message(self):
        # The history socket does not accept connect messages
        pass

    def process_hist_datum(self, datum) -> None:
        req_id = datum[:12]
        if 'E' == datum[13]:
            # Error
            self._req_failed[req_id] = True
            err_msg = "Unknown Error"
            end_err_msg = datum.find(',', 16)
            if end_err_msg != -1:
                err_msg = datum[16:end_err_msg]
            self._req_err[req_id] = err_msg
        elif '!ENDMSG!' == datum[13:21]:
            self._req_event[req_id].set()
        else:
            self._req_buf[req_id] += (datum + '\n')
            self._req_numlines[req_id] += 1

    def _get_next_req_id(self) -> str:
        with self._buf_lock:
            req_id = "H_%.10d" % (self._req_num)
            self._req_num += 1
            return req_id

    def _remove_request_data(self, req_id: str) -> None:
        with self._buf_lock:
            del self._req_failed[req_id]
            del self._req_err[req_id]
            del self._req_buf[req_id]
            del self._req_numlines[req_id]

    def read_ticks(self, req_id: str) -> np.array:
        with self._buf_lock:
            failed = self._req_failed[req_id]
            error_msg = self._req_err[req_id]
            tickdata_buffer = self._req_buf[req_id]
            num_pts = self._req_numlines[req_id]
            self._remove_request_data(req_id)
        if failed:
            return np.array([error_msg], dtype='object')
        else:
            read_buf = io.StringIO(tickdata_buffer)
            data = np.empty(num_pts, HistoryConn.tick_dtype)
            line_num = 0
            line = read_buf.readline()
            while line and (line_num < num_pts):
                [req_id, tick_time, last, last_sz, tot_vlm,
                 bid, ask, tick_id, last_type, mkt_center, cond_str, empty] = line.split(',')
                data[line_num]['tick_id'] = np.uint64(tick_id)
                data[line_num]['time'] = np.datetime64(tick_time, 'us')
                data[line_num]['last'] = np.float64(last)
                data[line_num]['last_sz'] = np.uint64(last_sz)
                data[line_num]['last_type'] = last_type
                data[line_num]['mkt_ctr'] = np.uint32(mkt_center)
                data[line_num]['tot_vlm'] = np.uint64(tot_vlm)
                data[line_num]['bid'] = np.float64(bid)
                data[line_num]['ask'] = np.float64(ask)
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
                line = read_buf.readline()
                if line_num >= num_pts:
                    assert line == ""
                if line == "":
                    assert line_num >= num_pts
            return data

    def request_ticks(self, ticker: str, max_ticks: int, ascend: bool=False, timeout: int=None) -> np.array:
        # HTX,[Symbol],[MaxDatapoints],[DataDirection],[RequestID],[DatapointsPerSend]<CR><LF>
        req_id = self._get_next_req_id()
        self._req_num += 1
        req_cmd = "HTX,%s,%d,%d,%s,\r\n" % (ticker, max_ticks, ascend, req_id)
        request_event = threading.Event()
        with self._buf_lock:
            self._req_buf[req_id] = ""
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_event[req_id] = request_event
            self._req_err[req_id] = ""
        self.send_cmd(req_cmd)
        request_event.wait(timeout = timeout)
        data = self.read_ticks(req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_ticks_for_days(self, ticker: str, num_days: int,
                               bgn_flt: datetime.time=None, end_flt: datetime.time=None,
                               ascend: bool=False, max_ticks: int=None, timeout: int=None) -> np.array:
        # HTD,[Symbol],[Days],[MaxDatapoints],[BeginFilterTime],[EndFilterTime],[DataDirection],[RequestID],[DatapointsPerSend]<CR><LF>
        req_id = self._get_next_req_id()
        bf_str = time_to_hhmmss(bgn_flt)
        ef_str = time_to_hhmmss(end_flt)
        mt_str = int_to_str(max_ticks)
        request_event = threading.Event()
        req_cmd = "HTD,%s,%d,%s,%s,%s,%d,%s,\r\n" % (ticker, num_days, mt_str, bf_str, ef_str, ascend, req_id)
        with self._buf_lock:
            self._req_buf[req_id] = ""
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_event[req_id] = request_event
            self._req_err[req_id] = ""
        self.send_cmd(req_cmd)
        request_event.wait(timeout = timeout)
        data = self.read_ticks(req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_ticks_in_prd(self, ticker: str, bgn_prd: datetime.datetime, end_prd: datetime.datetime,
                             bgn_flt: datetime.time=None, end_flt: datetime.time=None, ascend: bool=False,
                             max_ticks: int=None, timeout: int=None) -> np.array:
        # HTT,[Symbol],[BeginDate BeginTime],[EndDate EndTime],[MaxDatapoints],[BeginFilterTime],[EndFilterTime],[DataDirection],[RequestID],[DatapointsPerSend]<CR><LF>
        req_id = self._get_next_req_id()
        bp_str = datetime_to_yyyymmdd_hhmmss(bgn_prd)
        ep_str = datetime_to_yyyymmdd_hhmmss(end_prd)
        bf_str = time_to_hhmmss(bgn_flt)
        ef_str = time_to_hhmmss(end_flt)
        mt_str = int_to_str(max_ticks)
        req_cmd = "HTT,%s,%s,%s,%s,%s,%s,%d,%s,\r\n" % (ticker, bp_str, ep_str, mt_str, bf_str, ef_str, ascend, req_id)
        request_event = threading.Event()
        with self._buf_lock:
            self._req_buf[req_id] = ""
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_event[req_id] = request_event
            self._req_err[req_id] = ""
        self.send_cmd(req_cmd)
        request_event.wait(timeout = timeout)
        data = self.read_ticks(req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def read_bars(self, req_id: str) -> np.array:
        with self._buf_lock:
            failed = self._req_failed[req_id]
            error_msg = self._req_err[req_id]
            bardata_buffer = self._req_buf[req_id]
            num_pts = self._req_numlines[req_id]
            self._remove_request_data(req_id)
        if failed:
            return np.array([error_msg], dtype='object')
        else:
            read_buf = io.StringIO(bardata_buffer)
            data = np.empty(num_pts, HistoryConn.bar_dtype)
            line_num = 0
            line = read_buf.readline()
            while line and (line_num < num_pts):
                [req_id, bar_time, high_p, low_p, open_p, close_p, tot_vlm, prd_vlm, num_trds, empty] = line.split(',')
                data[line_num]['time'] = np.datetime64(bar_time, 's')
                data[line_num]['open_p'] = np.float64(open_p)
                data[line_num]['high_p'] = np.float64(high_p)
                data[line_num]['low_p'] = np.float64(low_p)
                data[line_num]['close_p'] = np.float64(close_p)
                data[line_num]['tot_vlm'] = np.int64(tot_vlm)
                data[line_num]['prd_vlm'] = np.int64(prd_vlm)
                data[line_num]['num_trds'] = np.int64(num_trds)
                line_num += 1
                line = read_buf.readline()
                if line_num >= num_pts:
                    assert line == ""
                if line == "":
                    assert line_num >= num_pts
            return data

    def request_bars(self, ticker: str, interval_len: int, interval_type: str, max_bars: int,
                    ascend: bool=False, timeout: int=None) -> np.array:
        # HIX,[Symbol],[Interval],[MaxDatapoints],[DataDirection],[RequestID],[DatapointsPerSend],[IntervalType]<CR><LF>
        assert interval_type in ('s', 'v', 't')
        req_id = self._get_next_req_id()
        req_cmd = "HIX,%s,%d,%d,%d,%s,,%s\r\n" % (ticker, interval_len, max_bars, ascend, req_id, interval_type)
        request_event = threading.Event()
        with self._buf_lock:
            self._req_buf[req_id] = ""
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_event[req_id] = request_event
            self._req_err[req_id] = ""
        self.send_cmd(req_cmd)
        request_event.wait(timeout=timeout)
        data = self.read_bars(req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_bars_for_days(self, ticker: str, interval_len: int, interval_type: str, days: int,
                              bgn_flt: datetime.time, end_flt: datetime.time, ascend: bool=False,
                              max_bars: int=None, timeout: int=None) -> np.array:
        # HID,[Symbol],[Interval],[Days],[MaxDatapoints],[BeginFilterTime],[EndFilterTime],[DataDirection],[RequestID],[DatapointsPerSend],[IntervalType]<CR><LF>
        assert interval_type in ('s', 'v', 't')
        req_id = self._get_next_req_id()
        bf_str = time_to_hhmmss(bgn_flt)
        ef_str = time_to_hhmmss(end_flt)
        mb_str = int_to_str(max_bars)
        req_cmd = "HID,%s,%d,%d,%s,%s,%s,%d,%s,,%s\r\n" % (
        ticker, interval_len, days, mb_str, bf_str, ef_str, ascend, req_id, interval_type)
        request_event = threading.Event()
        with self._buf_lock:
            self._req_buf[req_id] = ""
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_event[req_id] = request_event
            self._req_err[req_id] = ""
        self.send_cmd(req_cmd)
        request_event.wait(timeout=timeout)
        data = self.read_bars(req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_bars_in_period(self, ticker: str, interval_len: int, interval_type: str,
                               bgn_prd: datetime.datetime, end_prd: datetime.datetime,
                               bgn_flt: datetime.time=None, end_flt: datetime.time=None, ascend: bool=False,
                               max_bars: int=None, timeout: int=None) -> np.array:
        # HIT,[Symbol],[Interval],[BeginDate BeginTime],[EndDate EndTime],[MaxDatapoints],[BeginFilterTime],[EndFilterTime],[DataDirection],[RequestID],[DatapointsPerSend],[IntervalType]<CR><LF>
        assert interval_type in ('s', 'v', 't')
        req_id = self._get_next_req_id()
        bp_str = datetime_to_yyyymmdd_hhmmss(bgn_prd)
        ep_str = datetime_to_yyyymmdd_hhmmss(end_prd)
        bf_str = time_to_hhmmss(bgn_flt)
        ef_str = time_to_hhmmss(end_flt)
        mb_str = int_to_str(max_bars)

        req_cmd = "HIT,%s,%d,%s,%s,%s,%s,%s,%d,%s,,%s\r\n" % (
        ticker, interval_len, bp_str, ep_str, mb_str, bf_str, ef_str, ascend, req_id, interval_type)
        print(req_cmd)
        request_event = threading.Event()
        with self._buf_lock:
            self._req_buf[req_id] = ""
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_event[req_id] = request_event
            self._req_err[req_id] = ""
        self.send_cmd(req_cmd)
        request_event.wait(timeout=timeout)
        data = self.read_bars(req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def read_days(self, req_id: str) -> np.array:
        with self._buf_lock:
            failed = self._req_failed[req_id]
            error_msg = self._req_err[req_id]
            bardata_buffer = self._req_buf[req_id]
            num_pts = self._req_numlines[req_id]
            self._remove_request_data(req_id)
        if failed:
            return np.array([error_msg], dtype='object')
        else:
            read_buf = io.StringIO(bardata_buffer)
            data = np.empty(num_pts, HistoryConn.daily_data_dtype)
            line_num = 0
            line = read_buf.readline()
            while line and (line_num < num_pts):
                [req_id, date_stamp, high_p, low_p, open_p, close_p, prd_vlm, open_int, empty] = line.split(',')
                data[line_num]['date'] = np.datetime64(date_stamp, 'D')
                data[line_num]['open_p'] = np.float64(open_p)
                data[line_num]['high_p'] = np.float64(high_p)
                data[line_num]['low_p'] = np.float64(low_p)
                data[line_num]['close_p'] = np.float64(close_p)
                data[line_num]['prd_vlm'] = np.uint64(prd_vlm)
                data[line_num]['open_int'] = np.uint64(open_int)
                line_num += 1
                line = read_buf.readline()
                if line_num >= num_pts:
                    assert line == ""
                if line == "":
                    assert line_num >= num_pts
            return data

    def request_daily_data(self, ticker: str, num_days: int, ascend: bool=False, timeout: int=None):
        # HDX,[Symbol],[MaxDatapoints],[DataDirection],[RequestID],[DatapointsPerSend]<CR><LF>
        req_id = self._get_next_req_id()
        req_cmd = "HDX,%s,%d,%d,%s,\r\n" % (ticker, num_days, ascend, req_id)
        request_event = threading.Event()
        with self._buf_lock:
            self._req_buf[req_id] = ""
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_event[req_id] = request_event
            self._req_err[req_id] = ""
        self.send_cmd(req_cmd)
        request_event.wait(timeout=timeout)
        data = self.read_days(req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_daily_data_for_dates(self, ticker: str, bgn_dt: datetime.date, end_dt: datetime.date,
                                     ascend: bool=False, max_days: int=None, timeout: int=None):
        # HDT,[Symbol],[BeginDate],[EndDate],[MaxDatapoints],[DataDirection],[RequestID],[DatapointsPerSend]<CR><LF>
        req_id = self._get_next_req_id()
        bgn_str = date_to_yyyymmdd(bgn_dt)
        end_str = date_to_yyyymmdd(end_dt)
        md_str = int_to_str(max_days)
        req_cmd = "HDT,%s,%s,%s,%s,%d,%s,\r\n" % (ticker, bgn_str, end_str, md_str, ascend, req_id)
        request_event = threading.Event()
        with self._buf_lock:
            self._req_buf[req_id] = ""
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_event[req_id] = request_event
            self._req_err[req_id] = ""
        self.send_cmd(req_cmd)
        request_event.wait(timeout=timeout)
        data = self.read_days(req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_weekly_data(self, ticker: str, num_weeks: int, ascend: bool=False, timeout: int=None):
        # HWX,[Symbol],[MaxDatapoints],[DataDirection],[RequestID],[DatapointsPerSend]<CR><LF>
        req_id = self._get_next_req_id()
        req_cmd = "HWX,%s,%d,%d,%s,\r\n" % (ticker, num_weeks, ascend, req_id)
        request_event = threading.Event()
        with self._buf_lock:
            self._req_buf[req_id] = ""
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_event[req_id] = request_event
            self._req_err[req_id] = ""
        self.send_cmd(req_cmd)
        request_event.wait(timeout=timeout)
        data = self.read_days(req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_monthly_data(self, ticker: str, num_months: int, ascend: bool=False, timeout: int=None):
        # HMX,[Symbol],[MaxDatapoints],[DataDirection],[RequestID],[DatapointsPerSend]<CR><LF>
        req_id = self._get_next_req_id()
        req_cmd = "HMX,%s,%d,%d,%s,\r\n" % (ticker, num_months, ascend, req_id)
        request_event = threading.Event()
        with self._buf_lock:
            self._req_buf[req_id] = ""
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_event[req_id] = request_event
            self._req_err[req_id] = ""
        self.send_cmd(req_cmd)
        request_event.wait(timeout=timeout)
        data = self.read_days(req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data


class LookupConn(FeedConn):
    port = 9100
