import numpy as np
import io
from conn_base import *


class AdminConn(FeedConn):
    port = 9300

    def __init__(self, name, host, port):
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
        (type_int, client_id, client_name, start_tm,
         num_syms, num_reg_syms,
         kb_sent, kb_recvd, kb_queued) = msg.split(',')

        type_int = read_int(type_int)
        client_id = read_int(client_id)
        start_tm = read_yyyymmdd_hhmmss(start_tm)
        num_syms = read_int(num_syms)
        num_reg_syms = read_int(num_reg_syms)
        kb_sent = read_float(kb_sent)
        kb_recvd = read_float(kb_recvd)
        kb_queued = read_float(kb_queued)

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
                             "start_tm": start_tm,
                             "kb_sent": kb_sent, "kb_recvd": kb_recvd, "kb_queued": kb_queued}
        if 1 == type_int:
            client_stats_dict["num_quote_subs"] = num_syms
            client_stats_dict["num_reg_subs"] = num_reg_syms
        elif 2 == type_int:
            client_stats_dict["num_depth_subs"] = num_syms

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

    tick_id_dtype = np.uint64
    tick_time_dtype = np.dtype(np.datetime64, 'ms')
    bar_time_dtype = np.dtype(np.datetime64, 's')
    date_dtype = np.datype(np.datetime64, 'day')
    price_dtype = np.float64
    vlm_dtype = np.uint64
    last_type_dtype = np.dtype(np.str, 1)
    mkt_ctr_dtype = np.uint32
    cond_dtype = np.uint8

    tick_dtype = np.dtype([('tick_id', tick_id_dtype),
                           ('time', tick_time_dtype),
                           ('last', price_dtype),
                           ('last_sz', vlm_dtype),
                           ('last_type', last_type_dtype),
                           ('mkt_ctr', mkt_ctr_dtype),
                           ('tot_vlm', vlm_dtype),
                           ('bid', price_dtype),
                           ('ask', price_dtype),
                           ('cond1', cond_dtype),
                           ('cond2', cond_dtype),
                           ('cond3', cond_dtype),
                           ('cond4', cond_dtype)])

    bar_dtype = np.dtype([('time', bar_time_dtype),
                          ('open_p', price_dtype),
                          ('high_p', price_dtype),
                          ('low_p', price_dtype),
                          ('close_p', price_dtype),
                          ('tot_vlm', vlm_dtype),
                          ('prd_vlm', vlm_dtype),
                          ('num_trds', vlm_dtype)])

    daily_data_dtype = np.dtype([('time', date_dtype),
                                 ('open_p', price_dtype),
                                 ('high_p', price_dtype),
                                 ('low_p', price_dtype),
                                 ('close_p', price_dtype),
                                 ('prd_vlm', vlm_dtype),
                                 ('open_int', vlm_dtype)])




    def __init__(self, name, host, port):
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

    def process_hist_datum(self, datum) -> None:
        req_id = datum[:12]

        if 'E' == datum[14]:
            # Error
            self._req_failed[req_id] = True
            err_msg = "Unknown Error"
            end_err_msg = datum.find(',', 16)
            if end_err_msg != -1:
                err_msg = datum[16:end_err_msg]
            self._req_err[req_id] = err_msg
        elif '!ENDMSG!' == datum[14:22]:
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
        # failed = False
        # error_msg = ""
        # tickdata_buffer = ""
        # num_pts = 0
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
                 bid, ask, tick_id, last_type, mkt_center, cond_str] = line.split(',')
                data[line_num]['tick_id'] = HistoryConn.tick_id_dtype(tick_id)
                data[line_num]['time'] = np.datetime64(tick_time)
                data[line_num]['last'] = HistoryConn.price_dtype(last)
                data[line_num]['last_sz'] = HistoryConn.vlm_dtype(last_sz)
                data[line_num]['last_type'] = np.str(last_type)
                data[line_num]['mkt_ctr'] = HistoryConn.mkt_ctr_dtype(mkt_center)
                data[line_num]['tot_vlm'] = HistoryConn.vlm_dtype(tot_vlm)
                data[line_num]['bid'] = HistoryConn.price_dtype(bid)
                data[line_num]['ask'] = HistoryConn.price_dtype(ask)
                num_cond = len(cond_str) / 2
                if num_cond > 0:
                    data[line_num]['cond1'] = int(cond_str[0:2], 16)
                else:
                    data[line_num]['cond1'] = 0

                if num_cond > 1:
                    data[line_num]['cond2'] = int(cond_str[2:4], 16)
                else:
                    data[line_num]['cond2'] = 0

                if num_cond > 2:
                    data[line_num]['cond3'] = int(cond_str[4:6], 16)
                else:
                    data[line_num]['cond3'] = 0

                if num_cond > 3:
                    data[line_num]['cond4'] = int(cond_str[6:8], 16)
                else:
                    data[line_num]['cond4'] = 0

                line_num += 1
                line = read_buf.readline()
                if line_num >= num_pts:
                    assert line == ""
                if line == "":
                    assert line_num >= num_pts
            return data

    def read_bars(self, req_id: str) -> np.array:
        # failed = False
        # error_msg = ""
        # tickdata_buffer = ""
        # num_pts = 0
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
                [req_id, bar_time, high_p, low_p, open_p, close_p, tot_vlm, prd_vlm, num_trds] = line.split(',')
                data[line_num]['time'] = np.datetime64(bar_time)
                data[line_num]['open_p'] = HistoryConn.price_dtype(open_p)
                data[line_num]['high_p'] = HistoryConn.price_dtype(high_p)
                data[line_num]['low_p'] = HistoryConn.price_dtype(low_p)
                data[line_num]['close_p'] = HistoryConn.price_dtype(close_p)
                data[line_num]['tot_vlm'] = HistoryConn.vlm_dtype(tot_vlm)
                data[line_num]['prd_vlm'] = HistoryConn.vlm_dtype(prd_vlm)
                data[line_num]['num_trds'] = HistoryConn.vlm_dtype(num_trds)
                line_num += 1
                line = read_buf.readline()
                if line_num >= num_pts:
                    assert line == ""
                if line == "":
                    assert line_num >= num_pts
            return data


    def read_days(self, req_id: str) -> np.array:
        # failed = False
        # error_msg = ""
        # tickdata_buffer = ""
        # num_pts = 0
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
                [req_id, date_stamp, high_p, low_p, open_p, close_p, prd_vlm, open_int] = line.split(',')
                data[line_num]['date'] = np.datetime64(date_stamp)
                data[line_num]['open_p'] = HistoryConn.price_dtype(open_p)
                data[line_num]['high_p'] = HistoryConn.price_dtype(high_p)
                data[line_num]['low_p'] = HistoryConn.price_dtype(low_p)
                data[line_num]['close_p'] = HistoryConn.price_dtype(close_p)
                data[line_num]['prd_vlm'] = HistoryConn.vlm_dtype(prd_vlm)
                data[line_num]['open_int'] = HistoryConn.vlm_dtype(open_int)
                line_num += 1
                line = read_buf.readline()
                if line_num >= num_pts:
                    assert line == ""
                if line == "":
                    assert line_num >= num_pts
            return data

    def request_ticks(self, ticker: str, max_ticks: int, ascend: bool = False) -> np.array:
        # HTX,[Symbol],[MaxDatapoints],[DataDirection],[RequestID],[DatapointsPerSend]<CR><LF>
        req_id = self._get_next_req_id()
        self._req_num += 1
        req_cmd = "HTX,%s,%d,%d,%s,\r\n" % (symbol, max_ticks, ascend, req_id)
        request_event = threading.Event()
        with self._buf_lock:
            self._req_buf[req_id] = ""
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_event[req_id] = request_event
            self._req_err[req_id] = ""
        self.send_cmd(req_cmd)
        request_event.wait(timeout=None)
        data = self.read_ticks(self, req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_ticks_for_days(self, ticker: str, num_days: int,
                               bgn_flt: datetime.time = None, end_flt: datetime.time = None,
                               ascend: bool = False, max_ticks: int = None) -> np.array:
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
        request_event.wait(timeout=None)
        data = self.read_ticks(self, req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_ticks_in_period(self, ticker: str, bgn_prd: datetime.datetime, end_prd: datetime.datetime,
                                bgn_flt: datetime.time, end_flt: datetime.time, ascend: bool = False,
                                max_ticks: int = None) -> np.array:
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
        request_event.wait(timeout=None)
        data = self.read_ticks(self, req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_bars(self, ticker: str, interval: int, interval_type: str, max_bars: int,
                     ascend: bool = False) -> np.array:
        # HIX,[Symbol],[Interval],[MaxDatapoints],[DataDirection],[RequestID],[DatapointsPerSend],[IntervalType]<CR><LF>
        assert interval_type in ('s', 'v', 't')
        req_id = self._get_next_req_id()
        req_cmd = "HIX,%s,%d,%d,%d,%s,,%s\r\n" % (ticker, interval, max_bars, ascend, req_id, interval_type)
        request_event = threading.Event()
        with self._buf_lock:
            self._req_buf[req_id] = ""
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_event[req_id] = request_event
            self._req_err[req_id] = ""
        self.send_cmd(req_cmd)
        request_event.wait(timeout=None)
        data = self.read_bars(self, req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_bars_for_days(self, ticker: str, interval: int, interval_type: str, days: int,
                              bgn_flt: datetime.time, end_flt: datetime.time, ascend: bool = False,
                              max_bars: int = None) -> np.array:
        # HID,[Symbol],[Interval],[Days],[MaxDatapoints],[BeginFilterTime],[EndFilterTime],[DataDirection],[RequestID],[DatapointsPerSend],[IntervalType]<CR><LF>
        assert interval_type in ('s', 'v', 't')
        req_id = self._get_next_req_id()
        bf_str = time_to_hhmmss(bgn_flt)
        ef_str = time_to_hhmmss(end_flt)
        mb_str = int_to_str(max_bars)
        req_cmd = "HID,%s,%d,%d,%s,%s,%s,%d,%s,,%s\r\n" % (
        ticker, interval, days, mb_str, bf_str, ef_str, ascend, req_id, interval_type)
        request_event = threading.Event()
        with self._buf_lock:
            self._req_buf[req_id] = ""
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_event[req_id] = request_event
            self._req_err[req_id] = ""
        self.send_cmd(req_cmd)
        request_event.wait(timeout=None)
        data = self.read_bars(self, req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_bars_in_period(self, ticker: str, interval: int, interval_type: str,
                               bgn_prd: datetime.datetime, end_prd: datetime.datetime,
                               bgn_flt: datetime.time, end_flt: datetime.time, ascend: bool = False,
                               max_bars: int = None) -> np.array:
        # HIT,[Symbol],[Interval],[BeginDate BeginTime],[EndDate EndTime],[MaxDatapoints],[BeginFilterTime],[EndFilterTime],[DataDirection],[RequestID],[DatapointsPerSend],[IntervalType]<CR><LF>
        assert interval_type in ('s', 'v', 't')
        req_id = self._get_next_req_id()
        bp_str = datetime_to_yyyymmdd_hhmmss(bgn_prd)
        ep_str = datetime_to_yyyymmdd_hhmmss(end_prd)
        bf_str = time_to_hhmmss(bgn_flt)
        ef_str = time_to_hhmmss(end_flt)
        mb_str = int_to_str(max_bars)

        req_cmd = "HIT,%s,%d,%s,%s,%s,%s,%s,%d,%s,,%s\r\n" % (
        ticker, interval, bp_str, ep_str, mb_str, bf_str, ef_str, ascend, req_id, interval_type)
        request_event = threading.Event()
        with self._buf_lock:
            self._req_buf[req_id] = ""
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_event[req_id] = request_event
            self._req_err[req_id] = ""
        self.send_cmd(req_cmd)
        request_event.wait(timeout=None)
        data = self.read_bars(self, req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_daily_data(self, ticker: str, num_days: int, ascend: bool = False):
        # HDX,[Symbol],[MaxDatapoints],[DataDirection],[RequestID],[DatapointsPerSend]<CR><LF>
        req_id = self._get_next_req_id()
        req_cmd = "HDX,%s,%d,%d,%s,,\r\n" % (ticker, num_days, ascend, req_id)
        request_event = threading.Event()
        with self._buf_lock:
            self._req_buf[req_id] = ""
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_event[req_id] = request_event
            self._req_err[req_id] = ""
        self.send_cmd(req_cmd)
        request_event.wait(timeout=None)
        data = self.read_days(self, req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_daily_data_for_dates(self, ticker: str, bgn_dt: datetime.date, end_dt: datetime.date, ascend: bool,
                                     max_days: int = None):
        # HDT,[Symbol],[BeginDate],[EndDate],[MaxDatapoints],[DataDirection],[RequestID],[DatapointsPerSend]<CR><LF>
        req_id = self._get_next_day_id()
        bgn_str = date_to_yyyymmdd(bgn_dt)
        end_str = date_to_yyyymmdd(end_dt)
        md_str = int_to_str(max_days)
        req_cmd = "HDT,%s,%s,%s,%s,%d,%s,,\r\n" % (ticker, bgn_str, end_str, md_str, ascend, req_id)
        request_event = threading.Event()
        with self._buf_lock:
            self._req_buf[req_id] = ""
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_event[req_id] = request_event
            self._req_err[req_id] = ""
        self.send_cmd(req_cmd)
        request_event.wait(timeout=None)
        data = self.read_days(self, req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_weekly_data(self, ticker: str, num_weeks: int, ascend: bool = False):
        # HWX,[Symbol],[MaxDatapoints],[DataDirection],[RequestID],[DatapointsPerSend]<CR><LF>
        req_id = self._get_next_req_id()
        req_cmd = "HWX,%s,%d,%d,%s,,\r\n" % (ticker, num_weeks, ascend, req_id)
        request_event = threading.Event()
        with self._buf_lock:
            self._req_buf[req_id] = ""
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_event[req_id] = request_event
            self._req_err[req_id] = ""
        self.send_cmd(req_cmd)
        request_event.wait(timeout=None)
        data = self.read_days(self, req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data

    def request_monthly_data(self, ticker: str, num_months: int, ascend: bool = False):
        # HMX,[Symbol],[MaxDatapoints],[DataDirection],[RequestID],[DatapointsPerSend]<CR><LF>
        req_id = self._get_next_req_id()
        req_cmd = "HMX,%s,%d,%d,%s,,\r\n" % (ticker, num_months, ascend, req_id)
        request_event = threading.Event()
        with self._buf_lock:
            self._req_buf[req_id] = ""
            self._req_numlines[req_id] = 0
            self._req_failed[req_id] = False
            self._req_event[req_id] = request_event
            self._req_err[req_id] = ""
        self.send_cmd(req_cmd)
        request_event.wait(timeout=None)
        data = self.read_days(self, req_id)
        if data.dtype == object:
            err_msg = "Request: %s, Error: %s" % (req_cmd, str(data[0]))
            raise RuntimeError(err_msg)
        else:
            return data
