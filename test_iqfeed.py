import unittest
from service import *
from conn import *
import datetime

autoconnect=True
savelogininfo=True


class TestReadFunctions(unittest.TestCase):
    def test_read_float(self):
        self.assertEqual(2.0, read_float("2.0"))
        self.assertEqual(100.12, read_float("100.12"))
        self.assertEqual(None, read_float(""))

    def test_read_int(self):
        self.assertEqual(121, read_int("121"))
        self.assertEqual(-2, read_int("-2"))
        self.assertEqual(None, read_int(""))


class TestHistoryConn(unittest.TestCase):
    def setup(self):
        self.feed = FeedService(product = dtn_product_id, version = version, login = dtn_login, password = dtn_password)
        self.feed.launch()
        self.hist = HistoryConn("Unit Tester")
        self.hist.start_runner()

    def tearDown(self):
        self.hist.stop_runner()

    def test_latest_ticks(self):
        data = self.hist.request_ticks(ticker="INTC", max_ticks=10, ascend=False, timeout=30)
        self.assertEqual(10, data.shape[0])
        print("10 most recent ticks for INTC:")
        print(data)

        rn = datetime.datetime.now()
        end_prd = datetime.datetime(year=rn.year, month=rn.month, day=rn.day, hour=9, minute=30, second=0)
        bgn_prd = end_prd - datetime.timedelta(days=8)
        data = self.hist.request_ticks_in_prd(ticker="BRK.A", bgn_prd=bgn_prd, end_prd=end_prd)
        self.assertGreater(data.shape[0], 0)
        print("Last 8 cal days of ticks for BRK.A")
        print(data)

        rn = datetime.datetime.now()
        end_prd = datetime.datetime(year=rn.year, month=rn.month, day=rn.day, hour=16, minute=0, second=0)
        bgn_dt = end_prd - datetime.timedelta(days=8)
        bgn_prd = datetime.datetime(year=bgn_dt.year, month=bgn_dt.month, day=bgn_dt.day, hour=9, minute=30, second=0)
        bgn_flt = datetime.time(11, 30, 0)
        end_flt = datetime.time(11, 35, 0)
        ascend = True
        max_ticks=500
        data = self.hist.request_ticks_in_prd(ticker="MSFT", bgn_prd=bgn_prd, end_prd=end_prd,
                                         bgn_flt=bgn_flt, end_flt=end_flt, ascend=ascend,
                                         max_ticks=max_ticks)
        self.assertEqual(data.shape[0], 500)
        print("Last 8 days of ticks for MSFT between 11:30 and 11:35 in ascending order. Only 500 ticks:")
        print(data)

    def test_bars(self):
        data = self.hist.request_bars(ticker="SPY", interval_len=100, interval_type='t', max_bars=10)
        self.assertEqual(data.shape[0], 10)
        print("Last 10 bars of 100 trades for SPY")
        print(data)

        data = self.hist.request_bars(ticker="SPY", interval_len=10000, interval_type='v', max_bars=10)
        self.assertEqual(data.shape[0], 10)
        print("Last 10 bars of 10000 shares of vlm for SPY")
        print(data)

        data = self.hist.request_bars(ticker="SPY", interval_len=60, interval_type='s', max_bars=10)
        self.assertEqual(data.shape[0], 10)
        print("Last 10 bars of 60 seconds for SPY")
        print(data)

        bgn_flt = datetime.time(9, 30, 0)
        end_flt = datetime.time(9, 31, 0)

        tick_bar_data = self.hist.request_bars_for_days(ticker="INTC", interval_len=10, interval_type='t', days=1,
                                                   bgn_flt=bgn_flt, end_flt=end_flt)
        self.assertGreater(tick_bar_data.shape[0], 0)
        vlm_bar_data = self.hist.request_bars_for_days(ticker="INTC", interval_len=500, interval_type='v', days=1,
                                                  bgn_flt=bgn_flt, end_flt=end_flt)
        self.assertGreater(vlm_bar_data.shape[0], 0)
        sec_bar_data = self.hist.request_bars_for_days(ticker="INTC", interval_len=30, interval_type='s', days=1,
                                                  bgn_flt=bgn_flt, end_flt=end_flt)
        self.assertGreater(sec_bar_data.shape[0], 0)

        rn = datetime.datetime.now() - datetime.timedelta(days=7)
        end_prd = datetime.datetime(year=rn.year, month=rn.month, day=rn.day, hour=16, minute=0, second=0)
        bgn_prd = datetime.datetime(year=rn.year, month=rn.month, day=rn.day, hour=9, minute=30, second=0)
        tick_bar_data = self.hist.request_bars_in_period("INTC", interval_len=10, interval_type='t', bgn_prd=bgn_prd,
                                                    end_prd=end_prd, bgn_flt=bgn_flt, end_flt=end_flt, max_bars=10)
        self.assertGreater(tick_bar_data.shape[0], 0)
        vlm_bar_data = self.hist.request_bars_in_period("INTC", interval_len=5000, interval_type='v', bgn_prd=bgn_prd,
                                                   end_prd=end_prd, bgn_flt=bgn_flt, end_flt=end_flt, max_bars=10)
        self.assertGreater(vlm_bar_data.shape[0], 0)
        sec_bar_data = self.hist.request_bars_in_period("INTC", interval_len=10, interval_type='s', bgn_prd=bgn_prd,
                                                   end_prd=end_prd, bgn_flt=bgn_flt, end_flt=end_flt, max_bars=10)
        self.assertGreater(sec_bar_data.shape[0], 0)

    def test_days(self):
        daily_data = self.hist.request_daily_data(ticker="SPY", num_days=21, ascend=True)
        self.assertEqual(daily_data.shape[0], 21)
        print(daily_data)

        bgn_dt = datetime.date(year=2015, month=12, day=1)
        end_dt = datetime.date(year=2016, month=2, day=17)
        daily_data = self.hist.request_daily_data_for_dates(ticker="SPY", bgn_dt=bgn_dt, end_dt=end_dt, ascend=True)
        self.assertEqual(daily_data.shape[0], 52)
        print(daily_data)

        weekly_data = self.hist.request_weekly_data(ticker="SPY", num_weeks=52, ascend=True)
        self.assertEqual(weekly_data.shape[0], 52)
        print(weekly_data)

        monthly_data = self.hist.request_monthly_data(ticker="SPY", num_months=36, ascend=True)
        self.assertEqual(monthly_data.shape[0], 36)
        print(monthly_data)



if __name__ == '__main__':
    unittest.main()
