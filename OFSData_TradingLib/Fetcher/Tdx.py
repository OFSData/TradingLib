from OFSData_TradingLib.Base.Utils import get_code_type, get_code_market, get_index_market, CODE_MARKET_SH, CODE_MARKET_SZ
from OFSData_TradingLib.Base.Datetime import days_trade_range, days_trade_between
from OFSData_TradingLib.Base.Paralle import ThreadTasks
from OFSData_TradingLib.Fetcher.Base import Base
from pytdx.hq import TdxHq_API
from pytdx.exhq import TdxExHq_API
from fastcache import lru_cache
from retrying import retry
import datetime, pandas

class  Tdx(Base):
    # K线种类
    # 0 -   5 分钟K 线
    KLINE_5MIN = 0
    # 1 -   15 分钟K 线
    KLINE_15MIN = 1
    # 2 -   30 分钟K 线
    KLINE_30MIN = 2
    # 3 -   1 小时K 线
    KLINE_1HOUR = 3
    # 4 -   日K 线
    KLINE_DAILY = 4
    # 5 -   周K 线
    KLINE_WEEKLY = 5
    # 6 -   月K 线
    KLINE_MONTHLY = 6
    # 7 -   扩展市场 1 分钟
    KLINE_EX_1MIN = 7
    # 8 -   1 分钟K 线
    KLINE_1MIN = 8
    # 9 -   日K 线
    KLINE_RI_K = 9
    # 10 -  季K 线
    KLINE_3MONTH = 10
    # 11 -  年K 线
    KLINE_YEARLY = 11

    # 分笔行情最多2000条
    MAX_TRANSACTION_COUNT = 2000
    # K线数据最多800条
    MAX_KLINE_COUNT = 800

    HQ_HOSTS = {
        ('58.210.106.111', 7709),
        ('106.120.74.86', 7709),
        ('112.95.140.74', 7709),
        ('112.95.140.92', 7709),
        ('112.95.140.93', 7709),
        ('113.105.142.162', 7721),
        ('114.67.61.70', 7709),
        ('114.80.149.19', 7709),
        ('114.80.149.22', 7709),
        ('115.238.56.198', 7709),
        ('115.238.90.165', 7709),
        ('117.184.140.156', 7709),
        ('119.147.164.60', 7709),
        ('119.147.171.206', 80),
        ('120.79.60.82', 7709),
        ('123.125.108.23', 7709),
        ('123.125.108.24', 7709),
        ('124.160.88.183', 7709),
        ('180.153.18.170', 7709),
        ('180.153.18.171', 7709),
        ('180.153.39.51', 7709),
        ('218.108.47.69', 7709),
        ('218.108.98.244', 7709),
        ('218.75.126.9', 7709),
        ('221.194.181.176', 7709),
        ('39.100.68.59', 7709),
        ('39.98.198.249', 7709),
        ('39.98.234.173', 7709),
        ('47.103.48.45', 7709),
        ('60.12.136.250', 7709),
        ('60.191.117.167', 7709),
        ('61.152.249.56', 7709),
        ('61.153.209.138', 7709),
        ('jstdx.gtjas.com', 7709),
        ('shtdx.gtjas.com', 7709),
        ('sztdx.gtjas.com', 7709),
        }

    EX_HOSTS = {
        ('106.14.95.149', 7727),
        ('112.74.214.43', 7727),
        ('119.97.185.5', 7727),
       #('124.74.236.94', 7721),
        ('139.219.103.190', 7721),
        ('47.107.75.159', 7727),
        ('47.92.127.181', 7727),
        ('59.175.238.38', 7727),
        }

    def __init__ (self):
        self.__tasks = ThreadTasks()
        self.__hq = self.__tasks.queue()
        self.__ex = self.__tasks.queue()

    def __hq_ping(self, ip, port):
        api = TdxHq_API()
        with api.connect(ip, port, time_out=0.7):
            assert len(api.get_security_list(0, 1)) > 800
            api.disconnect()
            return True
        return False

    def __exhq_ping(self, ip, port):
        api = TdxExHq_API()
        with api.connect(ip, port, time_out=0.7):
            assert api.get_instrument_count() > 20000
            api.disconnect()
            return True
        return False
    
    def __ping(self, func, ip, port):
        try:
            start = datetime.datetime.now()
            return ip, port, func(ip, port), datetime.datetime.now() - start
        except Exception as e:
            return ip, port, False, datetime.timedelta(9, 9, 0)
    
    def __paralle_ping(self, hosts, func, queue):
        if queue.qsize() > 0:
            return self
        _max = datetime.timedelta(0, 9, 0)

        for ip, port in hosts:
            self.__tasks.add(self.__ping, func=func, ip=ip, port=port)
        server = list()
        for ip, port, status, time in self.__tasks.executor():
            if status is False or time > _max:
                continue
            server.append((ip, port, time))
        for x in set([(x[0], x[1]) for x in sorted(server, key=lambda x: x[2])]):
            queue.put(x)

        assert queue.qsize() > 0
        return self
    
    def __paralle_hq_ping(self):
        return self.__paralle_ping(hosts=self.HQ_HOSTS, func=self.__hq_ping, queue=self.__hq)
    
    def __paralle_exhq_ping(self):
        return self.__paralle_ping(hosts=self.EX_HOSTS, func=self.__exhq_ping, queue=self.__ex)
        
    def __code(self, code):
        return list(set([str(code)] if isinstance(code, str) or isinstance(code, int) else [] if code is None else list(map(lambda x:str(x), code))))

    def __day_offset(self, start, end, nums):
        _offset = len(days_trade_range(start=None))//nums
        _start = len(days_trade_range(start=start))//nums
        _end = len(days_trade_range(start=end))//nums
        #print(_offset,  _start, _end, _offset-_start, _offset-_end+1)
        return [(i*nums, nums) for i in range(_offset, -1, -1)][_offset-_start:_offset-_end+1]

    @retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
    def __hq_bars(self, code, offset, frequency=9, index=False):
        assert self.__hq.qsize() > 0
        api = TdxHq_API()
        if index is True:
            market_func = get_index_market
            bars_func = api.get_index_bars
        else:
            market_func = get_code_market
            bars_func = api.get_security_bars
        ip, port = self.__hq.get()
        with api.connect(ip, port):
            df = list()
            for _code in code:
                market = market_func(_code)
                for _start, _count in offset:
                    df.append(api.to_df(bars_func(frequency, market, _code, _start, _count)).assign(code=_code))
            api.disconnect()
            self.__hq.put((ip, port))
            if len(df) < 1:
                return None
            return pandas.concat(df, sort=False)

    def __paralle_hq_bars(self, code, start=None, end=datetime.datetime.today(), count=0, frequency=9, index=False):
        days = days_trade_range(start=start, end=end, count=count)
        start, end=days[0], days[-1]
        code = self.__code(code)
        csize = len(code)
        offset=self.__day_offset(start=start, end=end, nums=800)
        #print(offset)
        
        self.__paralle_hq_ping()
        if csize == 1:#单线程
            df = self.__hq_bars(code=code, offset=offset, frequency=frequency, index=index)
        else:
            qsize = self.__hq.qsize()
            if qsize >= csize:#每线程一个
                for _code in code:
                    self.__tasks.add(self.__hq_bars, code=[_code], offset=offset, frequency=frequency, index=index)
            else:#每线程多个
                ma = csize//qsize
                fix = csize%qsize
                for i in range(qsize):
                    _code = ma+1 if i < fix else ma
                    self.__tasks.add(self.__hq_bars, code=code[0:_code], offset=offset, frequency=frequency, index=index)
                    code = code[_code:]
            df = pandas.concat(self.__tasks.executor(), sort=False)
        df = df.assign(volume=df.vol, datetime=pandas.to_datetime(df['datetime']).dt.date)
        df = df[['datetime','code','open','high','low','close','volume','amount']].set_index(['datetime', 'code']).sort_index()
        df = df[df.index.get_level_values(0).isin(days)]
        return df

    def stock_day(self, code, start=None, end=datetime.datetime.today(), count=0, fq=None):
        return self.__paralle_hq_bars(code, start=start, end=end, count=count, frequency=self.KLINE_RI_K, index=False)

    def etf_day(self, code, start=None, end=datetime.datetime.today(), count=0):
        return self.stock_day(code, start=start, end=end, count=count)

    def index_day(self, code, start=None, end=datetime.datetime.today(), count=0):
        return self.__paralle_hq_bars(code, start=start, end=end, count=count, frequency=self.KLINE_RI_K, index=True)

    def bond_day(self, code, start=None, end=datetime.datetime.today(), count=0):
        return self.stock_day(code, start=start, end=end, count=count)

    @retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
    def __exhq_bars(self, code, offset, frequency=9):
        assert self.__ex.qsize() > 0
        api = TdxExHq_API()
        ip, port = self.__ex.get()
        with api.connect(ip, port):
            df = list()
            for _code in code:
                for _start, _count in offset:
                    df.append(api.to_df(api.get_instrument_bars(frequency, self.__exhq_list().xs(_code).market, _code, _start, _count)).assign(code=_code))
            api.disconnect()
            self.__ex.put((ip, port))
            if len(df) < 1:
                return None
            return pandas.concat(df, sort=False)

    def __paralle_exhq_bars(self, code, start=None, end=datetime.datetime.today(), count=0, frequency=9):
        days = days_trade_range(start=start, end=end, count=count)
        start, end=days[0], days[-1]
        code = self.__code(code)
        csize = len(code)
        offset=self.__day_offset(start=start, end=end, nums=700)
        
        self.__exhq_list()
        if csize == 1:#单线程
            df = self.__exhq_bars(code=code, offset=offset, frequency=frequency)
        else:
            qsize = self.__ex.qsize()
            if qsize >= csize:#每线程一个
                for _code in code:
                    self.__tasks.add(self.__exhq_bars, code=[_code], offset=offset, frequency=frequency)
            else:#每线程多个
                ma = csize//qsize
                fix = csize%qsize
                for i in range(qsize):
                    _code = ma+1 if i < fix else ma
                    self.__tasks.add(self.__exhq_bars,  code=code[0:_code], offset=offset, frequency=frequency)
                    code = code[_code:]
            df = pandas.concat(self.__tasks.executor(), sort=False)
        df = df.assign(datetime=pandas.to_datetime(df['datetime']).dt.date)
        df = df[['datetime','code','open','high','low','close','position','trade','price','amount']].set_index(['datetime', 'code']).sort_index()
        df = df[df.index.get_level_values(0).isin(days)]
        return df

    def fund_day(self, code, start=None, end=datetime.datetime.today(), count=0):
        return self.__paralle_exhq_bars(code=code, start=start, end=end, count=count, frequency=self.KLINE_RI_K)

    def option_day(self, code, start=None, end=datetime.datetime.today(), count=0):
        return self.fund_day(code, start=start, end=end, count=count)

    @retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
    def __hq_list(self, market):
        assert self.__hq.qsize() > 0
        api = TdxHq_API()
        ip, port = self.__hq.get()
        with api.connect(ip, port):
            df = list()
            for start in range(0, api.get_security_count(market=market), 1000):
                df.append(api.to_df(api.get_security_list(market, start)))
            api.disconnect()
            self.__hq.put((ip, port))
            df = pandas.concat(df, sort=False).assign(sse=market)
            df = df[['code', 'volunit', 'decimal_point', 'name', 'pre_close', 'sse']].dropna()
            df = df.assign(sse='sh' if market == CODE_MARKET_SH else 'sz', sec=get_code_type(df.code.tolist(), market))
            return df
        return None

    @lru_cache()
    def __paralle_hq_list(self):
        self.__paralle_hq_ping()
        self.__tasks.add(self.__hq_list, market=CODE_MARKET_SH)
        self.__tasks.add(self.__hq_list, market=CODE_MARKET_SZ)
        return pandas.concat(self.__tasks.executor(), sort=False).drop(columns=['pre_close']).set_index(['code', 'sec'])

    def stock_list(self,  code=None):
        df = self.__paralle_hq_list().xs('stock_cn', level=1)
        return df if code is None else df[df.index.isin(self.__code(code))]

    def etf_list(self, code=None):
        df = self.__paralle_hq_list().xs('etf_cn', level=1)
        return df if code is None else df[df.index.isin(self.__code(code))]

    def index_list(self,  code=None):
        df = self.__paralle_hq_list().xs('index_cn', level=1)
        return df if code is None else df[df.index.isin(self.__code(code))]

    def bond_list(self,  code=None):
        df = self.__paralle_hq_list().xs('bond_cn', level=1)
        return df if code is None else df[df.index.isin(self.__code(code))]

    @lru_cache()
    @retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
    def __exhq_list(self):
        self.__paralle_exhq_ping()
        api = TdxExHq_API()
        ip, port = self.__ex.get()
        with api.connect(ip, port):
            df = list()
            nums = api.get_instrument_count()
            for i in range((nums//500+(1 if nums%500 > 0 else 0))+1):
                df.append(api.to_df(api.get_instrument_info(i*500, 500)))
            self.__ex.put((ip, port))
            df = pandas.concat(df, sort=False).set_index('code')
            return df
        return None

    def fund_list(self,  code=None):
        df = self.__exhq_list()
        df = df[(df.category==8) & (df.market==33)]
        return df if code is None else df[df.index.isin(self.__code(code))]

    def option_list(self,  code):
        df = self.__exhq_list()
        df = df[(df.name.str[:6]==code) & (df.category.isin([1, 12])) & (df.market.isin([1,8,9]))]
        return df

    def __hq_tick(self, code):
        api = TdxHq_API()
        ip, port = self.__hq.get()
        with api.connect(ip, port):
            df = api.get_security_quotes(code)
            if df is not None:
                df = api.to_df(df)
            api.disconnect()
            self.__hq.put((ip, port))
            return df

    def __paralle_hq_tick(self, code, count=80):
        self.__paralle_hq_ping()
        if len(code) > count:#每线程80个
            code = list(code)
            for i in range(len(code)//count+int(len(code)%count>0)):
                self.__tasks.add(self.__hq_tick, code=code[i*count:(i+1)*count])
            df = pandas.concat(self.__tasks.executor(), sort=False)
        else:
            df = self.__hq_tick(code=code)
        df = df.assign(datetime=datetime.datetime.now())
        df = df.assign(sec=df.apply(lambda x:get_code_type(x.code, x.market), axis=1))
        if len(df.sec.isin(['etf_cn', 'bond_cn']).where(lambda x:x>0).dropna()) > 0:#ETF和可转债的价格要/10
            df.update(df[df.sec.isin(['etf_cn', 'bond_cn'])][['last_close','open','high','low','price','ask1','ask2','ask3','ask4','ask5','bid1','bid2','bid3','bid4','bid5']].apply(lambda x:x/10))
        df = df.set_index(['code', 'market']).sort_index()
        return df

    @retry(stop_max_attempt_number=3, wait_random_min=50, wait_random_max=100)
    def tick(self,  stock=None,  index=None, etf=None, bond=None):
        code = set()
        code = code | set(map(lambda x:(get_code_market(x), x), self.__code(stock)))
        code = code | set(map(lambda x:(get_code_market(x), x), self.__code(etf)))
        code = code | set(map(lambda x:(get_index_market(x), x), self.__code(index)))
        code = code | set(map(lambda x:(get_code_market(x), x), self.__code(bond)))
        return self.__paralle_hq_tick(code=code)

Fetcher = Tdx()

if __name__ == '__main__':
    print(Fetcher.tick(stock=['000001', 600000], index='000001', etf=['510050', '510300', '510500'], bond=113571))