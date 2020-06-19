import datetime

class Base():
    def stock_day(self, code, start=None, end=datetime.datetime.today(), count=0, fq=None):
        raise NotImplemented()

    def etf_day(self, code, start=None, end=datetime.datetime.today(), count=0):
        raise NotImplemented()

    def index_day(self, code, start=None, end=datetime.datetime.today(), count=0):
        raise NotImplemented()

    def bond_day(self, code, start=None, end=datetime.datetime.today(), count=0):
        raise NotImplemented()

    def stock_list(self, code=None):
        raise NotImplemented()

    def etf_list(self, code=None):
        raise NotImplemented()

    def index_list(self, code=None):
        raise NotImplemented()

    def bond_list(self, code=None):
        raise NotImplemented()

    def tick(self,  stock=None, index=None, etf=None, bond=None):
        raise NotImplemented()

