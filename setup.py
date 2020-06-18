try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages

setup(
    name='OFSData_TradingLib',
    #packages=find_packages()
    #entry_points={'console_scripts': ['traderlib = TraderLib.__main__:execute',]},
)