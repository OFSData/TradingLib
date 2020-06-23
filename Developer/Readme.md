conda env create -n cython --file=Env.yml

echo [build] > %CONDA_PREFIX%\Lib\distutils\distutils.cfg
echo compiler = mingw32 >> %CONDA_PREFIX%\Lib\distutils\distutils.cfg