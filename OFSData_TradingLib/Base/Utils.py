# 市场
CODE_MARKET_SH = 1  # 上海
CODE_MARKET_SZ = 0  # 深圳

def get_index_market(code):
    if isinstance(code, list):
        return list(map(get_index_market, code))
    assert isinstance(code, str)

    code = code.lower()
    if code.startswith(('000', '880')):
        return CODE_MARKET_SH
    elif code.startswith(('395', '399', '980')):
        return CODE_MARKET_SZ
    else:
        raise AssertionError()

def get_code_market(code):
    if isinstance(code, list):
        return list(map(get_code_market, code))
    assert isinstance(code, str)

    code = code.lower()
    #if code.startswith(('102', '110', '113', '120', '122', '124', '130', '132', '133', '134', '136', '140', '141', '143', '144', '147', '148', '510', '511', '512', '513', '515', '518', '600', '601', '603', '605', '688')):
    if code.startswith(('110', '113', '510', '511', '512', '513', '515', '518', '588', '600', '601', '603', '605')):
        return CODE_MARKET_SH
    #elif code.startswith(('000', '001', '002', '003', '101', '104', '105', '106', '107', '108', '109', '111', '112', '114', '115', '116', '117', '118', '119', '123', '127', '128', '131', '139', '159', '300')):
    elif code.startswith(('000', '001', '002', '003', '123', '127', '128', '159', '300')):
        return CODE_MARKET_SZ
    else:
        raise AssertionError(code)

def get_code_type(code, market):
    if isinstance(code, list):
        return list(map(lambda x:get_code_type(code=x, market=market), code))
    assert isinstance(code, str)

    code = code.lower()
    if market == CODE_MARKET_SH:
        if code.startswith(('600', '601', '603', '605')):
            return 'stock_cn'
        elif code.startswith(('900')):
            return 'stockB_cn'
        elif code.startswith(('000', '880')):
            return 'index_cn'
        elif code.startswith(('510', '511', '512', '513', '515', '518', '588')):
            return 'etf_cn'
        #elif code.startswith(('102', '110', '113', '120', '122', '124', '130', '132', '133', '134', '135', '136', '140', '141', '143', '144', '147', '148')):
        elif code.startswith(('110', '113', '126')):#只保留可转债
            return 'bond_cn'
    elif market == CODE_MARKET_SZ:
        if code.startswith(('000', '001', '002', '003', '300')):
            return 'stock_cn'
        elif code.startswith(('200', '201')):
            return 'stockB_cn'
        elif code.startswith(('395', '399', '980')):
            return 'index_cn'
        elif code.startswith(('159')):
            return 'etf_cn'
        #elif code.startswith(('101', '104', '105', '106', '107', '108', '109', '111', '112', '114', '115', '116', '117', '118', '119', '123', '127', '128', '131', '139')):
        elif code.startswith(('123', '125', '126', '127', '128', '129')):#只保留可转债
            return 'bond_cn'
    return 'undefined'