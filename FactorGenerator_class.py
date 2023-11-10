import os
import pandas as pd

class FactorGenerator:
    def __init__(self, ts_code):
        self.ts_code = ts_code 
        
    def data_download(self):
        self.daily = pd.read_csv(f'data/monthly_daily/{self.ts_code}.csv')
        self.limit = pd.read_csv(f'data/monthly_limit/{self.ts_code}.csv')
        self.adj_factor = pd.read_csv(f'data/monthly_adj_factor/{self.ts_code}.csv')
        self.daily_basic = pd.read_csv(f'data/monthly_daily_basic/{self.ts_code}.csv')
        self.moneyflow = pd.read_csv(f'data/monthly_moneyflow/{self.ts_code}.csv')
        self.financial = pd.read_csv(f'data/monthly_financial/{self.ts_code}.csv')
        self.income = pd.read_csv(f'data/monthly_income/{self.ts_code}.csv')
        self.balance = pd.read_csv(f'data/monthly_balance/{self.ts_code}.csv')
        self.cashflow = pd.read_csv(f'data/monthly_cashflow/{self.ts_code}.csv')
        self.data = pd.DataFrame()
        # 合并数据
        self.data = pd.concat([
                self.daily.set_index('trade_date'),
                self.limit.set_index('trade_date'),
                self.adj_factor.set_index('trade_date'),
                self.daily_basic.set_index('trade_date'),
                self.moneyflow.set_index('trade_date'),
                self.financial.set_index('end_date'),
                self.income.set_index('end_date'),
                self.balance.set_index('end_date'),
                self.cashflow.set_index('end_date')
            ], axis=1, join='inner').reset_index()
        self.data.rename(columns={'index': 'trade_date'}, inplace=True)
        self.data = self.data.loc[:,~self.data.columns.duplicated()]
        self.data['trade_date'] = pd.to_datetime(self.data['trade_date'], format='%Y-%m-%d')
        self.data = self.data.sort_values(by='trade_date') # 按交易日期排序数据
        # print(self.data)   
        pass
    
    def generate_predict_value(self):
        # 计算后一月的收盘价相对于当前收盘价的涨跌幅
        predict_value = 100 * (self.data['close'].shift(-1) - self.data['close']) / self.data['close']
        
        # 计算当前收盘价相对于前一月收盘价的涨跌幅
        shift_predict_value = 100 * (self.data['close'] - self.data['close'].shift(1)) / self.data['close'].shift(1)
        
        # 创建一个包含预测值、涨跌幅、收盘价等信息的DataFrame
        value = pd.DataFrame({
            'trade_date': self.data['trade_date'],  # 交易日期
            'predict_value': predict_value,  # 后一月的收盘价相对于当前收盘价的涨跌幅
            'shift_predict_value': shift_predict_value,  # 当前收盘价相对于前一月收盘价的涨跌幅
            'close' : self.data['close'],  # 收盘价
            'ts_code' : [self.ts_code for i in range(len(self.data['trade_date']))]  # 股票代码
        })
        
        return value

    
    def generate_value_factor(self):
        # 生成价值因子的逻辑
        # 价值因子：根据默认指标计算的价值因子，包括：
        # EP（每股盈利倒数）
        # EPcut（扣除非经常性损益后的每股盈利倒数）
        # BP（每股净资产倒数）
        # SP（每股销售收入倒数）
        # DP（股息率）
        # G/PE（盈利增长率与市盈率的乘积）
        
        # 计算因子
        ep = 1 / self.data['pe']
        ep_cut = 1 / self.data['pe_ttm']
        bp = 1 / self.data['pb']
        sp = 1 / self.data['ps_ttm']
        dp = self.data['dv_ratio']
        g_pe = self.data['netprofit_yoy'] * self.data['pe']

        # 合并因子数据
        value_factor = pd.DataFrame({
            'trade_date': self.data['trade_date'],
            'ep': ep,
            'ep_cut': ep_cut,
            'bp': bp,
            'sp': sp,
            'dp': dp,
            'g_pe': g_pe
        })
        return value_factor
    
    def generate_growth_factor(self):
        # netprofit_yoy 净利润增长率：公司利润增长水平
        # eps_yoy 每股收益增长率：每股收益的年度增长百分比
        # ROE（净资产收益率）：公司获得每一元股东权益的净利润能力
        # ROA（总资产回报率）：公司利用全部资产获取的净利润能力
        
        netprofit_yoy = self.data['netprofit_yoy']
        eps_yoy = self.data['basic_eps_yoy']
        roe = self.data['roe']
        roa = self.data['roa']
        if self.data['roa'].isnull().values.any():
            roa = self.data['n_income']/self.data['total_assets']
        if self.data['roa'].isnull().values.any():
            roe = self.data['n_income']/(self.data['total_assets'] - self.data['total_liab'])

        # 合并因子数据
        growth_factor = pd.DataFrame({
            'trade_date': self.data['trade_date'],
            'netprofit_yoy': netprofit_yoy,
            'eps_yoy': eps_yoy,
            'roe': roe,
            'roa': roa
        })
        
        return growth_factor
        
    def generate_financial_factor(self):
        # 总资产（total_assets）：公司所有的资产总额。
        # 总负债（total_liabilities）：公司所有的负债总额。
        # 净资产（equity）：公司净资产的价值，等于总资产减去总负债。
        # 流动比率（current_ratio）：公司流动资产与流动负债之间的关系，用于评估公司偿还短期债务的能力。
        # 毛利率（grossprofit_margin）：公司销售商品或提供服务所创造的毛利润占销售收入的比例，用于评估公司经营效益。
        # 净利润率（netprofit_margin）：公司净利润占销售收入的比例，用于评估公司盈利水平。
        
        # 计算因子
        total_assets = self.data['total_assets']
        total_liabilities = self.data['total_liab']
        equity = self.data['total_assets'] - self.data['total_liab']
        current_ratio = self.data['current_ratio']
        grossprofit_margin = self.data['grossprofit_margin']
        netprofit_margin = self.data['netprofit_margin']
        
        if self.data['current_ratio'].isnull().values.any():
            current_ratio = self.data['total_cur_liab']/self.data['total_cur_assets']
            
        if self.data['grossprofit_margin'].isnull().values.any():
            grossprofit_margin = self.data['operate_profit']/self.data['revenue']
        
        if self.data['netprofit_margin'].isnull().values.any():
            netprofit_margin = self.data['n_income']/self.data['revenue']

        # 合并因子数据
        financial_factor = pd.DataFrame({
            'trade_date': self.data['trade_date'],
            'total_assets': total_assets,
            'total_liabilities': total_liabilities,
            'equity': equity,
            'current_ratio': current_ratio,
            'grossprofit_margin': grossprofit_margin,
            'netprofit_margin': netprofit_margin
        })

        return financial_factor

    
    def generate_leverage_factor(self):
        # 负债比率（debt_ratio）：公司负债占总资产的比例，用于评估公司的财务风险程度。
        # 负债权益比（debt_to_equity_ratio）：公司负债与净资产之间的比例，用于评估公司财务杠杆程度。
        
        # 计算因子
        debt_ratio = self.data['debt_to_assets']
        debt_to_equity_ratio = self.data['debt_to_eqt']
        
        if self.data['debt_to_assets'].isnull().values.any():
            debt_ratio = self.data['total_liab']/self.data['total_assets']
            
        if self.data['debt_to_eqt'].isnull().values.any():
            debt_to_equity_ratio = self.data['total_liab']/(self.data['total_assets']-self.data['total_liab'])

        # 合并因子数据
        leverage_factor = pd.DataFrame({
            'trade_date': self.data['trade_date'],
            'debt_ratio': debt_ratio,
            'debt_to_equity_ratio': debt_to_equity_ratio
        })

        return leverage_factor 
    
    
    
    def generate_technical_factor(self):
        # 波动率：股票价格的波动程度
        # 相对强弱指数（RSI）：用于度量股票内在强度的指标
        # MACD（移动平均线收敛/发散）：用于识别股票价格趋势的指标
    
        ma3 = self.data['close'].rolling(window=3).mean()  # 计算3个月的移动平均
        ma6 = self.data['close'].rolling(window=6).mean()  # 计算6个月的移动平均
        ma9 = self.data['close'].rolling(window=9).mean()  # 计算9个月的移动平均
        ma12 = self.data['close'].rolling(window=12).mean()  # 计算12个月的移动平均
        momentum = self.data['close'] - self.data['close'].shift(1)  # 当前收盘价减去前一天的收盘价
        volatility = self.data['close'].rolling(window=24).std()  # 24个月的标准差

        # 计算每日价格变动
        price_diff = self.data['close'].diff(1)
        
        # 分别计算收益（正差额）和损失（负差额）
        gain = price_diff.where(price_diff > 0, 0)
        loss = -price_diff.where(price_diff < 0, 0)
        
        # 计算14天内的平均收益和平均损失
        avg_gain = gain.rolling(window=14).mean().fillna(0)
        avg_loss = loss.rolling(window=14).mean().fillna(0)
        # 计算相对强度（RS）和相对强弱指数（RSI）
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        # MA交叉指标的布尔型指标
        ma3_cross_ma12 = (ma3 > ma12).astype(int)  # 检查3个月均线是否高于12个月均线
        ma3_cross_ma9 = (ma3 > ma9).astype(int)  # 检查3个月均线是否高于9个月均线

        # 构建技术因子DataFrame
        technical_factor = pd.DataFrame({
            'trade_date': self.data['trade_date'],
            'ma3': ma3,
            'ma6': ma6,
            'ma9': ma9,
            'ma12': ma12,
            'momentum': momentum,
            'volatility': volatility,
            'rsi': rsi,
            'ma3_cross_ma9': ma3_cross_ma9,
            'ma3_cross_ma12': ma3_cross_ma12
        })
        return technical_factor
    
    def generate_all_factor(self):
        
        # 创建'factor'文件夹
        if not os.path.exists(f'factor'):
            os.makedirs(f'factor')
        
        self.data_download()
        financial_factor = self.generate_financial_factor()
        growth_factor = self.generate_growth_factor()
        leverage_factor = self.generate_leverage_factor()
        technical_factor = self.generate_technical_factor()
        value_factor = self.generate_value_factor()
        predict_value = self.generate_predict_value()
        self.all_factor = pd.DataFrame()
        # 合并数据
        self.all_factor = pd.concat([
                value_factor.set_index('trade_date'),
                growth_factor.set_index('trade_date'),
                financial_factor.set_index('trade_date'),
                leverage_factor.set_index('trade_date'),
                technical_factor.set_index('trade_date'),
                predict_value.set_index('trade_date'),
            ], axis=1, join='inner').reset_index()
        self.all_factor.rename(columns={'index': 'trade_date'}, inplace=True)
        self.all_factor = self.all_factor.loc[:,~self.all_factor.columns.duplicated()]
        
        # 保存因子数据
        file_name = f'factor/{self.ts_code}.csv'
        self.all_factor.to_csv(file_name, index=False)
        print(f'Saved factor data for {self.ts_code} to {file_name}')
        