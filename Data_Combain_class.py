import os
import pandas as pd
import datetime
from sklearn.preprocessing import StandardScaler, MinMaxScaler

class Data_Combain:
    def __init__(self, start_date, end_date):
        self.date_format = "%Y%m%d" 
        if isinstance(input, str):
            self.start_date = datetime.datetime.strptime(start_date, self.date_format)
            self.end_date = datetime.datetime.strptime(end_date, self.date_format)
        self.start_date = start_date
        self.end_date = end_date
        self.stock_codes = 'stock_codes' # 存储股票代码的文件名
        
    def data_download(self):
        self.df_stock_codes = pd.read_csv(f'data/{self.stock_codes}.csv') # 从文件中读取股票代码的数据框
        pass
    
    def combain(self):
        self.data_download() # 调用数据下载方法
        self.all_data = pd.DataFrame() # 创建一个空的数据框用于存储所有数据
        for stock_code in self.df_stock_codes['stock_codes']: # 遍历每个股票代码
            my_Data_Resample = Data_Resample(stock_code) # 创建一个 Data_Resample 类的实例
            temp_data = my_Data_Resample.resample() # 调用 Data_Resample 类的 resample 方法获取采样后的数据
            temp_data['trade_date'] = pd.to_datetime(temp_data['trade_date'], format='%Y-%m-%d') # 将交易日期转换为日期对象
            temp_data = temp_data[(temp_data['trade_date'] >= self.start_date) & (temp_data['trade_date'] <= self.end_date)] # 根据给定日期范围筛选数据
            self.all_data = pd.concat([self.all_data, temp_data], ignore_index=True) # 将临时数据框添加到所有数据中
        self.all_data = self.all_data.sort_values(by='trade_date') # 按交易日期排序数据
        self.all_data = self.all_data.reset_index(drop=True) # 重置索引
        numeric_data = self.all_data.select_dtypes(include=['float64', 'int64']) # 选择数值型列
        mean_value = numeric_data.mean() # 计算每列的均值
        self.all_data = self.all_data.fillna(mean_value) # 用均值填充缺失值
        my_Data_Normalization = Data_Normalization(self.all_data) # 创建一个 Data_Normalization 类的实例
        self.all_data = my_Data_Normalization.normalization() # 调用 Data_Normalization 类的 normalization 方法进行数据归一化
        return self.all_data
    
    def first_combain(self):
        self.data_download() # 调用数据下载方法
        for stock_code in self.df_stock_codes['stock_codes']: # 遍历每个股票代码
            my_Data_Resample = Data_Resample(stock_code) # 创建一个 Data_Resample 类的实例
            my_Data_Resample.save_resample_datetime() # 调用 Data_Resample 类的 save_resample_datetime 方法修正时间
        pass

class Data_Normalization:
    def __init__(self, data):
        self.data = data # 存储数据
        
        # 因子名称列表
        self.factor_name = ['close', 'shift_predict_value', 'ep', 'ep_cut', 'bp', 'sp', 'dp', 'g_pe', 
                            'netprofit_yoy', 'eps_yoy', 'roe', 'roa',
                            'total_assets', 'total_liabilities', 'equity', 'current_ratio', 'grossprofit_margin', 'netprofit_margin',
                            'debt_ratio', 'debt_to_equity_ratio',
                            'ma3', 'ma6', 'ma9', 'ma12', 'momentum', 'volatility', 'rsi']
        
        # 取值为 0 或 1 的因子名称列表
        self.one_zero_factor_name = ['ma3_cross_ma9', 'ma3_cross_ma12'] 
        
    def normalization(self):
        ss = StandardScaler() # 创建一个标准化对象，用于数据标准化
        self.factor_name.extend(['predict_value'])
        ss.fit(self.data[self.factor_name]) # 对指定的因子进行拟合，计算均值和标准差
        self.new_data = pd.DataFrame() # 创建一个空的数据框用于存储归一化后的数据
        self.new_data['trade_date'] = self.data['trade_date'] # 将交易日期列复制到新的数据框
        self.new_data['ts_code'] = self.data['ts_code'] # 将股票代码列复制到新的数据框
        # self.new_data['predict_value'] = self.data['predict_value'] # 将预测值列复制到新的数据框
        self.new_data[self.factor_name] = ss.transform(self.data[self.factor_name]) # 对指定的因子进行数据标准化
        self.new_data[self.one_zero_factor_name] = self.data[self.one_zero_factor_name] # 复制 0 或 1 的因子到新的数据框
        return self.new_data

class Data_Resample:
    def __init__(self, ts_code):
        self.ts_code = ts_code # 存储股票代码
        
    def data_download(self):
        self.data = pd.read_csv(f'factor/{self.ts_code}.csv') # 从文件中读取股票因子数据
        pass
    
    def resample(self):
        self.data_download() # 调用数据下载方法
        self.data = self.data[24:-1] # 对数据进行采样
        self.data = self.data.dropna(subset=['close']) # 删除没有收盘价的行
        self.data = self.data.reset_index(drop=True) # 重置索引
        return self.data

    def save_resample_datetime(self):
        # 在第一次使用时，需要对交易时间进行重置
        self.data_download() # 调用数据下载方法
        self.resample_datetime() # 在第一次使用时，调用resample_datetime需要对交易时间进行重置
        self.data.to_csv(f'factor/{self.ts_code}.csv') # 重存文件
        pass
    
    def resample_datetime(self):
        # 从文件中读取股票因子数据
        daily_price = pd.read_csv(f'data/daily_data/{self.ts_code}.csv')
        # 提取交易日期并转换为datetime格式，并去重
        daily_price_datetime = pd.to_datetime(daily_price['trade_date'].unique(), format='%Y%m%d')
        # 将策略中的交易日期列转换为datetime格式
        trade_date_change = pd.to_datetime(self.data['trade_date'], format='%Y-%m-%d')
        # 创建两个空列表，用于存储交易日期的变化情况
        trade_date_change_list_1 = []
        trade_date_change_list_2 = []
        # 遍历交易日期
        for i in range(len(trade_date_change)):
            j = 0
            # 在交易日期变化列表1中查找是否存在和当前交易日期相等的日期
            while(j < len(trade_date_change_list_1)):
                if trade_date_change[i] == trade_date_change_list_1[j]:
                    # 如果找到相等的日期，则将当前交易日期修改为对应的变化日期
                    trade_date_change.loc[i] = trade_date_change_list_2[j]
                    break
                j += 1
            # 如果在列表1中没有找到相等的日期，并且当前交易日期不在daily_price_datetime中
            if j == len(trade_date_change_list_1) and trade_date_change[i] not in daily_price_datetime:
                # 将当前交易日期加入列表1中
                trade_date_change_list_1.append(trade_date_change[i])
                # 不断向前遍历日期，直到找到一个在daily_price_datetime中的日期
                while(trade_date_change[i] not in daily_price_datetime):
                    trade_date_change[i] = trade_date_change[i] - datetime.timedelta(days=1)
                    max_time = datetime.datetime.strptime('20231231', '%Y%m%d')
                    min_time = datetime.datetime.strptime('20100101', '%Y%m%d')
                    # 如果日期超出了指定的范围，则退出循环
                    if trade_date_change[i] > max_time or trade_date_change[i] < min_time:
                        break
                # 将找到的日期添加到列表2中
                trade_date_change_list_2.append(trade_date_change[i])
        # 将修改后的交易日期更新到策略数据中
        self.data['trade_date'] = trade_date_change

    
