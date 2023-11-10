import os
import pandas as pd

class Converter:
    def __init__(self, stock_data, ts_code, file_type):
        self.stock_data = stock_data  
        self.ts_code = ts_code  
        self.file_type = file_type
        
        
    def convert_daily_to_monthly_data(self):
        # 创建'monthly_daily'文件夹
        if not os.path.exists(f'data/monthly_{self.file_type}'):
            os.makedirs(f'data/monthly_{self.file_type}')
        
        # 假设date_format为CSV文件中日期列的格式, 如 '%Y%m%d'
        date_format = '%Y%m%d'
        
        # 将日期列转换为datetime类型并设置为索引
        self.stock_data['trade_date'] = pd.to_datetime(self.stock_data['trade_date'], format=date_format)
        self.stock_data.set_index('trade_date', inplace=True)
        # 将日度数据转换为月频数据
        monthly_data = self.stock_data.resample('M').last().ffill()
        
        # 保存月频数据
        file_name = f'data/monthly_{self.file_type}/{self.ts_code}.csv'
        monthly_data.to_csv(file_name)
        print(f'Saved monthly data for {self.ts_code} to {file_name}')
        
        
    def convert_quarterly_to_monthly_data(self):
        # 创建'monthly_daily'文件夹
        if not os.path.exists(f'data/monthly_{self.file_type}'):
            os.makedirs(f'data/monthly_{self.file_type}')

        # 假设date_format为CSV文件中日期列的格式, 如 '%Y%m%d'
        date_format = '%Y%m%d'
        
        # 将日期列转换为datetime类型并设置为索引
        self.stock_data['end_date'] = pd.to_datetime(self.stock_data['end_date'], format=date_format)
        self.stock_data.set_index('end_date', inplace=True)
        
                
        # 将季度数据转换为月度数据
        monthly_data = self.stock_data.resample('M').last().ffill()
        
        # 季度数据再扩展两行
        monthly_data = self.extend_quarterly_to_monthly_data(monthly_data)
        monthly_data = self.extend_quarterly_to_monthly_data(monthly_data)
        
        # 修改索引名
        monthly_data = monthly_data.rename_axis("end_date").reset_index()
        
        # 保存月度数据
        file_name = f'data/monthly_{self.file_type}/{self.ts_code}.csv'
        monthly_data.to_csv(file_name, index=False)
        print(f'Saved monthly data for {self.ts_code} to {file_name}')

    def extend_quarterly_to_monthly_data(self, monthly_data):
        # 针对季度数据再扩展两行
        # 假设last_month_end_date是已有DataFrame的最后一个日期
        last_month_end_date = monthly_data.index[-1]

        last_monthly_data_row = monthly_data.iloc[[-1]]
        
        # 生成扩展的日期范围
        extended_dates = pd.date_range(start=last_month_end_date+pd.DateOffset(months=1),
                                    periods=1,
                                    freq='M')
        # print(extended_dates)
        # 创建新的DataFrame，使用原始DataFrame最后一个月的数据
        last_monthly_data_row.index = extended_dates

        # 将新的DataFrame与原始DataFrame连接起来
        monthly_data = pd.concat([monthly_data, last_monthly_data_row])

        return monthly_data