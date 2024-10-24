from yahoofinancials import YahooFinancials
from datetime import datetime
import pandas as pd
from tqdm import tqdm
import pickle
import os
import csv
import time

#stocks_folder_path = 'C:\\Users\\Tester\\Desktop\\stock_data_analysis\\stocks_data'
stocks_folder_path =  os.path.join(os. getcwd(), 'stocks_data')

# Check if the folder exists
if not os.path.exists(stocks_folder_path):
    # If not, create it
    os.makedirs(stocks_folder_path)


#tickers_df = pd.read_csv('full_tickers.txt', sep=' ', header=None, on_bad_lines='skip')
tickers_df = pd.read_csv('all_clean_tickers.csv', header=None, on_bad_lines='skip')
tickers = tickers_df[1].tolist()
tickers = tickers[15779:]
#tickers = tickers_list[2000:2015]

# Downloader function
def downloader(ticker):
   if ticker[0].isdigit():
      ticker = str(ticker)
   ticker_folder_path = os.path.join(stocks_folder_path, ticker)
   if not os.path.exists(ticker_folder_path):
      # If not, create it
      os.makedirs(ticker_folder_path)

   BS_getter(ticker, ticker_folder_path)
   CF_getter(ticker, ticker_folder_path)
   IS_getter(ticker, ticker_folder_path)
   Summary_getter(ticker, ticker_folder_path)

#Get Balance Sheet
def BS_getter(ticker, folder_path):
   yahoo_financials = YahooFinancials(ticker)
   get_bs_history = yahoo_financials.get_financial_stmts('annual', 'balance')
   get_bs_statements = get_bs_history['balanceSheetHistory'][ticker]

   main_df = pd.DataFrame.from_dict(get_bs_statements[0])
   for i in range(len(get_bs_statements) - 1):
      #print(main_df)
      second_df = pd.DataFrame.from_dict(get_bs_statements[i + 1])
      main_df = pd.concat([main_df,second_df], axis=1)
      ticker_csv = ticker + '_BS' + '.csv'
      csv_file_path = os.path.join(folder_path, ticker_csv)
   main_df.reset_index(inplace=True)
   # Save the DataFrame to a CSV file
   main_df.to_csv(csv_file_path, index=False)

#Get Income statement
def IS_getter(ticker, folder_path):
   yahoo_financials = YahooFinancials(ticker)
   get_income_history = yahoo_financials.get_financial_stmts('annual', 'income')
   get_income_statements = get_income_history['incomeStatementHistory'][ticker]

   main_df = pd.DataFrame.from_dict(get_income_statements[0])
   for i in range(len(get_income_statements) - 1):
      second_df = pd.DataFrame.from_dict(get_income_statements[i + 1])
      main_df = pd.concat([main_df,second_df], axis=1)
   main_df.reset_index(inplace=True)
   ticker_csv = ticker + '_IS' + '.csv'
   csv_file_path = os.path.join(folder_path, ticker_csv)

   # Save the DataFrame to a CSV file
   main_df.to_csv(csv_file_path, index=False)


#Get Cash Flow statement
def CF_getter(ticker, folder_path):
   yahoo_financials = YahooFinancials(ticker)
   get_cash_history = yahoo_financials.get_financial_stmts('annual', 'cash')
   get_cash_statements = get_cash_history['cashflowStatementHistory'][ticker]

   main_df = pd.DataFrame.from_dict(get_cash_statements[0])
   for i in range(len(get_cash_statements) - 1):
      second_df = pd.DataFrame.from_dict(get_cash_statements[i + 1])
      main_df = pd.concat([main_df,second_df], axis=1)
   main_df.reset_index(inplace=True)
   ticker_csv = ticker + '_CF' + '.csv'
   csv_file_path = os.path.join(folder_path, ticker_csv)

   # Save the DataFrame to a CSV file
   main_df.to_csv(csv_file_path, index=False)

def Summary_getter(ticker, folder_path):
   full_data_dict = {}
   yahoo_financials = YahooFinancials(ticker)
   cdt = datetime.now()
   clean_date = '%s/%s/%s' % (cdt.month, cdt.day, cdt.year)
   summary_data = yahoo_financials.get_summary_data(reformat=True)
   key_stats = yahoo_financials.get_key_statistics_data()
   fin_data = yahoo_financials.get_financial_data()
   profile_data = yahoo_financials.get_stock_profile_data()
   pe_ratio = yahoo_financials.get_pe_ratio()
   book_value = yahoo_financials.get_book_value()
   price_to_sales = yahoo_financials.get_price_to_sales()
   earnings_per_share = yahoo_financials.get_earnings_per_share()

   full_data_dict['Ticker name'] = ticker
   full_data_dict['Summary data'] = summary_data[ticker]
   full_data_dict['Key statistics'] = key_stats[ticker]
   full_data_dict['Financial data'] = fin_data[ticker]
   full_data_dict['Company profile data'] = profile_data[ticker]
   full_data_dict['P/E ration'] = pe_ratio
   full_data_dict['Book value'] = book_value
   full_data_dict['Price to sales'] = price_to_sales
   full_data_dict['Earnings per share'] = earnings_per_share
   full_data_dict['Date the data was gathered'] = clean_date


   ticker_pkl = ticker + '_summary' + '.pkl'
   pkl_file_path = os.path.join(folder_path, ticker_pkl)
   # save dictionary to person_data.pkl file
   with open(pkl_file_path, 'wb') as fp:
      pickle.dump(full_data_dict, fp)

if __name__ == "__main__":
   for ticker in tickers:
      print(ticker)
      try:
         downloader(ticker)
      except:
         pass
