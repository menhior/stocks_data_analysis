import os
import csv
import shutil
import pandas as pd
import pickle

#filters_to_user = input()
#low_liab_stocks = pd.read_csv('Low_L_to_A_ratios.csv')

class Analyzer_Class:
    def __init__(self):

        
        self.data_path = os.path.join(os.getcwd(), 'stocks_data')
        self.list_of_folder_paths = []

        for root, dirs, files in os.walk(self.data_path, topdown=False):
            for folder in dirs:
                folder_path = os.path.join(root, folder)
                self.list_of_folder_paths.append(folder_path)
        self.liab_threshold = 0.3
        self.Liab_to_Assets_ratio(self.list_of_folder_paths, self.liab_threshold)

        low_liability_stocks = pd.read_csv('Low_L_to_A_ratios.csv')
        
        self.failed_tickers_list = []
        self.failed_tickers_csv = os.path.join(os.getcwd(), 'failed_tickers.csv') 
        #self.clean_empty_folders(self.list_of_folder_paths, self.failed_tickers_list, self.failed_tickers_csv)
        
        self.pe_threshold = 20
        self.price_to_FCF_threshold = 25
        self.ROA_threshold = 10
        self.CROIC_threshold = 10
        #self.Liab_to_Assets_ratio(self.list_of_folder_paths, self.liab_threshold)
        self.folders_to_analyze = low_liability_stocks['Folder path'].to_list()
        #self.P_to_FCF_ratio(self.folders_to_analyze, self.price_to_FCF_threshold)
        #self.P_to_Earnings_ratio(self.folders_to_analyze, self.pe_threshold)
        #self.ROA_ratio(self.folders_to_analyze, self.ROA_threshold)
        self.CROIC_ratio(self.folders_to_analyze, self.CROIC_threshold)
        self.P_to_FCF_df = self.P_to_FCF_ratio(self.folders_to_analyze, self.price_to_FCF_threshold)
        print(len(self.P_to_FCF_df))
        self.ROA_df = self.ROA_ratio(self.folders_to_analyze, self.price_to_FCF_threshold)
        print(len(self.ROA_df))
        self.CROIC_df = self.CROIC_ratio(self.folders_to_analyze, self.CROIC_threshold)
        self.merged_df = pd.merge(self.P_to_FCF_df, self.ROA_df, on='Stock ticker', how='inner')
        self.merged_df = pd.merge(self.merged_df, self.CROIC_df, on='Stock ticker', how='inner')
        self.merged_df.to_csv('PFC+ROA+CROIC.csv', index=False)


    def clean_empty_folders(self, list_folder_paths, failed_tickers_list, output_path):
        for folder_path in list_folder_paths:
            # Check if the folder is empty
            files = os.listdir(folder_path)
            # Check if any file with a .exe extension exists
            pkl_files = [file for file in files if file.lower().endswith('.pkl')]
            if not pkl_files:
                failed_tickers_list.append(folder)
                try:

                    shutil.rmtree(folder_path)
                    print(f"Failed folder removed: {folder_path}")
                except OSError as e:
                    print(f"Error: {folder_path} - {e}")

        with open(output_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for item in failed_tickers_list:
                writer.writerow([item])

    def Liab_to_Assets_ratio(self, list_folder_paths, threshold):
        stock_names_list = []
        ratios_list = []
        folders_list = []
        for folder_path in list_folder_paths:
            # Check if the folder is empty
            stock_name = folder_path.split('\\')[-1]
            BS_file = folder_path + '\\' + stock_name + '_BS.csv'
            try:
                BS_df = pd.read_csv(BS_file)
                BS_df.set_index('index', inplace=True)
                BS_df.columns = pd.to_datetime(BS_df.columns)
                # Sort columns in chronological order
                BS_df = BS_df.sort_index(axis=1)

                # Convert the column names back to string format (if needed)
                #S_df.columns = BS_df.columns.strftime('%Y-%m-%d')
                last_year = BS_df.iloc[:, -1]
                stock_names_list.append(stock_name)
                try:
                    ratios_list.append(last_year['totalLiabilitiesNetMinorityInterest'] / last_year['totalAssets'])
                except:
                    ratios_list.append(None)
                folders_list.append(folder_path)
            except FileNotFoundError:
                print(f"BS for {stock_name} not found or path is incorrect.")
            except IndexError:
                print(f"No data for the year available for {stock_name}")

        L_to_A_ratio_dict = {'Stock ticker': stock_names_list, "L/A ratio": ratios_list, 'Folder path': folders_list}
        
        L_to_A_df = pd.DataFrame(L_to_A_ratio_dict)
        filtered_L_to_A_df = L_to_A_df[L_to_A_df['L/A ratio'] < threshold]

        filtered_L_to_A_df.to_csv('Low_L_to_A_ratios.csv', index=False)

    def P_to_Earnings_ratio(self, folder_paths_to_analyze, threshold):
        stock_names_list = []
        pe_ratios_list = []
        industries_list = []
        p_to_book_list = []
        folders_list = []
        for folder_path in folder_paths_to_analyze:
            # Check if the folder is empty
            stock_name = folder_path.split('\\')[-1]
            try:
                summary_file = folder_path + '\\' + stock_name + '_summary.pkl'
                with open(summary_file, 'rb') as file:
                    loaded_data = pickle.load(file)
                stock_names_list.append(stock_name)
                pe_ratios_list.append(loaded_data['P/E ration'])
                industries_list.append(loaded_data['Company profile data']['industry'])
                folders_list.append(folder_path)
                try:
                    p_to_book_list.append(loaded_data['Key statistics']['priceToBook'])
                except:
                    p_to_book_list.append('None')
            except FileNotFoundError:
                print(f"Summary data for {stock_name} not found or path is incorrect.")

        PE_ratio_dict = {'Stock ticker': stock_names_list, "P/E ratio": pe_ratios_list, 'Price to Book': p_to_book_list, 'Industry': industries_list, 'Folder path': folders_list}
        #print(PE_ratio_dict)
        PE_df = pd.DataFrame(PE_ratio_dict)
        PE_df = PE_df[(PE_df['Industry'] != 'Asset Management') & (PE_df['Industry'] != 'Credit Services')]
        filtered_PE_df = PE_df[PE_df['P/E ratio'] < threshold]
        sorted_PE_df = filtered_PE_df.sort_values(by=["P/E ratio", 'Price to Book'], ascending=True)
        sorted_PE_df .to_csv('Low_PE_ratios.csv', index=False)


    def P_to_FCF_ratio(self, folder_paths_to_analyze, threshold):
        stock_names_list = []
        p_to_fcf_ratios_list = []
        p_to_fcf_ratios_change_list = []
        industries_list = []
        p_to_book_list = []
        folders_list = []
        prices_at_time_scraped = []
        dates_when_scraped = []
        market_caps_list = []
        free_cash_flows_list = []
        for folder_path in folder_paths_to_analyze:
            # Check if the folder is empty
            stock_name = folder_path.split('\\')[-1]
            BS_file = folder_path + '\\' + stock_name + '_BS.csv'
            CF_file = folder_path + '\\' + stock_name + '_CF.csv'
            summary_file = folder_path + '\\' + stock_name + '_summary.pkl'
            try:
                BS_df = pd.read_csv(BS_file)
                CF_df = pd.read_csv(CF_file)
                with open(summary_file, 'rb') as file:
                    loaded_data = pickle.load(file)
                # Specify the string to check for in column names
                string_to_check = '2023'

                years_of_CF_reports = CF_df.columns

                # Remove columns containing the specified string
                columns_to_drop = [col for col in years_of_CF_reports if string_to_check in col]
                CF_df = CF_df.drop(columns=columns_to_drop, axis=1)
                CF_df.set_index('index', inplace=True)
                CF_df.columns = pd.to_datetime(CF_df.columns)
                # Sort columns in chronological order
                CF_df = CF_df.sort_index(axis=1)
                last_year_CF = CF_df.iloc[:,-1]

                years_of_BS_reports = BS_df.columns

                # Remove columns containing the specified string
                columns_to_drop = [col for col in years_of_BS_reports if string_to_check in col]
                BS_df = BS_df.drop(columns=columns_to_drop, axis=1)
                BS_df.set_index('index', inplace=True)
                BS_df.columns = pd.to_datetime(BS_df.columns)
                # Sort columns in chronological order
                BS_df = BS_df.sort_index(axis=1)
                last_year_BS = BS_df.iloc[:,-1]

                market_cap = loaded_data['Summary data']['marketCap']
                free_cash_flow_2022 = last_year_CF['cashFlowFromContinuingOperatingActivities']

                try:
                    capex_2022 = last_year_CF['capitalExpenditure']
                    free_cash_flow_2022 = free_cash_flow_2022 + capex_2022
                except KeyError:
                    pass

                p_to_fcf_ratio = market_cap/free_cash_flow_2022
                stock_names_list.append(stock_name)
                p_to_fcf_ratios_list.append(p_to_fcf_ratio)
                industries_list.append(loaded_data['Company profile data']['industry'])
                folders_list.append(folder_path)

                market_caps_list.append(market_cap)
                free_cash_flows_list.append(free_cash_flow_2022)

                try:
                    dates_when_scraped.append(loaded_data['Date the data was gathered'])
                except:
                    dates_when_scraped.append('None')

                try:
                    p_to_book_list.append(loaded_data['Key statistics']['priceToBook'])
                except:
                    p_to_book_list.append('None')

            except FileNotFoundError:
                print(f"CF or Summary for {stock_name} not found or path is incorrect.")
            except KeyError:
                print(f"Operating cash flow information not available for {stock_name}.")
            except IndexError:
                print(f"No data for the year available for {stock_name}")
            except UnboundLocalError:
                pass

        P_to_FCF_ratio_dict = {'Stock ticker': stock_names_list, "P/FCF ratio": p_to_fcf_ratios_list, 'Market Cap': market_caps_list, 'Free Cash Flow': free_cash_flows_list,'Price to Book': p_to_book_list, 'Industry': industries_list, 'Date Scraped': dates_when_scraped, 'Folder path': folders_list}
        P_to_FCF_df = pd.DataFrame(P_to_FCF_ratio_dict)
        P_to_FCF_df = P_to_FCF_df[(P_to_FCF_df['Industry'] != 'Asset Management') & (P_to_FCF_df['Industry'] != 'Credit Services') & (P_to_FCF_df['Industry'] != 'Insurance - Diversified')]
        filtered_P_to_FCF_df = P_to_FCF_df[(P_to_FCF_df['P/FCF ratio'] < threshold) & (P_to_FCF_df['P/FCF ratio'] > 0)]
        sorted_P_to_FCF_df = filtered_P_to_FCF_df.sort_values(by=["P/FCF ratio", 'Price to Book'], ascending=True)
        sorted_P_to_FCF_df.to_csv('Low_P_to_FCF_ratios.csv', index=False)
        return sorted_P_to_FCF_df

    def ROA_ratio(self, folder_paths_to_analyze, threshold):
        stock_names_list = []
        roa_ratios_list = []
        industries_list = []
        p_to_book_list = []
        folders_list = []
        for folder_path in folder_paths_to_analyze:
            # Check if the folder is empty
            stock_name = folder_path.split('\\')[-1]
            BS_file = folder_path + '\\' + stock_name + '_BS.csv'
            IS_file = folder_path + '\\' + stock_name + '_IS.csv'
            summary_file = folder_path + '\\' + stock_name + '_summary.pkl'
            try:
                BS_df = pd.read_csv(BS_file)
                IS_df = pd.read_csv(IS_file)
                with open(summary_file, 'rb') as file:
                    loaded_data = pickle.load(file)
                # Specify the string to check for in column names
                string_to_check = '2023'

                # Remove columns containing the specified string
                columns_to_drop_BS = [col for col in BS_df.columns if string_to_check in col]
                BS_df = BS_df.drop(columns=columns_to_drop_BS, axis=1)
                BS_df.set_index('index', inplace=True)
                BS_df.columns = pd.to_datetime(BS_df.columns)
                # Sort columns in chronological order
                BS_df = BS_df.sort_index(axis=1)
                last_year_BS = BS_df.iloc[:,-1]
                last_year_BS = BS_df.iloc[:,-1]

                # Remove columns containing the specified string
                columns_to_drop_IS = [col for col in IS_df.columns if string_to_check in col]
                IS_df = IS_df.drop(columns=columns_to_drop_IS, axis=1)
                IS_df.set_index('index', inplace=True)
                IS_df.columns = pd.to_datetime(IS_df.columns)
                # Sort columns in chronological order
                IS_df = IS_df.sort_index(axis=1)
                last_year_IS = IS_df.iloc[:,-1]
                last_year_IS = IS_df.iloc[:,-1]

                total_assets_2022 = last_year_BS['totalAssets']
                net_income_2022 = last_year_IS['netIncome']
                
                roa_ratio = net_income_2022 / total_assets_2022 * 100

                stock_names_list.append(stock_name)
                roa_ratios_list.append(roa_ratio)
                industries_list.append(loaded_data['Company profile data']['industry'])
                folders_list.append(folder_path)

                try:
                    p_to_book_list.append(loaded_data['Key statistics']['priceToBook'])
                except:
                    p_to_book_list.append('None')

            except FileNotFoundError:
                print(f"BS, IS or Summary for {stock_name} not found or path is incorrect.")
            except IndexError:
                print(f"No data for the year available for {stock_name}")
            except UnboundLocalError:
                pass

            #except KeyError:
                #print(f"Operating cash flow information not available for {stock_name}.")

        ROA_ratio_dict = {'Stock ticker': stock_names_list, "ROA ratio": roa_ratios_list, 'Price to Book': p_to_book_list, 'Industry': industries_list, 'Folder path': folders_list}
        ROA_df = pd.DataFrame(ROA_ratio_dict)
        ROA_df = ROA_df[(ROA_df['Industry'] != 'Asset Management') & (ROA_df['Industry'] != 'Credit Services') & (ROA_df['Industry'] != 'Insurance - Diversified')]
        filtered_ROA_df = ROA_df[(ROA_df['ROA ratio'] > threshold) & (ROA_df['ROA ratio'] > 0)]
        sorted_ROA_df = filtered_ROA_df.sort_values(by=["ROA ratio", 'Price to Book'], ascending=True)
        sorted_ROA_df.to_csv('High_ROA_ratios.csv', index=False)
        return sorted_ROA_df

    def CROIC_ratio(self, folder_paths_to_analyze, threshold):
        stock_names_list = []
        croic_ratios_list = []
        industries_list = []
        p_to_book_list = []
        folders_list = []
        for folder_path in folder_paths_to_analyze:
            # Check if the folder is empty
            stock_name = folder_path.split('\\')[-1]
            CF_file = folder_path + '\\' + stock_name + '_CF.csv'
            BS_file = folder_path + '\\' + stock_name + '_BS.csv'
            IS_file = folder_path + '\\' + stock_name + '_IS.csv'
            summary_file = folder_path + '\\' + stock_name + '_summary.pkl'
            try:
                CF_df = pd.read_csv(CF_file)
                BS_df = pd.read_csv(BS_file)
                IS_df = pd.read_csv(IS_file)
                with open(summary_file, 'rb') as file:
                    loaded_data = pickle.load(file)
                # Specify the string to check for in column names
                string_to_check = '2023'

                # Remove columns containing the specified string
                columns_to_drop = [col for col in CF_df.columns if string_to_check in col]
                CF_df = CF_df.drop(columns=columns_to_drop, axis=1)
                CF_df.set_index('index', inplace=True)
                CF_df.columns = pd.to_datetime(CF_df.columns)
                # Sort columns in chronological order
                CF_df = CF_df.sort_index(axis=1)
                last_year_CF = CF_df.iloc[:,-1]


                # Remove columns containing the specified string
                columns_to_drop = [col for col in BS_df.columns if string_to_check in col]
                BS_df = BS_df.drop(columns=columns_to_drop, axis=1)
                BS_df.set_index('index', inplace=True)
                BS_df.columns = pd.to_datetime(BS_df.columns)
                # Sort columns in chronological order
                BS_df = BS_df.sort_index(axis=1)
                last_year_BS = BS_df.iloc[:,-1]

                # Remove columns containing the specified string
                columns_to_drop = [col for col in IS_df.columns if string_to_check in col]
                IS_df = IS_df.drop(columns=columns_to_drop, axis=1)
                IS_df.set_index('index', inplace=True)
                IS_df.columns = pd.to_datetime(IS_df.columns)
                # Sort columns in chronological order
                IS_df = IS_df.sort_index(axis=1)
                last_year_IS = IS_df.iloc[:,-1]

                free_cash_flow_2022 = last_year_CF['cashFlowFromContinuingOperatingActivities']
                try:
                    capex_2022 = last_year_CF['capitalExpenditure']
                    free_cash_flow_2022 = free_cash_flow_2022 + capex_2022
                except KeyError:
                    pass

                total_assets = last_year_BS['totalAssets']
                current_liabilities = last_year_BS['currentLiabilities']
                cash = last_year_BS['cashAndCashEquivalents']

                total_revenue = last_year_IS['totalRevenue']
                total_revenue_five_percent = total_revenue * 0.05

                if cash > total_revenue_five_percent:
                    excess_cash = cash - total_revenue_five_percent
                else:
                    excess_cash = 0

                invested_capital = total_assets - current_liabilities - excess_cash
                croic_ratio = free_cash_flow_2022/invested_capital * 100
                stock_names_list.append(stock_name)
                croic_ratios_list.append(croic_ratio)
                industries_list.append(loaded_data['Company profile data']['industry'])
                folders_list.append(folder_path)

                try:
                    p_to_book_list.append(loaded_data['Key statistics']['priceToBook'])
                except:
                    p_to_book_list.append('None')

            except FileNotFoundError:
                print(f"Some financial report not found or path is incorrect.")
            except KeyError:
                print(f"Some financial information information not available for {stock_name}.")
            except IndexError:
                print(f"No data for the year available for {stock_name}")
            except UnboundLocalError:
                pass


        CROIC_ratio_dict = {'Stock ticker': stock_names_list, "CROIC ratio": croic_ratios_list, 'Price to Book': p_to_book_list, 'Industry': industries_list, 'Folder path': folders_list}
        CROIC_df = pd.DataFrame(CROIC_ratio_dict)
        CROIC_df = CROIC_df[(CROIC_df['Industry'] != 'Asset Management') & (CROIC_df['Industry'] != 'Credit Services') & (CROIC_df['Industry'] != 'Insurance - Diversified')]
        filtered_CROIC_df = CROIC_df[(CROIC_df['CROIC ratio'] > threshold) & (CROIC_df['CROIC ratio'] > 0)]
        sorted_CROIC_df = filtered_CROIC_df.sort_values(by=["CROIC ratio", 'Price to Book'], ascending=True)
        sorted_CROIC_df.to_csv('High_CROIC_ratios.csv', index=False)

        return sorted_CROIC_df

Analyzer = Analyzer_Class()