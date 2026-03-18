# /plugins/fmp/client.py
import requests
from airflow.models import Variable
from datetime import date

today = str(date.today())

class FMPClient:
    def __init__(self):
        """
        Basic Configuration, get url and key from config settings
        """
        self.base_url = Variable.get("FMP_BASE_URL")
        self.api_key = Variable.get("FMP_API_KEY")

    def _get(self, endpoint, params=None):
        """
        Base get resquests build, we will call this with every param
        """
        url = f"{self.base_url}{endpoint}"
        params = params or {}
        params['apikey'] = self.api_key

        #print(url)
        #print(params)

        # Here, requests build the url correctly even if params is dict
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        return resp.json()
    
    
    #################################################################
       ######################### Endpoints ######################
    #################################################################

    # Company info
    def get_profile(self,symbol):
        return self._get(f'profile?symbol={symbol}')[0] # It returns the first [0] because api returns [{}]
    
    # Company symbol and name but with 36k companies!!
    def get_stock_symbol(self):
        params = {'marketCapMoreThan': '10000000000', 'isEtf': 'False', 
                  'isFund': 'False', 'limit': '3000' }
        return self._get('company-screener', params=params)
    
    # Stock price historical
    def get_historical_price(self,symbol,dateFrom='2010-01-01',dateTo=today):
        params = {'from':dateFrom, 'to':dateTo}
        return self._get(f'historical-price-eod/full?symbol={symbol}', params=params)

    # Income statement
    def get_income_statement(self,symbol,period,limit=16):
        params = {'period':period, 'limit':limit}
        return self._get(f'income-statement?symbol={symbol}', params=params)
    
    # Balance sheet
    def get_balance_sheet(self,symbol,period,limit=16):
        params = {'period': period, 'limit': limit}
        return self._get(f'balance-sheet-statement?symbol={symbol}', params=params)
    
    # Cashflow statement
    def get_cashflow_statement(self,symbol,period,limit=16):
        params = {'period': period, 'limit':limit}
        return self._get(f'cash-flow-statement?symbol={symbol}', params=params)
    
    # Company snapshot info
    def get_company_snapshot(self,symbol):
        return self._get(f'search-exchange-variants?symbol={symbol}')
    
    # Segment revenue by product endpoint
    def get_product_revenue(self,symbol):
        params = {'period':'annual'} # Only have access to annual period
        return self._get(f'revenue-product-segmentation?symbol={symbol}',params=params)
    
    # Revenue by geographic segment
    def get_geographic_segment(self,symbol):
        params = {'period':'annual'} # Only have access to annual period
        return self._get(f'revenue-geographic-segmentation?symbol={symbol}', params=params)

    # Historical market cap
    def get_historical_mktcap(self,symbol,from_d,to_d):
        params = {'from': from_d, 'to':to_d}
        return self._get(f'historical-market-capitalization?symbol={symbol}',params=params)