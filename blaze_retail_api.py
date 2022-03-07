import pandas as pd
from glob import glob
import os
import requests
from datetime import datetime, timedelta

class blaze_retail_api():


    def __init__(
            self,
            partner_key=os.getenv('blz_partner_key'),
            Authorization = os.getenv('blz_api_key')
            ) -> None:
        try:
            self.partner_key = partner_key
            self.Authorization = Authorization
        except Exception as e:
            print(e)
        assert not (self.partner_key is None or self.Authorization is None)

        self.inventories = self._get_inventory_locations().set_index('name')['id'].to_dict()


    def get_products(self, skip: int=0) -> pd.DataFrame:
        """
        Retrieve all products from BLAZE API.
        Args:
            skip (int, optional): Nbr records to skip at API call. Defaults to 0.
        Returns:
            pd.DataFrame: df of all products
        """
        url = "https://api.partners.blaze.me/api/v1/partner/products"

        headers = {
            'partner_key': self.partner_key,
            'Authorization': self.Authorization
            }
        params = {'skip': skip}

        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            dat = pd.DataFrame().from_records(response.json().get('values'))
            if skip  >= response.json().get('total'):
                return dat
            else:
                return(
                    pd.concat([
                        dat,
                        self.get_products(skip=skip + response.json().get('limit'))
                    ])
                )
        else:
            return f'Error retrieving products: with status code {response.status_code}'


    def get_vendors(self, skip: int=0) -> pd.DataFrame:
        """
        Get all vendors from BLAZE, recursively
        Args:
            skip (int, optional): nbr records to skip. Defaults to 0.
        Returns:
            pd.DataFrame: vendors data
        """
        url = "https://api.partners.blaze.me/api/v1/partner/vendors"

        headers = {
            'partner_key': self.partner_key,
            'Authorization': self.Authorization
            }
        params = {'skip': skip}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            dat = pd.DataFrame().from_records(response.json().get('values')).rename(
                columns={
                    'name': 'vendor_name'
                    }
                ).drop_duplicates('id')
            if skip >= response.json().get('total'):
                return dat
            else:
                return(
                    pd.concat([
                        dat,
                        self.get_vendors(skip=skip + response.json().get('limit'))
                    ])
                )
        else:
            return f'Error retrieving products: with status code {response.status_code}'


    def get_brands(self, skip: int=0) -> pd.DataFrame:
        """
        Get all brands from BLAZE API recursively
        Args:
            skip (int, optional): nbr of records to skip (api param). Defaults to 0.
        Returns:
            pd.DataFrame: Brands data
        """
        url = "https://api.partners.blaze.me/api/v1/partner/store/inventory/brands"

        headers = {
            'partner_key': self.partner_key,
            'Authorization': self.Authorization
            }
        params = {'limit': 200, 'start': skip}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            dat = pd.DataFrame().from_records(response.json().get('values')).rename(
                columns={
                    'name': 'brand_name', 'id': 'brand_id'
                    }
                ).drop_duplicates('brand_id')
            if skip >= response.json().get('total'):
                return dat
            else:
                return(
                    pd.concat([
                        dat,
                        self.get_brands(skip=skip + response.json().get('limit'))
                    ])
                )
        else:
            return f'Error retrieving products: with status code {response.status_code}'


    def get_curr_inventory(self, skip: int=0, inventory: str='Safe') -> pd.DataFrame:
        """
        Retrieve batch quantities from BLAZE.
        Args:
            skip (int, optional): nbr records to skip at API call. Defaults to 0.
            inventory (str, optional): inventory to query. Defaults to 'safe'.
        Returns:
            pd.DataFrame: batch quantity df
        """

        url = "https://api.partners.blaze.me/api/v1/partner/store/batches/quantities"
        headers = {
            'partner_key': self.partner_key,
            'Authorization': self.Authorization
            }
        params = {'inventoryId': self.inventories.get(inventory), 'start': skip}

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            dat = pd.DataFrame().from_records(response.json().get('values'))
            if skip >= response.json().get('total'):
                return dat
            else:
                return (
                    pd.concat([
                        dat,
                        self.get_curr_inventory(
                            skip=skip + response.json().get('limit'),
                            inventory=inventory
                            )
                        ])
                    )
        else:
            return (f'Error retrieving inventory: {inventory} with '
            f'id {self.inventories.get(inventory)} with status code {response.status_code}')


    def _get_inventory_locations(self) -> pd.DataFrame:
        """Gets inventory locations for the current shop context. Primarily used for __init__
        but can be called directly. Or use <object>.inventories for a list of inventories
        in the current context.

        Returns:
            pd.DataFrame: DF of inventories active in BLAZE retail and their attributes
        """
        url = 'https://api.partners.blaze.me/api/v1/partner/store/inventory/inventories'
        headers = {
            'partner_key': self.partner_key,
            'Authorization': self.Authorization
            }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            dat = pd.DataFrame().from_records(response.json().get('values'))
            return dat
        else:
            return f'Error retrieving inventory locations with status code {response.status_code}'

    def get_members(
            self,
            start_date: datetime.timestamp=(datetime.today() - timedelta(days=1)).timestamp() * 1000,
            end_date: datetime.timestamp=datetime.today().timestamp() * 1000,
            skip: int=0,
            limit: int=100
            ) -> pd.DataFrame:
        url = 'https://api.partners.blaze.me/api/v1/partner/members'
        headers = {
            'partner_key': self.partner_key,
            'Authorization': self.Authorization
            }
        params = {
            'startDate': start_date,
            'endDate': end_date,
            'skip': skip,
            'limit': limit
            }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            if skip >= response.json().get('total'):
                dat = pd.DataFrame().from_records(response.json().get('values'))
                return dat
            else:
                return(
                    pd.concat([
                        dat,
                        self.get_members(
                            start_date=start_date,
                            end_date=end_date,
                            skip=skip + response.json().get('limit')
                        )
                    ])
                )
        
#####################################################################################################
#TODO FIX HOW SALES ARE FORMATTED FOR EASIER USE IN DF OR OTHERWISE
#####################################################################################################
    def get_item_sales(self,
            start: str=(datetime.today() - timedelta(days=1)).strftime('%m/%d/%Y'),
            end: str=datetime.today().strftime('%m/%d/%Y'),
            skip=0
            ):
        """Currently unused, needs revision. See comments in main()
        Args:
            start (str, optional): [description]. Defaults to (datetime.today() - timedelta(days=1)).strftime('%m/%d/%Y').
            end (str, optional): [description]. Defaults to datetime.today().strftime('%m/%d/%Y').
            skip (int, optional): [description]. Defaults to 0.
        Returns:
            [type]: [description]
        """

        url = "https://api.partners.blaze.me/api/v1/partner/transactions"
        headers = {
                'partner_key': os.getenv('blz_partner_key'),
                'Authorization': os.getenv('blz_api_key')
                }
        params = {'startDate': start, 'endDate': end, 'skip': skip}
        response = requests.get(url, headers=headers, params=params)
        if response.ok:
            dat = pd.DataFrame().from_dict(response.json().get('values'))
            itms = dat.cart.apply(lambda x: x.get('items'))
            itms = itms.apply(lambda x: pd.DataFrame().from_records(x))
            itms = combine_txns(itms)
            dat['joinid'] = dat.id.apply(lambda x: x[0:-3])
            dat = dat.rename(columns={'id': 'txn_id'})
            dat['created_dt'] = dat.created.apply(lambda x: datetime.fromtimestamp(x/1000))
            dat['completedTime'] = dat['completedTime'].apply(lambda x: datetime.fromtimestamp(x/1000))
            itms['joinid'] = itms.id.apply(lambda x: x[0:-3])
            ln_itm_txns = dat.merge(
                itms,
                how='left',
                on='joinid'
                )
            ln_itm_txns = ln_itm_txns[[
                'created_dt',
                'completedTime',
                'transNo',
                'productId',
                'quantity'
                ]]
            if skip + response.json().get("limit") >= response.json().get('total'):
                return ln_itm_txns
            else:
                return pd.concat([
                    ln_itm_txns,
                    get_sls(start=start, end=end, skip=skip + response.json().get('limit'))
                    ])
        else:
            return f'Error retrieving sls data from BLAZE -- response code: {response.status_code}:{response.reason}'

#########################################################################################################################

if __name__ == '__main__':
    b = blaze_retail_api()
    i = b.get_curr_inventory()