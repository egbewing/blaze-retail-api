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
            dat = pd.DataFrame().from_records(pd.json_normalize(response.json().get('values')))
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
            dat = pd.DataFrame().from_records(pd.json_normalize(response.json().get('values')))
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
            dat = pd.DataFrame().from_records(pd.json_normalize(response.json().get('values')))
            return dat
        else:
            return f'Error retrieving inventory locations with status code {response.status_code}'

    def get_members(
            self,
            start_date: str=(datetime.today() - timedelta(days=1)).strftime('%m/%d/%Y'),
            end_date: str=datetime.today().strftime('%m/%d/%Y'),
            skip: int=0,
            limit: int=100
            ) -> pd.DataFrame:
        """Get DF of members under current context according to dates given. Dates given reflect member
        join date. If member joined between given dates, then member will be retrieved.

        Args:
            start_date (str, optional): start date of window. Defaults to (datetime.today() - timedelta(days=1)).strftime('%m/%d/%Y').
            end_date (str, optional): ending date of window. Defaults to datetime.today().strftime('%m/%d/%Y').
            skip (int, optional): records to skip. Defaults to 0.
            limit (int, optional): query limit. Defaults to 100.

        Returns:
            pd.DataFrame: DF of members and their attributes.
        """
        epoch_offset = 1000  # offset for datetime for BLAZE TS format
        _start_date = int(datetime.strptime(start_date, '%m/%d/%Y').timestamp() * epoch_offset)
        _end_date = int(datetime.strptime(end_date, '%m/%d/%Y').timestamp() * epoch_offset)
        url = 'https://api.partners.blaze.me/api/v1/partner/members'
        headers = {
            'partner_key': self.partner_key,
            'Authorization': self.Authorization
            }
        params = {
            'startDate': _start_date,
            'endDate': _end_date,
            'skip': skip,
            'limit': limit
            }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            dat = pd.DataFrame().from_records(pd.json_normalize(response.json().get('values')))
            if skip >= response.json().get('total'):
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
    def get_employees(self, skip: int=0, limit: int=200):

        url = 'https://api.partners.blaze.me/api/v1/partner/employees'
        headers = {
            'partner_key': self.partner_key,
            'Authorization': self.Authorization
            }
        params = {'start': skip, 'limit': limit}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            dat = pd.DataFrame().from_records(pd.json_normalize(response.json().get('values')))
            if skip >= response.json().get('total'):
                dat = pd.DataFrame().from_records(response.json().get('values'))
                return dat
            else:
                return(
                    pd.concat([
                        dat,
                        self.get_employees(
                            skip=skip+ response.json().get('limit'),
                            limit=limit
                            )
                        ])
                    )

    def get_item_sales(
            self,
            start_date: str=(datetime.today() - timedelta(days=1)).strftime('%m/%d/%Y'),
            end_date: str=datetime.today().strftime('%m/%d/%Y'),
            skip: int=0
            ):
        """Get line item sales for specified dates.

        Args:
            start_date (str, optional): date window start Defaults to (datetime.today() - timedelta(days=1)).strftime('%m/%d/%Y').
            end_date (str, optional): _description_. Defaults to datetime.today().strftime('%m/%d/%Y').
            skip (int, optional): _description_. Defaults to 0.
        """
        url = "https://api.partners.blaze.me/api/v1/partner/transactions"
        headers = {
                'partner_key': os.getenv('blz_partner_key'),
                'Authorization': os.getenv('blz_api_key')
                }
        params = {'startDate': start_date, 'endDate': end_date, 'skip': skip}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            dat = pd.DataFrame(
                list(
                    pd.json_normalize(
                        response.json().get('values'))\
                        ['cart.items'].explode().reset_index(drop=True)
                    )
                )
            if skip + response.json().get("limit") >= response.json().get('total'):
                return dat
            else:
                return pd.concat([
                    dat,
                    self.get_item_sales(
                        start_date=start_date,
                        end_date=end_date,
                        skip=skip + response.json().get('limit')
                        )
                    ])

#########################################################################################################################

if __name__ == '__main__':
    b = blaze_retail_api()
    s = b.get_item_sales()
    e = b.get_employees()