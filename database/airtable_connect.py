import unicodedata
import airtable
import pandas as pd
from datetime import datetime
import os
import requests

class GetAirTables:    
    def __init__(self, base_id=None, access_token=None):
        credentials = self.get_airtable_credentials()
        self.BASE_ID = base_id or credentials['base_id']
        self.ACCESS_TOKEN = access_token or credentials['access_token']
        self.at = airtable.Airtable(self.BASE_ID, self.ACCESS_TOKEN)

    @staticmethod
    def get_airtable_credentials():
        creds_str = os.environ.get('AIRTABLE_CREDS', '')
        creds_list = [cred.split(' ', 1) for cred in creds_str.split(',') if cred]
        return {
            'base_id': next((item[1] for item in creds_list if item[0] == 'base_id'), None),
            'access_token': next((item[1] for item in creds_list if item[0] == 'access_token'), None),
        }
    
    def get_timed_table(self, table_id, time_column='Last Modified'):
        current_time = datetime.now().replace(minute=0, second=0, microsecond=0)
        iso_time = current_time.isoformat() + 'Z'
        filter_formula = f"IS_BEFORE({{{time_column}}}, '{iso_time}')"
        
        table_data = self.at.get(table_id, filter_by_formula=filter_formula)
        
        records = table_data['records']
        df = pd.json_normalize([record['fields'] for record in records])
        
        return df
    
    def get_table(self, table_id):
        data = self.at.get(table_id)
        
        for r in self.at.iterate(table_id):
            if r not in data['records']:
                data['records'].append(r)
        
        base_schema = self.get_base_schema(self.BASE_ID)
        
        all_columns = []
        for table in base_schema["tables"]:
            if table['id'] == table_id:
                all_columns = [field['name'] for field in table['fields']]
                break
            
        df = pd.DataFrame(data=[record['fields'] for record in data['records']])
        df = df.reindex(columns=all_columns)
        
        return df
    
    def get_record_id(self, table_id, id_column, name_column, query):
        try:
            client_full_name, client_id = query
            records = self.at.get(table_id, fields=[name_column, id_column])
    
            for record in records['records']:
                fields = record.get('fields', {})
                
                name = fields.get(name_column)
                name = unicodedata.normalize('NFKC', name) if name else None
                
                client_ids = fields.get(id_column)
                record_id = record['id']
                
                if name == client_full_name and client_id in client_ids:
                    return record_id
            
        except Exception as e:
            print(f"Error in get_record_id: {str(e)}")
            return None

    def verify_update_tables(self, table_id, name_column, id_column, update_column):
        records = self.at.get(table_id, fields=[name_column, id_column, update_column])

        updated_clients = []
        if not records.get('records'):
            return updated_clients

        for record in records['records']:
            fields = record.get('fields', {})
            client_name = fields.get(name_column, "Unknown Name")
            client_id = fields.get(id_column, "ID not found")
            status = "Updated" if fields.get(update_column) is True else "Pending"
            updated_clients.append(f"{status}: {client_id} - {client_name}")

        return updated_clients

    def verify_and_update(self, table_id, name_column, id_column, update_column, query):
        try:
            client_full_name, client_id = query
            records = self.at.get(table_id, fields=[name_column, id_column, update_column])['records']
            
            for record in records:
                fields = record.get('fields', {})
                
                name = fields.get(name_column)
                id = fields.get(id_column)
                
                name = unicodedata.normalize("NFKC", name) if name else None
                id = unicodedata.normalize("NFKC", id) if id else None
                
                if name == client_full_name and id == client_id:
                    if not fields.get(update_column, False):
                        data = {update_column: True}
                        response = self.at.update(table_id, record['id'], data)
                        print(f"Update done on Airtable: {response['fields'][name_column]}")
                    return True
            
            print(f"Record not found: {query}")
            return False
                    
        except Exception as e:
            print(f"Error trying to check update: {str(e)}")
            return False
    
    def get_base_schema(self, base_id):
        url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
        headers = {
            "Authorization": f"Bearer {self.ACCESS_TOKEN}",
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error getting schema: {response.status_code} - {response.text}")
    
    def get_comments(self, table_id):
        all_comments = []
        
        for record in self.at.iterate(table_id):
            url = f"https://api.airtable.com/v0/{self.BASE_ID}/{table_id}/{record['id']}/comments"
            headers = {
                "Authorization": f"Bearer {self.ACCESS_TOKEN}",
            }
            
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                raise Exception(f"Error listing comments: {response.status_code}, {response.text}")
            
            comment_data = response.json().get("comments", [])
            
            for c in comment_data:
                c['record_id'] = record['id']
            
            all_comments.extend(comment_data)
            
        return pd.DataFrame(all_comments) if all_comments else pd.DataFrame()
    
    def create_comment(self, table_id, record_id, comment):
        url = f"https://api.airtable.com/v0/{self.BASE_ID}/{table_id}/{record_id}/comments"
        
        headers = {
            "Authorization": f"Bearer {self.ACCESS_TOKEN}",
            "Content-Type": "application/json"
        }
        
        data = {
            "text": comment
        }
        
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            print(f"Commented modifications on the client's table.")
            return True
        else:
            print(f"Error creating comment on {record_id}: {response.status_code}, {response.text}")
            return False
    
    def get_client_database(self, main_table_id, checking_table_id, id_column, name_column):
        try:
            clients_data = self.get_table(main_table_id)
            clients_checking = self.get_table(checking_table_id)

            clients_checking[name_column] = clients_checking[name_column].apply(
                lambda x: x[0] if isinstance(x, list) and len(x) == 1 else x
            )
            clients_checking[id_column] = clients_checking[id_column].apply(
                lambda x: x[0] if isinstance(x, list) and len(x) == 1 else x
            )

            clients_data['index'] = clients_data[id_column] + ' | ' + clients_data[name_column]
            clients_checking['index'] = clients_checking[id_column] + ' | ' + clients_checking[name_column]

            clients = pd.merge(clients_data, clients_checking, on="index", how='left')
            
            for col in [name_column, id_column]:
                clients[col] = clients.apply(
                    lambda row: row[f'{col}_x'] if row[f'{col}_x'] == row[f'{col}_y'] else None, axis=1
                )

            clients.drop(columns=[col for col in clients.columns if '_x' in col or '_y' in col], inplace=True)
            
            return clients
        
        except Exception as e:
            raise