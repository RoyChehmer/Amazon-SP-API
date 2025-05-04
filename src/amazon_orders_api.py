import requests
import json
from datetime import datetime, timezone, timedelta
import os
import time
from requests.exceptions import RequestException
import psycopg2
from psycopg2.extras import Json
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Integer, Float, JSON, Boolean
from sqlalchemy.dialects.postgresql import TIMESTAMP, JSONB
import logging
import re


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""
SP-API Region Code	SP-API Endpoint	                        AWS Region	Marketplaces
'na'	            https://sellingpartnerapi-na.amazon.com	us-east-1	US, Canada, Mexico
'eu'	            https://sellingpartnerapi-eu.amazon.com	eu-west-1	UK, Germany, France, Italy, Spain, etc.
'fe'	            https://sellingpartnerapi-fe.amazon.com	us-west-2	Brazil, Argentina, Chile, Peru, etc.

Country	Marketplace	Marketplace ID
🇺🇸 USA	amazon.com	ATVPDKIKX0DER
🇨🇦 Canada	amazon.ca	A2EUQ1WTGCTBG2
🇲🇽 Mexico	amazon.com.mx	A1AM78C64UM0Y8

Country	Marketplace	Marketplace ID
🇬🇧 UK	amazon.co.uk	A1F83G8C2ARO7P
🇩🇪 Germany	amazon.de	A1PA6795UKMFR9
🇫🇷 France	amazon.fr	A13V1IB3VIYZZH
🇮🇹 Italy	amazon.it	APJ6JRA9NG5V4
🇪🇸 Spain	amazon.es	A1RKKUPIHCS9HS
🇳🇱 Netherlands	amazon.nl	A1805IZSGTT6HS
🇸🇪 Sweden	amazon.se	A2NODRKZP88ZB9
🇵🇱 Poland	amazon.pl	A1C3SOZRARQ6R3
🇹🇷 Turkey	amazon.com.tr	A33AVAJ2PDY3EV
🇧🇪 Belgium	amazon.com.be	AMEN7PMS3EDWL

Country	Marketplace	Marketplace ID
🇯🇵 Japan	amazon.co.jp	A1VC38T7YXB528
🇮🇳 India	amazon.in	A21TJRUUN4KGV
🇦🇺 Australia	amazon.com.au	A39IBJ37TRP1C6
🇸🇬 Singapore	amazon.sg	A19VAU5U5O7RUS
"""

class AmazonOrdersAPI:
    def __init__(self, client_id, client_secret, refresh_token, region='na', db_config=None,
                 max_retries=5, max_wait_time=32):
        """
        Initialize the Amazon Orders API client
        
        Args:
            client_id (str): Your SP API client ID
            client_secret (str): Your SP API client secret
            refresh_token (str): Your SP API refresh token
            region (str): The region for the API endpoint (default: 'na')
            db_config (dict): Database configuration
            max_retries (int): Maximum number of retry attempts (default: 5)
            max_wait_time (int): Maximum wait time in seconds between retries (default: 32)
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.region = region
        self.access_token = None
        self.token_expiry = None
        self.base_url = f'https://sellingpartnerapi-{region}.amazon.com'
        self.max_retries = max_retries
        self.max_wait_time = max_wait_time
        
        # Database configuration
        self.db_config = db_config or {
            'host': 'localhost',
            'port': '5432',
            'database': 'amazon',
            'user': 'user',
            'password': 'password'
        }
        
        # Check if database exists and initialize connection
        if self._check_database_exists():
            self._init_db()
            self._create_tables()
        else:
            logger.error(f"Database {self.db_config['database']} does not exist. Please create it first.")
            raise Exception(f"Database {self.db_config['database']} does not exist")

    def _check_database_exists(self):
        """Check if the database exists and create it if it doesn't"""
        try:
            # Connect to the default PostgreSQL database
            conn = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database='postgres'  # Connect to default database
            )
            conn.autocommit = True  # Required for creating databases
            
            # Create cursor
            cur = conn.cursor()
            
            # Check if database exists
            check_if_DB_exist = f"SELECT 1 FROM pg_database WHERE datname = '{self.db_config['database']}'"
            cur.execute(check_if_DB_exist)
            exists = cur.fetchone()
            
            if not exists:
                # Create database
                create_db_query = f"CREATE DATABASE {self.db_config['database']}"
                cur.execute(create_db_query)
                logger.info(f"Database {self.db_config['database']} created successfully")
            else:
                logger.info(f"Database {self.db_config['database']} already exists")
            
            # Close cursor and connection
            cur.close()
            conn.close()
            
            return True  # Return True as we either found or created the database
            
        except Exception as e:
            logger.error(f"Error checking/creating database: {str(e)}")
            raise

    def _init_db(self):
        """Initialize database connection"""
        try:
            # Connect to our specific database
            self.engine = create_engine(
                f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            )
            self.metadata = MetaData()
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise

    def _create_tables(self):
        """Create tables if they don't exist"""
        try:
            # Create orders table
            orders_table = Table(
                'amazon_orders',
                self.metadata,
                Column('amazon_order_id', String, primary_key=True),
                Column('purchase_date', TIMESTAMP(timezone=True)),
                Column('last_update_date', TIMESTAMP(timezone=True)),
                Column('order_status', String),
                Column('fulfillment_channel', String),
                Column('sales_channel', String),
                Column('order_channel', String),
                Column('ship_service_level', String),
                Column('shipping_address', JSONB),
                Column('order_total', JSONB),
                Column('number_of_items_shipped', Integer),
                Column('number_of_items_unshipped', Integer),
                Column('payment_execution_detail', JSONB),
                Column('payment_method', String),
                Column('marketplace_id', String),
                Column('buyer_info', JSONB),
                Column('buyer_email', String),
                Column('shipping_state', String),
                Column('shipping_postal_code', String),
                Column('shipping_city', String),
                Column('shipping_country_code', String),
                Column('order_currency', String),
                Column('order_amount', String),
                Column('automated_shipping_settings', JSONB),
                Column('has_regulated_items', Boolean),
                Column('easy_ship_shipment_status', String),
                Column('cba_displayable_shipping_label', String),
                Column('order_type', String),
                Column('earliest_ship_date', TIMESTAMP(timezone=True)),
                Column('latest_ship_date', TIMESTAMP(timezone=True)),
                Column('earliest_delivery_date', TIMESTAMP(timezone=True)),
                Column('latest_delivery_date', TIMESTAMP(timezone=True)),
                Column('is_business_order', Boolean),
                Column('is_prime', Boolean),
                Column('is_premium_order', Boolean),
                Column('is_global_express_enabled', Boolean),
                Column('replaced_order_id', String),
                Column('is_replacement_order', Boolean),
                Column('promise_response_due_date', TIMESTAMP(timezone=True)),
                Column('is_estimated_ship_date_set', Boolean),
                Column('is_sold_by_ab', Boolean),
                Column('is_iba', Boolean),
                Column('default_ship_from_location_address', JSONB),
                Column('buyer_invoice_preference', String),
                Column('is_access_point_order', Boolean),
                Column('seller_order_id', String),
                Column('seller_note', String),
                Column('created_at', TIMESTAMP(timezone=True), default=datetime.now(timezone.utc)),
                Column('updated_at', TIMESTAMP(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
            )

            # Create order items table
            order_items_table = Table(
                'amazon_order_items',
                self.metadata,
                Column('id', Integer, primary_key=True),
                Column('amazon_order_id', String),
                Column('asin', String),
                Column('seller_sku', String),
                Column('order_item_id', String),
                Column('title', String),
                Column('quantity_ordered', Integer),
                Column('quantity_shipped', Integer),
                Column('product_info', JSONB),
                Column('points_granted', JSONB),
                Column('item_price', JSONB),
                Column('shipping_price', JSONB),
                Column('item_tax', JSONB),
                Column('shipping_tax', JSONB),
                Column('shipping_discount', JSONB),
                Column('shipping_discount_tax', JSONB),
                Column('promotion_discount', JSONB),
                Column('promotion_discount_tax', JSONB),
                Column('promotion_ids', JSONB),
                Column('cod_fee', JSONB),
                Column('cod_fee_discount', JSONB),
                Column('is_gift', Boolean),
                Column('condition_note', String),
                Column('condition_id', String),
                Column('condition_subtype_id', String),
                Column('scheduled_delivery_start_date', TIMESTAMP(timezone=True)),
                Column('scheduled_delivery_end_date', TIMESTAMP(timezone=True)),
                Column('price_designation', String),
                Column('tax_collection', JSONB),
                Column('serial_number_required', Boolean),
                Column('is_transparency', Boolean),
                Column('ioss_number', String),
                Column('store_chain_store_id', String),
                Column('deemed_reseller_category', String),
                Column('created_at', TIMESTAMP(timezone=True), default=datetime.now(timezone.utc)),
                Column('updated_at', TIMESTAMP(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
            )

            # Create order details table
            order_details_table = Table(
                'amazon_order_details',
                self.metadata,
                Column('amazon_order_id', String, primary_key=True),
                Column('purchase_date', TIMESTAMP(timezone=True)),
                Column('last_update_date', TIMESTAMP(timezone=True)),
                Column('order_status', String),
                Column('fulfillment_channel', String),
                Column('sales_channel', String),
                Column('order_channel', String),
                Column('ship_service_level', String),
                Column('shipping_address', JSONB),
                Column('order_total', JSONB),
                Column('number_of_items_shipped', Integer),
                Column('number_of_items_unshipped', Integer),
                Column('payment_execution_detail', JSONB),
                Column('payment_method', String),
                Column('marketplace_id', String),
                Column('buyer_info', JSONB),
                Column('automated_shipping_settings', JSONB),
                Column('has_regulated_items', Boolean),
                Column('easy_ship_shipment_status', String),
                Column('cba_displayable_shipping_label', String),
                Column('order_type', String),
                Column('earliest_ship_date', TIMESTAMP(timezone=True)),
                Column('latest_ship_date', TIMESTAMP(timezone=True)),
                Column('earliest_delivery_date', TIMESTAMP(timezone=True)),
                Column('latest_delivery_date', TIMESTAMP(timezone=True)),
                Column('is_business_order', Boolean),
                Column('is_prime', Boolean),
                Column('is_premium_order', Boolean),
                Column('is_global_express_enabled', Boolean),
                Column('replaced_order_id', String),
                Column('is_replacement_order', Boolean),
                Column('promise_response_due_date', TIMESTAMP(timezone=True)),
                Column('is_estimated_ship_date_set', Boolean),
                Column('is_sold_by_ab', Boolean),
                Column('is_iba', Boolean),
                Column('default_ship_from_location_address', JSONB),
                Column('buyer_invoice_preference', String),
                Column('is_access_point_order', Boolean),
                Column('seller_order_id', String),
                Column('seller_note', String),
                Column('created_at', TIMESTAMP(timezone=True), default=datetime.now(timezone.utc)),
                Column('updated_at', TIMESTAMP(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
            )

            # Create the tables
            self.metadata.create_all(self.engine)
            logger.info("Tables created successfully")

        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            raise

    def _save_response(self, data, table_name=None):
        """
        Save API response to the database only

        Args:
            data (dict): The API response data
            table_name (str): Name of the table to save data to
        """
        if table_name:
            try:
                if 'payload' in data:
                    if 'Orders' in data['payload']:
                        for order in data['payload']['Orders'][:1]:
                            self._save_to_table('amazon_orders', order)
                            break
                    elif 'OrderItems' in data['payload']:
                        # Handle order items
                        for item in data['payload']['OrderItems']:
                            item['amazon_order_id'] = data['payload'].get('AmazonOrderId')
                            self._save_to_table('amazon_order_items', item)
                    else:
                        # Handle single order details
                        self._save_to_table('amazon_order_details', data['payload'])
                else:
                    # Handle other response types
                    self._save_to_table(table_name, data)

            except Exception as e:
                logger.error(f"Error saving to database: {str(e)}")
                # Continue execution even if database save fails

    def _save_to_table(self, table_name, data):
        """
        Save data to a specific table
        
        Args:
            table_name (str): Name of the table
            data (dict): Data to save
        """
        try:
            # Convert data to DataFrame
            # TODO: remove [:1] if you want to save all data
            df = pd.json_normalize(data)
            
            # Define column mappings
            column_mappings = {
                'amazon_orders': {
                    'AmazonOrderId': 'amazon_order_id',
                    'PurchaseDate': 'purchase_date',
                    'LastUpdateDate': 'last_update_date',
                    'OrderStatus': 'order_status',
                    'FulfillmentChannel': 'fulfillment_channel',
                    'SalesChannel': 'sales_channel',
                    'OrderChannel': 'order_channel',
                    'ShipServiceLevel': 'ship_service_level',
                    'ShippingAddress': 'shipping_address',
                    'OrderTotal': 'order_total',
                    'NumberOfItemsShipped': 'number_of_items_shipped',
                    'NumberOfItemsUnshipped': 'number_of_items_unshipped',
                    'PaymentExecutionDetail': 'payment_execution_detail',
                    'PaymentMethod': 'payment_method',
                    'MarketplaceId': 'marketplace_id',
                    'BuyerInfo': 'buyer_info',
                    'BuyerInfo.BuyerEmail': 'buyer_email',
                    'ShippingAddress.StateOrRegion': 'shipping_state',
                    'ShippingAddress.PostalCode': 'shipping_postal_code',
                    'ShippingAddress.City': 'shipping_city',
                    'ShippingAddress.CountryCode': 'shipping_country_code',
                    'OrderTotal.CurrencyCode': 'order_currency',
                    'OrderTotal.Amount': 'order_amount',
                    'AutomatedShippingSettings': 'automated_shipping_settings',
                    'HasRegulatedItems': 'has_regulated_items',
                    'EasyShipShipmentStatus': 'easy_ship_shipment_status',
                    'CbaDisplayableShippingLabel': 'cba_displayable_shipping_label',
                    'OrderType': 'order_type',
                    'EarliestShipDate': 'earliest_ship_date',
                    'LatestShipDate': 'latest_ship_date',
                    'EarliestDeliveryDate': 'earliest_delivery_date',
                    'LatestDeliveryDate': 'latest_delivery_date',
                    'IsBusinessOrder': 'is_business_order',
                    'IsPrime': 'is_prime',
                    'IsPremiumOrder': 'is_premium_order',
                    'IsGlobalExpressEnabled': 'is_global_express_enabled',
                    'ReplacedOrderId': 'replaced_order_id',
                    'IsReplacementOrder': 'is_replacement_order',
                    'PromiseResponseDueDate': 'promise_response_due_date',
                    'IsEstimatedShipDateSet': 'is_estimated_ship_date_set',
                    'IsSoldByAB': 'is_sold_by_ab',
                    'IsIBA': 'is_iba',
                    'DefaultShipFromLocationAddress': 'default_ship_from_location_address',
                    'BuyerInvoicePreference': 'buyer_invoice_preference',
                    'IsAccessPointOrder': 'is_access_point_order',
                    'SellerOrderId': 'seller_order_id',
                    'SellerNote': 'seller_note',
                    'PaymentMethodDetails': 'payment_method_details',
                    'ShipmentServiceLevelCategory': 'shipment_service_level_category',
                    'IsISPU': 'is_ispu',
                },
                'amazon_order_details': {
                    'AmazonOrderId': 'amazon_order_id',
                    'PurchaseDate': 'purchase_date',
                    'LastUpdateDate': 'last_update_date',
                    'OrderStatus': 'order_status',
                },
                'amazon_order_items': {
                    'IsGift': 'is_gift',
                    'IsTransparency': 'is_transparency',                    
                    'ASIN': 'asin',
                    'OrderItemId': 'order_item_id',
                    'SellerSKU': 'seller_sku',
                    'Title': 'title',
                    'QuantityOrdered': 'quantity_ordered',
                    'QuantityShipped': 'quantity_shipped',
                }
            }
            
            # Use the appropriate mapping for the current table
            column_mapping = column_mappings.get(table_name, {})
            df = df.rename(columns=column_mapping)
            
            # Add timestamps if they don't exist
            if 'created_at' not in df.columns:
                df['created_at'] = datetime.now(timezone.utc)
            if 'updated_at' not in df.columns:
                df['updated_at'] = datetime.now(timezone.utc)        

            # Output/log the insert statement and data
            # logger.info(f"Preparing to insert into table '{table_name}':")
            # logger.info(f"Columns: {list(df.columns)}")
            # logger.info(f"Data: {df.to_dict(orient='records')}")

            # Only keep columns that exist in the table
            valid_columns_dict = {
                'amazon_orders': [
                    'amazon_order_id', 'purchase_date', 'last_update_date', 'order_status', 'fulfillment_channel',
                    'sales_channel', 'order_channel', 'ship_service_level', 'shipping_address', 'order_total',
                    'number_of_items_shipped', 'number_of_items_unshipped', 'payment_execution_detail', 'payment_method',
                    'marketplace_id', 'buyer_info', 'buyer_email', 'shipping_state', 'shipping_postal_code', 'shipping_city',
                    'shipping_country_code', 'order_currency', 'order_amount', 'automated_shipping_settings', 'has_regulated_items',
                    'easy_ship_shipment_status', 'cba_displayable_shipping_label', 'order_type', 'earliest_ship_date',
                    'latest_ship_date', 'earliest_delivery_date', 'latest_delivery_date', 'is_business_order', 'is_prime',
                    'is_premium_order', 'is_global_express_enabled', 'replaced_order_id', 'is_replacement_order',
                    'promise_response_due_date', 'is_estimated_ship_date_set', 'is_sold_by_ab', 'is_iba',
                    'default_ship_from_location_address', 'buyer_invoice_preference', 'is_access_point_order', 'seller_order_id',
                    'seller_note', 'created_at', 'updated_at'
                ],
                'amazon_order_details': [
                    'amazon_order_id', 'purchase_date', 'last_update_date', 'order_status', 'fulfillment_channel',
                    'sales_channel', 'order_channel', 'ship_service_level', 'shipping_address', 'order_total',
                    'number_of_items_shipped', 'number_of_items_unshipped', 'payment_execution_detail', 'payment_method',
                    'marketplace_id', 'buyer_info', 'automated_shipping_settings', 'has_regulated_items',
                    'easy_ship_shipment_status', 'cba_displayable_shipping_label', 'order_type', 'earliest_ship_date',
                    'latest_ship_date', 'earliest_delivery_date', 'latest_delivery_date', 'is_business_order', 'is_prime',
                    'is_premium_order', 'is_global_express_enabled', 'replaced_order_id', 'is_replacement_order',
                    'promise_response_due_date', 'is_estimated_ship_date_set', 'is_sold_by_ab', 'is_iba',
                    'default_ship_from_location_address', 'buyer_invoice_preference', 'is_access_point_order',
                    'seller_order_id', 'seller_note', 'created_at', 'updated_at'
                ],
                'amazon_order_items': [
                    'id', 'amazon_order_id', 'asin', 'seller_sku', 'order_item_id', 'title', 'quantity_ordered',
                    'quantity_shipped', 'product_info', 'points_granted', 'item_price', 'shipping_price', 'item_tax',
                    'shipping_tax', 'shipping_discount', 'shipping_discount_tax', 'promotion_discount', 'promotion_discount_tax',
                    'promotion_ids', 'cod_fee', 'cod_fee_discount', 'is_gift', 'condition_note', 'condition_id',
                    'condition_subtype_id', 'scheduled_delivery_start_date', 'scheduled_delivery_end_date', 'price_designation',
                    'tax_collection', 'serial_number_required', 'is_transparency', 'ioss_number', 'store_chain_store_id',
                    'deemed_reseller_category', 'created_at', 'updated_at'
                ],
            }
            df = df[[col for col in df.columns if col in valid_columns_dict[table_name]]]

            # Save to database
            df.to_sql(table_name, self.engine, if_exists='append', index=False, method='multi')
            logger.info(f"Data saved to {table_name}")
            
        except Exception as e:
            logger.error(f"Error saving to table {table_name}: {str(e)}")
            raise

    def get_access_token(self):
        """
        Get a new access token using the refresh token
        """
        token_url = 'https://api.amazon.com/auth/o2/token'
        
        payload = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token,
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        try:
            response = requests.post(token_url, data=payload, headers=headers)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data['access_token']
            # Set token expiry to 1 hour from now (Amazon tokens typically expire in 1 hour)
            self.token_expiry = datetime.now() + timedelta(hours=1)
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to get access token: {str(e)}")

    def _make_request(self, endpoint, params=None, table_name=None):
        """
        Make an authenticated request to the SP API with exponential backoff retry
        
        Args:
            endpoint (str): The API endpoint to call
            params (dict): Query parameters for the request
            table_name (str): Name of the table to save data to
            
        Returns:
            dict: Response from the API
        """
        if not self.access_token or datetime.now() >= self.token_expiry:
            self.get_access_token()
            
        headers = {
            'x-amz-access-token': self.access_token,
            'Content-Type': 'application/json'
        }
        
        url = f"{self.base_url}{endpoint}"
        
        # Initialize retry variables
        retry_count = 0
        wait_time = 1  # Start with 1 second wait
        
        while True:
            try:
                response = requests.get(url, headers=headers, params=params)
                
                # If we get a 429 error and haven't exceeded max retries, wait and retry
                if response.status_code == 429 and retry_count < self.max_retries:
                    logger.warning(f"Rate limit exceeded. Waiting {wait_time} seconds before retry {retry_count + 1}/{self.max_retries}")
                    time.sleep(wait_time)
                    wait_time = min(wait_time * 2, self.max_wait_time)  # Double the wait time, but don't exceed max
                    retry_count += 1
                    continue
                
                # For any other status code, raise an exception
                if response.status_code != 200:
                    raise Exception(f"API request failed with status code {response.status_code}: {response.text}")
                
                # If we get here, the request was successful
                data = response.json()
                #logger.info(f"Request successful: {data}")
                if table_name:
                    self._save_response(data, table_name)
                return data
                
            except RequestException as e:
                if retry_count < self.max_retries:
                    logger.warning(f"Request failed: {str(e)}. Waiting {wait_time} seconds before retry {retry_count + 1}/{self.max_retries}")
                    time.sleep(wait_time)
                    wait_time = min(wait_time * 2, self.max_wait_time)
                    retry_count += 1
                    continue
                raise Exception(f"Request failed after {self.max_retries} retries: {str(e)}")

    def _get_all_pages(self, endpoint, params=None, table_name=None, page_limit=None):
        """
        Get all pages of results for a paginated endpoint
        
        Args:
            endpoint (str): The API endpoint to call
            params (dict): Query parameters for the request
            table_name (str): Name of the table to save data to
            page_limit (int): Maximum number of pages to fetch (None for all pages)
            
        Returns:
            list: List of all responses from all pages
        """
        all_responses = []
        next_token = None
        page_count = 0
        
        while True:
            if page_limit and page_count >= page_limit:
                break
                
            current_params = params.copy() if params else {}
            if next_token:
                current_params['NextToken'] = next_token
                
            response = self._make_request(
                endpoint,
                params=current_params,
                table_name=table_name
            )
            
            all_responses.append(response)
            page_count += 1
            
            # Check for nextToken in the response
            next_token = response.get('payload', {}).get('NextToken')            
            if not next_token:
                break
                
        return all_responses
        
    def get_orders(self, created_after=None, created_before=None, order_statuses=None, 
                  marketplace_ids=None, next_token=None, get_all_pages=False, 
                  page_limit=None, table_name='amazon_orders'):
        """
        Get orders from Amazon SP API
        
        Args:
            created_after (str): ISO 8601 date format
            created_before (str): ISO 8601 date format
            order_statuses (list): List of order statuses to filter by
            marketplace_ids (list): List of marketplace IDs
            next_token (str): Token for pagination
            get_all_pages (bool): Whether to fetch all pages of results
            page_limit (int): Maximum number of pages to fetch (only used if get_all_pages is True)
            table_name (str): Name of the table to save data to
            
        Returns:
            dict or list: Single response or list of responses from all pages
        """
        params = {}
        if created_after:
            params['CreatedAfter'] = created_after
        if created_before:
            params['CreatedBefore'] = created_before
        if order_statuses:
            params['OrderStatuses'] = order_statuses
        if marketplace_ids:
            params['MarketplaceIds'] = marketplace_ids
        if next_token:
            params['NextToken'] = next_token
            
        if get_all_pages:
            return self._get_all_pages('/orders/v0/orders', params, table_name, page_limit)
        else:
            return self._make_request('/orders/v0/orders', params, table_name)
    
    def get_order_details(self, order_id, table_name='amazon_order_details'):
        """
        Get detailed information about a specific order
        
        Args:
            order_id (str): The Amazon order ID
            table_name (str): Name of the table to save data to
            
        Returns:
            dict: Order details from the API
        """
        return self._make_request(f'/orders/v0/orders/{order_id}', table_name=table_name)
    
    def get_order_items(self, order_id, next_token=None, get_all_pages=False, 
                       page_limit=None, table_name='amazon_order_items'):
        """
        Get items for a specific order
        
        Args:
            order_id (str): The Amazon order ID
            next_token (str): Token for pagination
            get_all_pages (bool): Whether to fetch all pages of results
            page_limit (int): Maximum number of pages to fetch (only used if get_all_pages is True)
            table_name (str): Name of the table to save data to
            
        Returns:
            dict or list: Single response or list of responses from all pages
        """
        params = {}
        if next_token:
            params['NextToken'] = next_token
            
        if get_all_pages:
            return self._get_all_pages(f'/orders/v0/orders/{order_id}/orderItems', params, table_name, page_limit)
        else:
            return self._make_request(f'/orders/v0/orders/{order_id}/orderItems', params, table_name)

def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

# Example usage
if __name__ == "__main__":
    client_id = os.getenv("AMAZON_OAUTH_CLIENT_ID")
    client_secret = os.getenv("AMAZON_OAUTH_CLIENT_SECRET")
    refresh_token = os.getenv("AMAZON_OAUTH_REFRESH_TOKEN")
    
    # Initialize the API client with custom output directory and retry settings
    api = AmazonOrdersAPI(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        max_retries=5,  # Maximum number of retry attempts
        max_wait_time=32,  # Maximum wait time in seconds
        db_config={
            'host': 'localhost',
            'port': '5432',
            'database': 'amazon',
            'user': 'user',
            'password': 'password'
        }
    )
    
    try:
        # Example 1: Get recent orders with pagination
        created_after = (datetime.now() - timedelta(days=2)).isoformat()
        marketplace_ids = ["ATVPDKIKX0DER"]  # US marketplace ID
        
        # Get all pages of orders
        all_orders = api.get_orders(
            created_after=created_after,
            marketplace_ids=marketplace_ids,
            get_all_pages=True,  # Enable pagination
            page_limit=5,  # Optional: limit to 5 pages
            table_name='amazon_orders'  # Table name for orders
        )
        
        # Process all orders
        # for page_num, orders_page in enumerate(all_orders, 1):        
        for page_num, orders_page in enumerate(all_orders, 1):
            logger.info(f"\nProcessing page {page_num}")
            if orders_page.get('payload', {}).get('Orders'):                
                for order in orders_page['payload']['Orders']:
                    order_id = order['AmazonOrderId']
                    
                    # Get order details
                    order_details = api.get_order_details(
                        order_id,
                        table_name='amazon_order_details'
                    )
                    
                    # Get order items with pagination
                    order_items = api.get_order_items(
                        order_id,
                        get_all_pages=True,  # Enable pagination                        
                        table_name='amazon_order_items'
                    )
                    
    except Exception as e:
        logger.error(f"Error: {str(e)}") 