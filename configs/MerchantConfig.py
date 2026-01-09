from dagster import Config
class MerchantRowConfig(Config):
    merchant_id : str
    merchant_name: str
    business_type : str
    merchant_state : str