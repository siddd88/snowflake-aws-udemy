use role accountadmin;

use schema "ECOMMERCE_DB"."ECOMMERCE_LIV";


create or replace api integration currency_conversion_int
  api_provider = aws_api_gateway
  api_aws_role_arn = ''
  api_allowed_prefixes = ('')
  enabled = true;
  
desc integration currency_conversion_int;

create or replace external function currency_conversion_external_function(from_currency varchar,to_currency varchar)
    returns variant
    api_integration = external_function_integration
    as '';


select currency_conversion_external_function('USD','EUR')

select currency_conversion_external_function('USD','EUR')[0] as exchange_value;