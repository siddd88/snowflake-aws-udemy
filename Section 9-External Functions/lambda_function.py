import json
import urllib3

def lambda_handler(event, context):
    
    status_code = 200
    array_of_rows_to_return = [ ]
    http = urllib3.PoolManager()

    try:
        event_body = event["body"]
        payload = json.loads(event_body)
        rows = payload["data"]

        for row in rows:

            row_number = row[0]
            from_currency = row[1]
            to_currency = row[2]
            
            response = http.request('GET','https://open.er-api.com/v6/latest/'+from_currency)
            response_data = response.data.decode('utf8').replace("'", '"')
            data = json.loads(response_data)
    
            exchange_rate_value = data['rates'][to_currency]

            output_value = [exchange_rate_value]
            row_to_return = [row_number, output_value]
            array_of_rows_to_return.append(row_to_return)

        json_compatible_string_to_return = json.dumps({"data" : array_of_rows_to_return})

    except Exception as err:
        status_code = 400
        json_compatible_string_to_return = event_body

    return {
        'statusCode': status_code,
        'body': json_compatible_string_to_return
    }

 # Example Request 
# {
#     "data":
#         [
#             [0,"USD", "INR"]
#         ]
# }



