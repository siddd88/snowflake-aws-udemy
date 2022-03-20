use role sysadmin;

use schema "ECOMMERCE_DB"."ECOMMERCE_LIV";

CREATE OR REPLACE FUNCTION test_date_js(A string)
RETURNS OBJECT
LANGUAGE JAVASCRIPT
STRICT
AS
$$
function test_js_func(x)
{
  var now = new Date(x);
  return now 
}
return test_js_func(A); 
$$;
  
select test_date_js('2022-02-01');


CREATE OR REPLACE FUNCTION simple_js_func(A float)
  RETURNS OBJECT
  LANGUAGE JAVASCRIPT
  STRICT
AS
$$
function test_function(x){  
  var lr = {};
  lr['a'] = x*2;
  lr['b'] = x*3;
  return lr;
  }
  return test_function(A); 
$$;

SELECT simple_js_func(2::float);

CREATE OR REPLACE FUNCTION range_to_values(PREFIX VARCHAR, RANGE_START FLOAT, RANGE_END FLOAT)
    RETURNS TABLE (IP_ADDRESS VARCHAR)
    LANGUAGE JAVASCRIPT
    AS $$
      {
        processRow: function f(row, rowWriter, context)  {
          var suffix = row.RANGE_START;
          while (suffix <= row.RANGE_END)  {
            rowWriter.writeRow( {IP_ADDRESS: row.PREFIX + "." + suffix} );
            suffix = suffix + 1;
            }
          }
      }
      $$;
SELECT * FROM TABLE(range_to_values('192.168.1', 42::FLOAT, 45::FLOAT));