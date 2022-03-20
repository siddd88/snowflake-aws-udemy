CREATE OR REPLACE PROCEDURE sp_load_files()
RETURNS string
LANGUAGE javascript
EXECUTE AS caller
AS $$
var v_step=0;
var output_array = [];  // array
    
// helper funtion for logging
 function log ( msg ) { 
  output_array.push(msg)
  }     
try{

     v_step=10; // truncate table before loading
     
     snowflake.createStatement( {sqlText: "truncate table dept"}).execute(); 
     
     v_step=20; // get current date
     
     var rs_file_load_date = snowflake.createStatement(
                   {sqlText: "select to_char(current_date,'YYYYMMDD')"}).execute();
                   
     rs_file_load_date.next(); 
     
     var lv_file_load_date  =  rs_file_load_date.getColumnValue(1);
     
     log('lv_file_load_date: ' +  lv_file_load_date)  // just for logging 
     
     v_step=30; // copy comamnd
     
     var sql_copy_stmt = `copy into dept from @mystage/dept_${lv_file_load_date}.csv
                          file_format = (format_name = myformat)
                         `;                       
     log('sql_copy_stmt: ' +  sql_copy_stmt)  //  just logging
     
     snowflake.createStatement( {sqlText: sql_copy_stmt}).execute();          
       
     return output_array;
    // return "SUCCESS";

}catch(err){         
          ret = "Failed at Step " + v_step 
          ret += "\n Failed: Code: " + err.code + "\n  State: " + err.state;
          ret += "\n  Message: " + err.message;
          ret += "\nStack Trace:\n" + err.stackTraceTxt;             
    return ret;  

}

$$;