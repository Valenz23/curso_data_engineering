{{ 
    
codegen.generate_base_model(
    source_name='codegen_sql_server_dbo',
    table_name='addresses',
    materialized='view'
) 

}}