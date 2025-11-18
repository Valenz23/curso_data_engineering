{{ 
    codegen.generate_source(
            schema_name = 'nascar',
            database_name = 'alumno23_dev_bronze_db',
            generate_columns = true,
            include_descriptions = true            
    ) 
}}