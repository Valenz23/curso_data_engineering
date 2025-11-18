{{ 
    codegen.generate_source(
            schema_name = 'nascar',
            database_name = 'alumno23_dev_bronze_db',
            generate_columns = true,
            include_descriptions = true            
    ) 
}}

{{ 
    codegen.generate_base_model(
        source_name='nascar',
        table_name='nascar_results'
    ) 
}}

{{ 
    codegen.generate_model_yaml(
        model_names=[
            'stg_nascar__car_manufacturer'
        ]
    ) 
}}