{{
    config (
        materialized='incremental', 
        incremental_strategy='delete+insert',
        unique_key='PRODUCT_ID'
    )
}}

WITH PRODUCT_SRC AS
(
    SELECT PRODUCT_ID, PRODUCT_NAME, PRODUCT_PRICE, CREATED_AT, CURRENT_TIMESTAMP AS INSERT_DTS
    FROM {{source('product','PRODUCT_SRC')}}

    {% if is_incremental() %}
    WHERE CREATED_AT > ( SELECT MAX(INSERT_DTS) FROM {{this}} )
    {% endif %}
)

SELECT * FROM PRODUCT_SRC