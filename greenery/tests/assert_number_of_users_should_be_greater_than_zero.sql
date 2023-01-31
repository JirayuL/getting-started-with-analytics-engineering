select value from {{
    ref('user_count_model')
}}
where value <= 0