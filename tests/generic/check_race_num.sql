{% test check_race_num(model, column_name) %}

   select *
   from {{ model }}
   where {{ column_name }} not between -2 and 36

{% endtest %}