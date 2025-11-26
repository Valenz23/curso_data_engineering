{% test check_points(model, column_name) %}

   select *
   from {{ model }}
   where {{ column_name }} not between -100 and 100

{% endtest %}