{% test check_car_num(model, column_name) %}

   select *
   from {{ model }}
   where {{ column_name }} not between 0 and 99

{% endtest %}