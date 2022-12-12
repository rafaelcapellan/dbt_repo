/* Revisar si la configuración del model se adecúa a las best practices definidas en Mutua*/
/* Revisar si el cálculo de ños fiscales coincicide con lo que Mutua considera un año fiscal*/

-- Documentación:
	-- https://hub.getdbt.com/dbt-labs/dbt_utils/latest/
	-- https://github.com/dbt-labs/dbt-utils#date_spine-source
-- En este caso genera los datos de las fechas del período comprendido entre el 01/01/2010 y la fecha acual mas 10 años


{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'id_date'
    )
}}


with date_spine as (

  {{ dbt_utils.date_spine(
      start_date="to_date('01/01/2000', 'mm/dd/yyyy')",
      datepart="day",
      end_date="to_date('12/31/2100', 'mm/dd/yyyy')"
     )
  }}

)



select 
  to_char(date_day, 'yyyyMMdd') ::integer                                                 as id_fecha,
  date_day                                                                                as fe_dia,
  right(to_char(date_day, 'yyyyMMdd'),2) ::integer                                        as nu_dia,
  case
    when dayname(date_day)='Mon' then 'Lunes'  
    when dayname(date_day)='Tue' then 'Martes'
    when dayname(date_day)='Wed' then 'Miércoles'
    when dayname(date_day)='Thu' then 'Jueves'
    when dayname(date_day)='Fri' then 'Viernes'
    when dayname(date_day)='Sat' then 'Sábado'
    when dayname(date_day)='Sun' then 'Domingo'
  end                                                                                     as no_dia_semana,
  case
    when dayname(date_day)='Mon' then 'Lun'  
    when dayname(date_day)='Tue' then 'Mar'
    when dayname(date_day)='Wed' then 'Mie'
    when dayname(date_day)='Thu' then 'Jue'
    when dayname(date_day)='Fri' then 'Vie'
    when dayname(date_day)='Sat' then 'Sab'
    when dayname(date_day)='Sun' then 'Dom'
  end                                                                                     as no_dia_semana_abrv,
  case
    when date_part(dayofweek, date_day)=0 then 7
    else date_part(dayofweek, date_day)
  end                                                                                     as nu_dia_semana,
  date_part('day', date_day)                                                              as nu_dia_mes,
  row_number() over (partition by date_part('year', date_day) order by date_day)          as nu_dia_anyo,
  weekofyear(date_day)                                                                    as nu_semana, 
  date_part('month', date_day)                                                            as nu_mes,
  case
    when date_part('month', date_day)=1 then 'Enero'
    when date_part('month', date_day)=2 then 'Febrero'
    when date_part('month', date_day)=3 then 'Marzo'
    when date_part('month', date_day)=4 then 'Abril'
    when date_part('month', date_day)=5 then 'Mayo'
    when date_part('month', date_day)=6 then 'Junio'
    when date_part('month', date_day)=7 then 'Julio'
    when date_part('month', date_day)=8 then 'Agosto'
    when date_part('month', date_day)=9 then 'Septiembre'
    when date_part('month', date_day)=10 then 'Octubre'
    when date_part('month', date_day)=11 then 'Noviembre'
    when date_part('month', date_day)=12 then 'Diciembre'
  end                                                                                     as no_mes,
  case
    when date_part('month', date_day)=1 then 'Ene'
    when date_part('month', date_day)=2 then 'Feb'
    when date_part('month', date_day)=3 then 'Mar'
    when date_part('month', date_day)=4 then 'Abr'
    when date_part('month', date_day)=5 then 'May'
    when date_part('month', date_day)=6 then 'Jun'
    when date_part('month', date_day)=7 then 'Jul'
    when date_part('month', date_day)=8 then 'Ago'
    when date_part('month', date_day)=9 then 'Sep'
    when date_part('month', date_day)=10 then 'Oct'
    when date_part('month', date_day)=11 then 'Nov'
    when date_part('month', date_day)=12 then 'Dic'
  end                                                                                     as no_mes_abrv,
  date_part(quarter, date_day)                                                            as nu_trimestre,
  date_part('year', date_day)                                                             as nu_anyo,
  left(to_char(date_day, 'yyyyMMdd'),6) ::integer                                         as nu_anyo_mes,
  case
     when date_part(dayofweek, date_day)=0 or date_part(dayofweek, date_day)=6 then true
     else false
  end                                                                                     as sw_fin_semana

  /* dateadd('day', -(date_part(dayofweek, date_day)), date_day)                             as first_day_of_week,
  row_number() over (partition by year_actual, quarter_actual order by date_day)          as day_of_quarter,
  row_number() over (partition by year_actual order by date_day)                          as day_of_year,
  case when month_actual < 2
    then year_actual
    else (year_actual+1) end                                                              as fiscal_year,
  case when month_actual < 2 then '4'
    when month_actual < 5 then '1'
    when month_actual < 8 then '2'
    when month_actual < 11 then '3'
    else '4' end                                                                          as fiscal_quarter,
  row_number() over (partition by fiscal_year, fiscal_quarter order by date_day)          as day_of_fiscal_quarter,
  row_number() over (partition by fiscal_year order by date_day)                          as day_of_fiscal_year,
  to_char(date_day, 'mmmm')                                                               as month_name,
  trunc(date_day, 'month')                                                                as first_day_of_month,
  last_value(date_day) over (partition by year_actual, month_actual order by date_day)    as last_day_of_month,
  first_value(date_day) over (partition by year_actual order by date_day)                 as first_day_of_year,
  last_value(date_day) over (partition by year_actual order by date_day)                  as last_day_of_year,
  first_value(date_day) over (partition by year_actual, quarter_actual order by date_day) as first_day_of_quarter,
  last_value(date_day) over (partition by year_actual, quarter_actual order by date_day)  as last_day_of_quarter,
  first_value(date_day) over (partition by fiscal_year, fiscal_quarter order by date_day) as first_day_of_fiscal_quarter,
  last_value(date_day) over (partition by fiscal_year, fiscal_quarter order by date_day)  as last_day_of_fiscal_quarter,
  first_value(date_day) over (partition by fiscal_year order by date_day)                 as first_day_of_fiscal_year,
  last_value(date_day) over (partition by fiscal_year order by date_day)                  as last_day_of_fiscal_year,
  datediff('week', first_day_of_fiscal_year, date_day) +1                              as week_of_fiscal_year,
  case when extract('month', date_day) = 1 then 12
    else extract('month', date_day) - 1 end                                               as month_of_fiscal_year,
  last_value(date_day) over (partition by first_day_of_week order by date_day)            as last_day_of_week,
  (year_actual || '-q' || extract(quarter from date_day))                                 as quarter_name,
  (fiscal_year || '-' || decode(fiscal_quarter,
    1, 'q1',
    2, 'q2',
    3, 'q3',
    4, 'q4'))                                                                             as fiscal_quarter_name,  
  ('fy' || substr(fiscal_quarter_name, 3, 7))                                             as fiscal_quarter_name_fy,
  dense_rank() over (order by fiscal_quarter_name)                                        as fiscal_quarter_number_absolute,
  fiscal_year || '-' || monthname(date_day)                                               as fiscal_month_name,
  ('fy' || substr(fiscal_month_name, 3, 8))                                               as fiscal_month_name_fy,
  date_trunc('month', last_day_of_fiscal_quarter)                                         as last_month_of_fiscal_quarter,
  iff(date_trunc('month', last_day_of_fiscal_quarter) = date_day, true, false)         as is_first_day_of_last_month_of_fiscal_quarter,
  date_trunc('month', last_day_of_fiscal_year)                                            as last_month_of_fiscal_year,
  iff(date_trunc('month', last_day_of_fiscal_year) = date_day, true, false)            as is_first_day_of_last_month_of_fiscal_year*/

from date_spine