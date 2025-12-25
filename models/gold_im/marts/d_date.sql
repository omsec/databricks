{{
    config(
        materialized='incremental',
        unique_key='d_date'
    )
}}

-- https://community.databricks.com/t5/community-articles/build-amp-refresh-a-calendar-dates-table/td-p/90809
with calendarDates as (
  select
    explode(array_dates) as calendar_date
  from
    (
      select
        sequence(
          make_date(2020, 01, 01),
          make_date(2050, 01, 01),
          interval 1 day
        ) as array_dates
    )
)
select
  10000 * year(calendar_date) + 100 * month(calendar_date) + day(calendar_date) as d_date,
  to_date(calendar_date) as calendar_date,
  year(calendar_date) calendar_year,
  month(calendar_date) calendar_month,
  day(calendar_date) calendar_day_of_month,
  date_format(calendar_date, 'EEEE MMMM dd yyyy') calendar_date_full,
  date_format(calendar_date, 'EEEE') calendar_day_name,
  date_add(to_date(calendar_date), -1) as calendar_date_previous_day,
  date_add(to_date(calendar_date), 1) as calendar_date_next_day,
  case
    when date_add(calendar_date, (weekday(calendar_date) + 1) -1) = calendar_date then to_date(calendar_date)
    else date_add(calendar_date, -(weekday(calendar_date)))
  end as calendar_week_start,
  date_add(
    case
      when date_add(calendar_date, (weekday(calendar_date) + 1) -1) = calendar_date then to_date(calendar_date)
      else date_add(calendar_date, -(weekday(calendar_date)))
    end,
    6
  ) as calendar_week_end,
  weekday(calendar_date) + 1 as calendar_week_day,
  weekofyear(calendar_date) calendar_week_of_year,
  date_format(calendar_date, 'MMMM yyyy') calendar_month_year,
  date_format(calendar_date, 'MMMM') calendar_month_name,
  date_add(last_day(add_months(calendar_date, -1)), 1) calendar_first_day_of_month,
  last_day(calendar_date) calendar_last_day_of_month,
  case
    when month(calendar_date) in (1, 2, 3) then 1
    when month(calendar_date) in (4, 5, 6) then 2
    when month(calendar_date) in (7, 8, 9) then 3
    else 4
  end AS fiscal_quarter,
  year(date_add(calendar_date, 89)) AS fiscal_year,
  case
    when to_date(now()) = calendar_date then true
    else false
  end as current_day,
  CASE
    WHEN to_date(now()) BETWEEN (
      case
        when date_add(calendar_date, (weekday(calendar_date) + 1) -1) = calendar_date then to_date(calendar_date)
        else date_add(calendar_date, -(weekday(calendar_date)))
      end
    )
    AND (
      date_add(
        case
          when date_add(calendar_date, (weekday(calendar_date) + 1) -1) = calendar_date then to_date(calendar_date)
          else date_add(calendar_date, -(weekday(calendar_date)))
        end,
        6
      )
    )
    THEN true
    else false
  end as current_week,
  case
    when month(to_date(now())) = month(calendar_date)
    and year(to_date(now())) = year(calendar_date) then true
    else false
  end as current_month,
  case
    when year(to_date(now())) = year(calendar_date) then true
    else false
  end as current_year
from
  calendarDates