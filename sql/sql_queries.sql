----- SQL Query to fetch all the duplicate records in a table


Select user_id, user_name, email
from (select *,
row_number() over (partition by user_name order by user_id) as rn
from users u
order by user_id) x
where x.rn != 1; 


----- SQL query to fetch the second last record from employee table

Select emp_id, emp_name, dept_name, salary
from (select *,
row_number() over (order by emp_id desc) as rn
from employee e) x
where x.rn = 2;


----- SQL query to display only the details of employees who either earn the highest salary or the lowest salary in each department from the employee table

Select x.*
from employee e
join (select *,
max(salary) over (partition by dept_name) as max_salary,
min(salary) over (partition by dept_name) as min_salary
from employee) x
on e.emp_id = x.emp_id
and (e.salary = x.max_salary or e.salary = x.min_salary)
order by x.dept_name, x.salary;

------ From the login_details table, fetch the users who logged in consecutively 3 or more times

Select distinct repeated_names
from (select *,
case when user_name = lead(user_name) over(order by login_id)
and  user_name = lead(user_name,2) over(order by login_id)
then user_name else null end as repeated_names
from login_details) x
where x.repeated_names is not null; 


----- Time difference filter

datediff(now(), cast(unix_timestamp(`dt_prem_inscription`, "yyyyMMdd") as timestamp)) < 2

----- Filter a table with a query

SELECT A.*
  FROM `perfsegmentation_base_aggrega_to_crm` A
  INNER JOIN (SELECT max(`id_mois`) as id_max FROM `perfsegmentation_base_aggrega`) B 
  ON A.`id_mois`=B.id_max
  
----- Query that returns the top 3 spenders(users) by country
  
  SELECT *
FROM (SELECT  *, rank() over (partition by country order by total desc) as rk
			FROM (SELECT country, user_id, sum(total_amount_spent) as total
				FROM userdatabase
				WHERE install_source='ua'
				GROUP BY user_id, country ) a
		) b

WHERE rk <=3
ORDER BY country, rk

----- Query that gives the daily average revenue per game, with daily average revenue = total_amount_spent / total unique players

SELECT install_date, game, (total_amount_spent/total_unique_players) as daily_average_revenue`
FROM (SELECT install_date, game, COUNT(DISTINCT user_id )as total_unique_players, SUM(total_amount_spent) as total_amount_spent`
	FROM gamers
	GROUP BY install_date, game
			) a
      
      




