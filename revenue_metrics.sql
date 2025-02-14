WITH month_table AS (
	SELECT
		gp.user_id,
		gp.game_name,
		-- розбивка дати на місяці
		date(date_trunc('month', gp.payment_date)) AS payment_month,
		-- загальний дохід
		sum(gp.revenue_amount_usd) AS total_revenue,
		-- кількість платних користувачів
		count(DISTINCT gp.user_id) AS paid_users
	FROM
		project.games_payments gp
	GROUP BY
		payment_month,
		gp.user_id,
		gp.game_name
	ORDER BY
		payment_month
),
regular_revenue AS (
	SELECT
		*,
		-- попередній місяць
		date(mt.payment_month - INTERVAL '1 month') AS prev_month,
		-- наступний місяць
		date(mt.payment_month + INTERVAL '1 month') AS next_month,
		-- дохід в попередньому місяці
		LAG(mt.total_revenue) OVER (PARTITION BY mt.user_id ORDER BY mt.payment_month) AS prev_total_revenue,
		-- попередній місяць, в якому була оплата
		LAG(mt.payment_month) OVER (PARTITION BY mt.user_id ORDER BY mt.payment_month) AS prev_payment_month,
		-- наступний місяць, в якому була оплата
		LEAD(mt.payment_month) OVER (PARTITION BY mt.user_id ORDER BY mt.payment_month) AS next_payment_month,
		-- платні користувачі за минулий місяць
		LAG(mt.paid_users) OVER(ORDER BY mt.payment_month) AS prev_paid_users
	FROM
		month_table mt
),
first_last_payment_month AS (
	SELECT
		mt.user_id,
		-- перший місяць, в якому була оплата
		min(mt.payment_month) AS first_payment_month,
		-- останній місяць, в якому була оплата
		max(mt.payment_month) AS last_payment_month
	FROM
		month_table mt
	GROUP BY
		mt.user_id
),
total_table AS (
	SELECT
		rr.*,
		gpu."language",
		gpu.age,
		-- щомісячний регулярний дохід
		CASE
			WHEN rr.prev_payment_month IS NULL
				THEN rr.total_revenue
			WHEN rr.payment_month = rr.prev_payment_month + INTERVAL '1 month'
				THEN rr.total_revenue
		END AS mrr,
		-- середній дохід на одного платного користувача
		round(rr.total_revenue / NULLIF(rr.paid_users,
		0),
		4) AS arppu,
		-- кількість нових платних користувачів (помісячно)
		CASE
			WHEN rr.payment_month = flpm.first_payment_month
				THEN 1
		END AS new_paid_users,
		-- щомісячний регулярний дохід новими користувачами
		CASE
			WHEN rr.prev_payment_month IS NULL
				THEN rr.total_revenue
		END AS new_mrr,
		-- кількість користувачів, що перестали платити (помісячно)
		CASE
			WHEN rr.next_payment_month IS NULL OR rr.next_payment_month > rr.next_month
				THEN 1
		END AS churned_users,
		-- відтік доходу на місяць
		CASE
			WHEN rr.next_payment_month IS NULL
			OR rr.next_payment_month > rr.next_month
				THEN rr.total_revenue
		END AS churned_revenue,
		-- приріст MRR
		CASE
			WHEN rr.prev_month = rr.prev_payment_month
			AND rr.total_revenue > rr.prev_total_revenue
				THEN rr.total_revenue - rr.prev_total_revenue
		END AS expansion_mrr,
		-- скорочення MRR
		CASE
			WHEN rr.prev_month = rr.prev_payment_month
			AND rr.total_revenue < rr.prev_total_revenue
				THEN rr.total_revenue - rr.prev_total_revenue
		END AS contraction_mrr,
		-- кількість часу протягом, якого клієнт користувався продуктом
		date_part('month',
		age(flpm.last_payment_month, flpm.first_payment_month)) AS lt,
		-- дохід, що приніс клієнт за час користування продуктом
		sum(rr.total_revenue) OVER (PARTITION BY rr.user_id ORDER BY rr.payment_month) AS ltv
	FROM
		regular_revenue rr
	LEFT JOIN first_last_payment_month flpm ON
		flpm.user_id = rr.user_id
	LEFT JOIN project.games_paid_users gpu ON
		gpu.user_id = rr.user_id
	ORDER BY
		rr.user_id,
		rr.payment_month
)
SELECT
	tt.payment_month,
	tt.user_id,
	tt.game_name,
	tt."language",
	tt.age,
	tt.total_revenue,
	tt.mrr,
	tt.paid_users,
	tt.arppu,
	tt.new_paid_users,
	tt.new_mrr,
	tt.churned_users,
	-- коефіцієнт відтоку користувачів
	round(CASE
		WHEN tt.prev_paid_users > 0
			THEN (tt.churned_users :: NUMERIC / tt.prev_paid_users)
	END, 4) AS churn_rate,
	tt.churned_revenue,
	-- коефіцієнт відтоку доходів
	round(CASE
		WHEN tt.prev_paid_users > 0
			THEN tt.churned_revenue / NULLIF((
				SELECT sum(total_revenue)
				FROM regular_revenue
				WHERE payment_month = tt.prev_month), 0)
	END, 4) AS revenue_churn_rate,
	tt.expansion_mrr,
	tt.contraction_mrr,
	tt.lt,
	tt.ltv
FROM
	total_table tt