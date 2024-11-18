WITH combined_data AS (
    -- Збираємо основні дані: користувач, гра, платіжний місяць та суму доходу
    SELECT 
        pu.user_id,
        pu.game_name,
        DATE_TRUNC('month', p.payment_date) AS payment_month,
        SUM(p.revenue_amount_usd) AS total_revenue
    FROM 
        project.games_paid_users pu
    JOIN 
        project.games_payments p
    ON 
        pu.user_id = p.user_id AND pu.game_name = p.game_name
    GROUP BY 
        pu.user_id, pu.game_name, DATE_TRUNC('month', p.payment_date)
),
revenue_lag_lead_months AS (
    -- Отримуємо лагові і лідові значення для метрик типу churn, expansion та contraction
    SELECT 
        user_id,
        game_name,
        payment_month,
        total_revenue,
        LAG(payment_month) OVER (PARTITION BY user_id ORDER BY payment_month) AS previous_paid_month,
        LEAD(payment_month) OVER (PARTITION BY user_id ORDER BY payment_month) AS next_paid_month,
        LAG(total_revenue) OVER (PARTITION BY user_id ORDER BY payment_month) AS previous_paid_month_revenue
    FROM 
        combined_data
),
revenue_metrics AS (
    -- Обчислюємо метрики на основі сценаріїв для кожного місяця і користувача
    SELECT 
        payment_month,
        user_id,
        game_name,
        total_revenue AS revenue_amount,
        'revenue' AS revenue_type
    FROM revenue_lag_lead_months

    UNION ALL

    SELECT 
        payment_month,
        user_id,
        game_name,
        total_revenue AS revenue_amount,
        'new_mrr' AS revenue_type
    FROM revenue_lag_lead_months
    WHERE previous_paid_month IS NULL  -- новий MRR

    UNION ALL

    SELECT 
        payment_month + INTERVAL '1 month' AS payment_month,
        user_id,
        game_name,
        -total_revenue AS revenue_amount,
        'churn' AS revenue_type
    FROM revenue_lag_lead_months
    WHERE next_paid_month IS NULL OR next_paid_month != payment_month + INTERVAL '1 month'  -- Churn

    UNION ALL

    SELECT 
        payment_month,
        user_id,
        game_name,
        total_revenue AS revenue_amount,
        'back_from_churn_revenue' AS revenue_type
    FROM revenue_lag_lead_months
    WHERE previous_paid_month IS NOT NULL AND previous_paid_month != payment_month - INTERVAL '1 month'  -- повернення після відтоку

    UNION ALL

    SELECT 
        payment_month,
        user_id,
        game_name,
        -total_revenue AS revenue_amount,
        'contraction_revenue' AS revenue_type
    FROM revenue_lag_lead_months
    WHERE previous_paid_month = payment_month - INTERVAL '1 month' 
        AND total_revenue < previous_paid_month_revenue  -- зменшення доходу

    UNION ALL

    SELECT 
        payment_month,
        user_id,
        game_name,
        total_revenue - previous_paid_month_revenue AS revenue_amount,
        'expansion_revenue' AS revenue_type
    FROM revenue_lag_lead_months
    WHERE previous_paid_month = payment_month - INTERVAL '1 month'
        AND total_revenue > previous_paid_month_revenue  -- збільшення доходу
),
churned_revenue_calc AS (
    -- Розраховуємо Churned Revenue для користувачів, що відтікли (від'ємне значення)
    SELECT 
        user_id,
        game_name,
        payment_month,
        CASE
            WHEN next_paid_month IS NULL
            OR next_paid_month != payment_month + INTERVAL '1 month'
                THEN -total_revenue  -- додаємо мінус для від'ємного значення
            ELSE 0
        END AS churned_revenue
    FROM 
        revenue_lag_lead_months
)
-- Фінальний вибір даних на рівні кожного користувача за кожен місяць
SELECT 
    rm.payment_month,
    TO_CHAR(rm.payment_month, 'Month YYYY') AS formatted_payment_month,  -- відформатоване поле
    rm.user_id,
    rm.game_name,
    rm.revenue_amount,
    rm.revenue_type,
    cr.churned_revenue,
    pu.language,
    pu.has_older_device_model,
    pu.age
FROM 
    revenue_metrics rm
LEFT JOIN 
    churned_revenue_calc cr ON rm.user_id = cr.user_id AND rm.payment_month = cr.payment_month
LEFT JOIN 
    project.games_paid_users pu ON rm.user_id = pu.user_id
ORDER BY 
    rm.payment_month, rm.user_id, rm.game_name, rm.revenue_type;
