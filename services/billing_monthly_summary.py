from db import get_connection
from datetime import date, datetime


def generate_monthly_invoice_summary():
    today = date.today()
    if today.month == 1:
        reference_date = date(today.year - 1, 12, 1)
    else:
        reference_date = date(today.year, today.month - 1, 1)
        
    query = """
        WITH
        forms_cobranca AS (
            SELECT f.id, f.client_id
            FROM forms f
            LEFT JOIN form_settings fs ON fs.form_id = f.id
            WHERE (
                f.is_active = TRUE
                OR (f.is_active = FALSE AND f.updated_at >= DATE_TRUNC('month', %s))
            )
            AND f.created_at < DATE_TRUNC('month', %s + INTERVAL '1 month')
            AND (
                fs.inativation_date IS NULL
                OR fs.inativation_date::date != DATE_TRUNC('month', %s)::date
            )
        ),

        respostas_ate_mes AS (
            SELECT client_id, COUNT(*) AS total_responses
            FROM feedbacks
            WHERE submitted_at < DATE_TRUNC('month', %s + INTERVAL '1 month')
            GROUP BY client_id
        ),

        ia_reports_mes AS (
            SELECT client_id, COUNT(*) AS total_ai_reports
            FROM ai_reports
            WHERE created_at >= DATE_TRUNC('month', %s)
              AND created_at < DATE_TRUNC('month', %s + INTERVAL '1 month')
            GROUP BY client_id
        )

       SELECT
        c."Id" AS client_id,
        p.id AS plan_id,
        DATE_TRUNC('month', %s) AS reference_month,

        -- Formulários
        COUNT(DISTINCT fc.id) AS total_forms_mes,
        LEAST(COUNT(DISTINCT fc.id), COALESCE(p.max_forms, 0)) AS forms_dentro_plano,
        GREATEST(COUNT(DISTINCT fc.id) - COALESCE(p.max_forms, 0), 0) AS forms_excedentes,

        -- Respostas
        COALESCE(rm.total_responses, 0) AS total_respostas_armazenadas,
        LEAST(COALESCE(rm.total_responses, 0), COALESCE(p.max_responses, 0)) AS respostas_dentro_plano,
        GREATEST(COALESCE(rm.total_responses, 0) - COALESCE(p.max_responses, 0), 0) AS respostas_excedentes,

        -- IA
        COALESCE(ia.total_ai_reports, 0) AS total_ai_reports,
        COUNT(DISTINCT fc.id) * COALESCE(p.ai_reports_per_form, 0) AS ai_reports_limite,
        GREATEST(
            COALESCE(ia.total_ai_reports, 0) - (COUNT(DISTINCT fc.id) * COALESCE(p.ai_reports_per_form, 0)),
            0
        ) AS extra_ai_reports,

        -- Cobranças
        GREATEST(COUNT(DISTINCT fc.id) - COALESCE(p.max_forms, 0), 0) * COALESCE(pr_form.price, 0) AS form_excess_charge,
        GREATEST(COALESCE(rm.total_responses, 0) - COALESCE(p.max_responses, 0), 0) /
            COALESCE(NULLIF(pr_resp.unit_size, 0), 100) * COALESCE(pr_resp.price, 0) AS response_excess_charge,
        GREATEST(
            COALESCE(ia.total_ai_reports, 0) - (COUNT(DISTINCT fc.id) * COALESCE(p.ai_reports_per_form, 0)),
            0
        ) * COALESCE(pr_ai.price, 0) AS ai_report_excess_charge,

        -- Valor total e base
        CASE
            WHEN p.plan_type = 'usage_based' THEN (
                GREATEST(COUNT(DISTINCT fc.id) - COALESCE(p.max_forms, 0), 0) * COALESCE(pr_form.price, 0) +
                GREATEST(COALESCE(rm.total_responses, 0) - COALESCE(p.max_responses, 0), 0) /
                    COALESCE(NULLIF(pr_resp.unit_size, 0), 100) * COALESCE(pr_resp.price, 0) +
                GREATEST(
                    COALESCE(ia.total_ai_reports, 0) - (COUNT(DISTINCT fc.id) * COALESCE(p.ai_reports_per_form, 0)),
                    0
                ) * COALESCE(pr_ai.price, 0)
            )
            ELSE (
                p.base_price +
                GREATEST(COUNT(DISTINCT fc.id) - COALESCE(p.max_forms, 0), 0) * COALESCE(pr_form.price, 0) +
                GREATEST(COALESCE(rm.total_responses, 0) - COALESCE(p.max_responses, 0), 0) /
                    COALESCE(NULLIF(pr_resp.unit_size, 0), 100) * COALESCE(pr_resp.price, 0) +
                GREATEST(
                    COALESCE(ia.total_ai_reports, 0) - (COUNT(DISTINCT fc.id) * COALESCE(p.ai_reports_per_form, 0)),
                    0
                ) * COALESCE(pr_ai.price, 0)
            )
        END AS valor_fatura_ate_agora,
        
        p.base_price as valor_base_fatura,
        p.name as nome_plano


        FROM clients c
        JOIN plans p ON p.id = c."PlanId"
        LEFT JOIN pricing_rules pr_form ON pr_form.plan_id = p.id AND pr_form.item = 'form'
        LEFT JOIN pricing_rules pr_resp ON pr_resp.plan_id = p.id AND pr_resp.item = 'response_pack'
        LEFT JOIN pricing_rules pr_ai ON pr_ai.plan_id = p.id AND pr_ai.item = 'ai_report'
        LEFT JOIN forms_cobranca fc ON fc.client_id = c."Id"
        LEFT JOIN respostas_ate_mes rm ON rm.client_id = c."Id"
        LEFT JOIN ia_reports_mes ia ON ia.client_id = c."Id"

        GROUP BY
            c."Id", p.id, p.plan_type, p.base_price, p.max_forms, p.max_responses, p.ai_reports_per_form,
            rm.total_responses, ia.total_ai_reports,
            pr_form.price, pr_resp.price, pr_resp.unit_size, pr_ai.price;
    """

    insert_query = """
    INSERT INTO billing_summary (
        client_id,
        plan_id,
        plan_name,
        reference_month,
        total_forms,
        included_forms,
        extra_forms,
        total_responses,
        included_responses,
        extra_responses,
        total_ai_reports,
        included_ai_reports,
        extra_ai_reports,
        amount_forms,
        amount_responses,
        amount_ai_reports,
        base_price
    )
    VALUES (
        %(client_id)s,
        %(plan_id)s,
        %(plan_name)s,
        %(reference_month)s,
        %(total_forms_mes)s,
        %(forms_dentro_plano)s,
        %(forms_excedentes)s,
        %(total_respostas_armazenadas)s,
        %(respostas_dentro_plano)s,
        %(respostas_excedentes)s,
        %(total_ai_reports)s,
        %(ai_reports_limite)s,
        %(extra_ai_reports)s,
        %(form_excess_charge)s,
        %(response_excess_charge)s,
        %(ai_report_excess_charge)s,
        %(valor_base_fatura)s
    )
    """


    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, [reference_date]*7)
                rows = cur.fetchall()

                for row in rows:
                     cur.execute(insert_query, {
                        'client_id': row[0],
                        'plan_id': row[1],
                        'plan_name': row[17],
                        'reference_month': row[2],
                        'total_forms_mes': row[3],
                        'forms_dentro_plano': row[4],
                        'forms_excedentes': row[5],
                        'total_respostas_armazenadas': row[6],
                        'respostas_dentro_plano': row[7],
                        'respostas_excedentes': row[8],
                        'total_ai_reports': row[9],
                        'ai_reports_limite': row[10],
                        'extra_ai_reports': row[11],
                        'form_excess_charge': row[12],
                        'response_excess_charge': row[13],
                        'ai_report_excess_charge': row[14],
                        'valor_base_fatura': row[16]
                    })

                conn.commit()
                print(f"Resumo de cobrança gerado para {len(rows)} cliente(s) - {reference_date.strftime('%Y-%m')}")
    except Exception as e:
        print(f"[ERRO] Falha ao gerar resumo de cobrança: {e}")
