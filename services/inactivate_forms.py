from db import get_connection
from datetime import date, datetime
import json

def inactivate_expired_forms():
    today = date.today()

    select_query = """
    SELECT f.id, f.client_id, f.name
    FROM forms f
    JOIN form_settings fs ON f.id = fs.form_id
    WHERE f.is_active = TRUE
      AND fs.inativation_date IS NOT NULL
      AND DATE(fs.inativation_date) <= %s;
    """

    update_query = """
    UPDATE forms f
    SET is_active = FALSE, updated_at = NOW()
    FROM form_settings fs
    WHERE f.id = fs.form_id
      AND f.is_active = TRUE
      AND fs.inativation_date IS NOT NULL
      AND DATE(fs.inativation_date) <= %s;
    """

    insert_log_query = """
    INSERT INTO client_action_logs (client_id, action_id, form_id, timestamp, details)
    VALUES (%s, 10, %s, NOW(), %s);
    """

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Buscar formulários a serem inativados
                cur.execute(select_query, (today,))
                forms_to_inactivate = cur.fetchall()

                # Inativar os formulários
                cur.execute(update_query, (today,))
                print(f"[{today}] Formulários inativados: {cur.rowcount}")

                # Inserir logs
                for form_id, client_id, form_name in forms_to_inactivate:
                    details = {
                        "form_id": form_id,
                        "form_name": form_name,
                        "reason": "Inativação agendada",
                        "inactivated_at": datetime.utcnow().isoformat() + "Z"
                    }
                    cur.execute(insert_log_query, (
                        client_id,
                        form_id,
                        json.dumps(details, ensure_ascii=False)
                    ))

                conn.commit()
    except Exception as e:
        print(f"[ERRO] Falha ao inativar formulários: {e}")
