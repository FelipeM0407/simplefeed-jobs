from services.inactivate_forms import inactivate_expired_forms

def run_daily_jobs():
    print("[INFO] Executando jobs diários...")
    inactivate_expired_forms()

def run_monthly_jobs():
    print("[INFO] Executando jobs mensais...")
    # Futuramente: importar e executar resumo de cobrança
    pass
