from services.inactivate_forms import inactivate_expired_forms
from services.billing_monthly_summary import generate_monthly_invoice_summary

def run_daily_jobs():
    print("[INFO] Executando jobs diários...")
    inactivate_expired_forms()

def run_monthly_jobs():
    print("[INFO] Executando jobs mensais...")
    generate_monthly_invoice_summary()

