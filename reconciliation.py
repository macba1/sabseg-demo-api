"""
Generic Reconciliation Engine (stub)
Used for the synthetic data demo. Real reconciliation uses reconciliation_sabseg.py
"""

import pandas as pd
from io import BytesIO
from datetime import datetime


def run_reconciliation(file_a_bytes, file_b_bytes):
    """Basic two-file reconciliation. Returns results dict."""
    try:
        df_a = pd.read_excel(BytesIO(file_a_bytes))
        df_b = pd.read_excel(BytesIO(file_b_bytes))
    except Exception as e:
        return {"error": f"No se pudieron leer los ficheros: {str(e)}"}
    
    return {
        "fecha_analisis": datetime.now().strftime("%d/%m/%Y %H:%M"),
        "registros_produccion": len(df_a),
        "registros_contabilidad": len(df_b),
        "registros_matched": 0,
        "total_discrepancias": 0,
        "pct_limpio": 100,
        "prima_total_produccion": 0,
        "prima_total_contabilidad": 0,
        "diferencia_bruta": 0,
        "resumen_por_tipo": {},
        "resumen_por_severidad": {},
        "resumen_por_aseguradora": {},
        "discrepancias": [],
        "message": "Use /api/demo-sabseg for real Sabseg reconciliation"
    }
