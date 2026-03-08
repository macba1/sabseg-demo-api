"""
Motor de Reconciliación Contable
================================
Cruza Fichero A (Producción/Venta) vs Fichero B (Contabilidad)
Detecta, clasifica y explica discrepancias.
"""

import pandas as pd
from datetime import datetime
from io import BytesIO


def run_reconciliation(file_a_bytes: bytes, file_b_bytes: bytes) -> dict:
    """
    Receives two Excel files as bytes, runs reconciliation, returns results dict.
    """
    df_a = pd.read_excel(BytesIO(file_a_bytes))
    df_b = pd.read_excel(BytesIO(file_b_bytes))

    # ── Auto-detect column mapping ────────────────────────────────────────
    # Try to find receipt/policy columns in both files
    col_map_a = detect_columns(df_a, "A")
    col_map_b = detect_columns(df_b, "B")

    receipt_col_a = col_map_a.get("recibo")
    receipt_col_b = col_map_b.get("recibo")
    policy_col_a = col_map_a.get("poliza")
    policy_col_b = col_map_b.get("poliza")
    prima_col_a = col_map_a.get("prima_neta")
    prima_col_b = col_map_b.get("prima_neta")
    total_col_a = col_map_a.get("prima_total")
    total_col_b = col_map_b.get("prima_total")
    comision_pct_a = col_map_a.get("comision_pct")
    comision_pct_b = col_map_b.get("comision_pct")
    comision_eur_a = col_map_a.get("comision_eur")
    comision_eur_b = col_map_b.get("comision_eur")
    fecha_col_a = col_map_a.get("fecha")
    fecha_col_b = col_map_b.get("fecha")
    aseg_col_a = col_map_a.get("aseguradora")
    aseg_col_b = col_map_b.get("aseguradora")
    ramo_col_a = col_map_a.get("ramo")
    ramo_col_b = col_map_b.get("ramo")
    cliente_col_a = col_map_a.get("cliente")
    cliente_col_b = col_map_b.get("cliente")

    if not receipt_col_a or not receipt_col_b:
        return {"error": "No se han podido detectar las columnas de recibo en los ficheros."}

    # ── Matching ──────────────────────────────────────────────────────────
    receipts_a = set(df_a[receipt_col_a].dropna().astype(str))
    receipts_b = set(df_b[receipt_col_b].dropna().astype(str))

    matched = receipts_a & receipts_b
    only_in_a = receipts_a - receipts_b
    only_in_b = receipts_b - receipts_a

    # Index by receipt
    a_indexed = df_a.set_index(df_a[receipt_col_a].astype(str))
    b_indexed = df_b.set_index(df_b[receipt_col_b].astype(str))

    # Detect duplicates in B
    b_counts = df_b[receipt_col_b].astype(str).value_counts()
    duplicates_b = set(b_counts[b_counts > 1].index)

    discrepancies = []
    disc_id = 0

    # ── Type 1: Missing in B ─────────────────────────────────────────────
    for receipt in only_in_a:
        disc_id += 1
        row = a_indexed.loc[receipt]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        imp = safe_float(row, total_col_a) or safe_float(row, prima_col_a) or 0
        discrepancies.append({
            "id": disc_id,
            "tipo": "Registro faltante en contabilidad",
            "severidad": "Alta",
            "poliza": safe_str(row, policy_col_a),
            "recibo": receipt,
            "aseguradora": safe_str(row, aseg_col_a),
            "ramo": safe_str(row, ramo_col_a),
            "cliente": safe_str(row, cliente_col_a),
            "importe_produccion": imp,
            "importe_contabilidad": None,
            "diferencia": imp,
            "explicacion": f"El recibo {receipt} existe en producción ({imp:,.2f}€) pero no aparece en contabilidad. Posible recibo no contabilizado o error de carga.",
            "accion_recomendada": "Verificar si el recibo debe contabilizarse o si hay un error de carga.",
        })

    # ── Type 2: Duplicates in B ───────────────────────────────────────────
    for receipt in duplicates_b:
        if receipt not in receipts_a:
            continue
        disc_id += 1
        row_a = a_indexed.loc[receipt]
        if isinstance(row_a, pd.DataFrame):
            row_a = row_a.iloc[0]
        rows_b = df_b[df_b[receipt_col_b].astype(str) == receipt]
        count = len(rows_b)
        imp_a = safe_float(row_a, total_col_a) or 0
        imp_b = rows_b[total_col_b].sum() if total_col_b else 0
        discrepancies.append({
            "id": disc_id,
            "tipo": "Duplicado en contabilidad",
            "severidad": "Alta",
            "poliza": safe_str(row_a, policy_col_a),
            "recibo": receipt,
            "aseguradora": safe_str(row_a, aseg_col_a),
            "ramo": safe_str(row_a, ramo_col_a),
            "cliente": safe_str(row_a, cliente_col_a),
            "importe_produccion": imp_a,
            "importe_contabilidad": float(imp_b),
            "diferencia": float(imp_b - imp_a),
            "explicacion": f"El recibo {receipt} aparece {count} veces en contabilidad. Total contabilizado: {imp_b:,.2f}€ vs producción: {imp_a:,.2f}€. Exceso: {imp_b - imp_a:,.2f}€.",
            "accion_recomendada": f"Anular {count - 1} asiento(s) duplicado(s).",
        })

    # ── Types 3-5: Compare matched records ────────────────────────────────
    for receipt in matched:
        if receipt in duplicates_b:
            continue

        row_a = a_indexed.loc[receipt]
        row_b = b_indexed.loc[receipt]
        if isinstance(row_a, pd.DataFrame):
            row_a = row_a.iloc[0]
        if isinstance(row_b, pd.DataFrame):
            row_b = row_b.iloc[0]

        p_a = safe_float(row_a, prima_col_a)
        p_b = safe_float(row_b, prima_col_b)
        t_a = safe_float(row_a, total_col_a)
        t_b = safe_float(row_b, total_col_b)

        # Prima / total difference
        if p_a is not None and p_b is not None:
            diff = abs(p_a - p_b)
            if diff > 0.05:
                disc_id += 1
                if diff <= 0.10:
                    discrepancies.append({
                        "id": disc_id,
                        "tipo": "Redondeo",
                        "severidad": "Baja",
                        "poliza": safe_str(row_a, policy_col_a),
                        "recibo": receipt,
                        "aseguradora": safe_str(row_a, aseg_col_a),
                        "ramo": safe_str(row_a, ramo_col_a),
                        "cliente": safe_str(row_a, cliente_col_a),
                        "importe_produccion": t_a or p_a,
                        "importe_contabilidad": t_b or p_b,
                        "diferencia": round((t_a or p_a) - (t_b or p_b), 2),
                        "explicacion": f"Diferencia de {diff:.2f}€ en prima neta. Compatible con redondeo.",
                        "accion_recomendada": "Aceptar si está dentro de tolerancia.",
                    })
                else:
                    discrepancies.append({
                        "id": disc_id,
                        "tipo": "Suplemento no reflejado",
                        "severidad": "Media",
                        "poliza": safe_str(row_a, policy_col_a),
                        "recibo": receipt,
                        "aseguradora": safe_str(row_a, aseg_col_a),
                        "ramo": safe_str(row_a, ramo_col_a),
                        "cliente": safe_str(row_a, cliente_col_a),
                        "importe_produccion": t_a or p_a,
                        "importe_contabilidad": t_b or p_b,
                        "diferencia": round((t_a or p_a) - (t_b or p_b), 2),
                        "explicacion": f"Diferencia de {diff:.2f}€ en prima neta. Producción: {p_a:,.2f}€, Contabilidad: {p_b:,.2f}€. Posible suplemento pendiente de contabilizar.",
                        "accion_recomendada": "Verificar si existe suplemento pendiente. Ajustar asiento si procede.",
                    })

        # Commission difference
        c_pct_a = safe_float(row_a, comision_pct_a)
        c_pct_b = safe_float(row_b, comision_pct_b)
        c_eur_a = safe_float(row_a, comision_eur_a)
        c_eur_b = safe_float(row_b, comision_eur_b)

        if c_pct_a is not None and c_pct_b is not None and abs(c_pct_a - c_pct_b) > 0.01:
            disc_id += 1
            discrepancies.append({
                "id": disc_id,
                "tipo": "Comisión incorrecta",
                "severidad": "Media",
                "poliza": safe_str(row_a, policy_col_a),
                "recibo": receipt,
                "aseguradora": safe_str(row_a, aseg_col_a),
                "ramo": safe_str(row_a, ramo_col_a),
                "cliente": safe_str(row_a, cliente_col_a),
                "importe_produccion": c_eur_a,
                "importe_contabilidad": c_eur_b,
                "diferencia": round((c_eur_a or 0) - (c_eur_b or 0), 2),
                "explicacion": f"Comisión en producción: {c_pct_a:.0f}% ({c_eur_a:,.2f}€). Contabilizada: {c_pct_b:.0f}% ({c_eur_b:,.2f}€).",
                "accion_recomendada": f"Verificar el % de comisión pactado con {safe_str(row_a, aseg_col_a)} para {safe_str(row_a, ramo_col_a)}.",
            })

        # Date difference
        f_a = safe_str(row_a, fecha_col_a)
        f_b = safe_str(row_b, fecha_col_b)
        if f_a and f_b and f_a != f_b:
            try:
                for date_fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
                    try:
                        d_a = datetime.strptime(str(f_a)[:10], date_fmt)
                        d_b = datetime.strptime(str(f_b)[:10], date_fmt)
                        break
                    except:
                        continue
                else:
                    d_a = d_b = None

                if d_a and d_b:
                    days = abs((d_a - d_b).days)
                    if days > 5:
                        disc_id += 1
                        discrepancies.append({
                            "id": disc_id,
                            "tipo": "Diferencia de timing",
                            "severidad": "Baja",
                            "poliza": safe_str(row_a, policy_col_a),
                            "recibo": receipt,
                            "aseguradora": safe_str(row_a, aseg_col_a),
                            "ramo": safe_str(row_a, ramo_col_a),
                            "cliente": safe_str(row_a, cliente_col_a),
                            "importe_produccion": t_a or p_a,
                            "importe_contabilidad": t_b or p_b,
                            "diferencia": 0,
                            "explicacion": f"Fecha producción: {f_a}. Fecha contable: {f_b}. Diferencia de {days} días. Puede afectar al devengo.",
                            "accion_recomendada": f"Verificar periodo de imputación.",
                        })
            except:
                pass

    # ── Summary ───────────────────────────────────────────────────────────
    total_a = df_a[total_col_a].sum() if total_col_a else (df_a[prima_col_a].sum() if prima_col_a else 0)
    total_b = df_b[total_col_b].sum() if total_col_b else (df_b[prima_col_b].sum() if prima_col_b else 0)

    by_type = {}
    for d in discrepancies:
        t = d["tipo"]
        if t not in by_type:
            by_type[t] = {"count": 0, "importe_total": 0}
        by_type[t]["count"] += 1
        by_type[t]["importe_total"] += abs(d["diferencia"] or 0)

    by_sev = {}
    for d in discrepancies:
        s = d["severidad"]
        by_sev[s] = by_sev.get(s, 0) + 1

    by_aseg = {}
    for d in discrepancies:
        a = d.get("aseguradora", "Desconocida")
        by_aseg[a] = by_aseg.get(a, 0) + 1

    return {
        "fecha_analisis": datetime.now().strftime("%d/%m/%Y %H:%M"),
        "registros_produccion": len(df_a),
        "registros_contabilidad": len(df_b),
        "registros_matched": len(matched),
        "total_discrepancias": len(discrepancies),
        "pct_limpio": round((1 - len(discrepancies) / max(len(df_a), 1)) * 100, 2),
        "prima_total_produccion": round(float(total_a), 2),
        "prima_total_contabilidad": round(float(total_b), 2),
        "diferencia_bruta": round(float(total_a - total_b), 2),
        "resumen_por_tipo": by_type,
        "resumen_por_severidad": by_sev,
        "resumen_por_aseguradora": by_aseg,
        "discrepancias": discrepancies,
        "columnas_detectadas": {
            "fichero_a": col_map_a,
            "fichero_b": col_map_b,
        },
    }


# ── Column detection helpers ──────────────────────────────────────────────────

COLUMN_PATTERNS = {
    "recibo": ["recibo", "receipt", "ref. recibo", "nº recibo", "n recibo", "num recibo", "numero recibo"],
    "poliza": ["poliza", "póliza", "policy", "ref. poliza", "ref. póliza", "nº poliza", "nº póliza", "n poliza", "num poliza"],
    "prima_neta": ["prima neta", "base imponible", "net premium", "prima_neta", "premium_net"],
    "prima_total": ["prima total", "total facturado", "total", "gross premium", "prima_total", "premium_total"],
    "comision_pct": ["% comis", "comision %", "comisión %", "commission %", "% comisión contable", "pct comision"],
    "comision_eur": ["comisión €", "comision €", "comisión eur", "comision eur", "comisión contabilizada", "commission"],
    "fecha": ["fecha emis", "fecha contable", "fecha_emision", "issue_date", "fecha emisión"],
    "aseguradora": ["aseguradora", "compañía", "compania", "insurer", "cia"],
    "ramo": ["ramo", "producto", "product", "branch", "line"],
    "cliente": ["cliente", "razón social", "razon social", "customer", "tomador", "asegurado"],
}


def detect_columns(df: pd.DataFrame, label: str) -> dict:
    """Try to match dataframe columns to known patterns."""
    result = {}
    cols_lower = {c: c.lower().strip() for c in df.columns}

    for field, patterns in COLUMN_PATTERNS.items():
        for col, col_low in cols_lower.items():
            for pattern in patterns:
                if pattern in col_low:
                    result[field] = col
                    break
            if field in result:
                break

    return result


def safe_float(row, col):
    if not col:
        return None
    try:
        v = row[col]
        if pd.isna(v):
            return None
        return float(v)
    except:
        return None


def safe_str(row, col):
    if not col:
        return "—"
    try:
        v = row[col]
        if pd.isna(v):
            return "—"
        return str(v)
    except:
        return "—"
