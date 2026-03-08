"""
Motor de Reconciliación Contable
================================
Cruza Fichero A (Producción/Venta) vs Fichero B (Contabilidad)
y opcionalmente vs Fichero C (Liquidaciones Aseguradoras).
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


# ── Triangular reconciliation (A vs B + A vs C) ───────────────────────────────

def _find_col(df: pd.DataFrame, *candidates: str):
    """Find a column by exact match first, then substring (case-insensitive)."""
    for name in candidates:
        for col in df.columns:
            if col.strip().lower() == name.lower():
                return col
    for name in candidates:
        for col in df.columns:
            if name.lower() in col.strip().lower():
                return col
    return None


def run_triangular_reconciliation(
    file_a_bytes: bytes, file_b_bytes: bytes, file_c_bytes: bytes
) -> dict:
    """
    Runs A vs B reconciliation (normal) and adds A vs C cross
    (Estadística de Venta vs Liquidaciones Aseguradoras).
    Discrepancies from C are marked with fuente='Liquidación aseguradora'.
    """
    result = run_reconciliation(file_a_bytes, file_b_bytes)
    if "error" in result:
        return result

    df_a = pd.read_excel(BytesIO(file_a_bytes))
    df_c = pd.read_excel(BytesIO(file_c_bytes))

    col_map_a = detect_columns(df_a, "A")
    policy_col_a  = col_map_a.get("poliza")
    prima_col_a   = col_map_a.get("prima_neta")
    total_col_a   = col_map_a.get("prima_total")
    com_pct_col_a = col_map_a.get("comision_pct")
    com_eur_col_a = col_map_a.get("comision_eur")
    recibo_col_a  = col_map_a.get("recibo")
    aseg_col_a    = col_map_a.get("aseguradora")
    ramo_col_a    = col_map_a.get("ramo")
    cliente_col_a = col_map_a.get("cliente")

    # Locate C columns using exact names from spec with fuzzy fallback
    c_poliza_col  = _find_col(df_c, "Nº Póliza Mediador", "póliza mediador", "poliza mediador")
    c_prima_col   = _find_col(df_c, "Prima Liquidada")
    c_com_pct_col = _find_col(df_c, "% Comisión Liquidada", "% comision liquidada")
    c_com_eur_col = _find_col(df_c, "Comisión Liquidada €", "Comision Liquidada €", "comisión liquidada eur")
    c_aseg_col    = _find_col(df_c, "Aseguradora")
    c_periodo_col = _find_col(df_c, "Periodo Liquidación", "Periodo Liquidacion")
    c_estado_col  = _find_col(df_c, "Estado")

    if not policy_col_a or not c_poliza_col:
        result["triangular"] = False
        result["triangular_error"] = "No se pudo cruzar A vs C: columna de póliza no detectada."
        return result

    policies_a = set(df_a[policy_col_a].dropna().astype(str))
    policies_c = set(df_c[c_poliza_col].dropna().astype(str))
    matched_ac = policies_a & policies_c
    only_in_a  = policies_a - policies_c

    a_by_pol = df_a.set_index(df_a[policy_col_a].astype(str))
    c_by_pol = df_c.set_index(df_c[c_poliza_col].astype(str))

    disc_ac = []
    disc_id = result["total_discrepancias"]

    # ── Policies in A not liquidated by C ─────────────────────────────────
    for poliza in only_in_a:
        row_a = a_by_pol.loc[poliza]
        if isinstance(row_a, pd.DataFrame):
            row_a = row_a.iloc[0]
        imp = safe_float(row_a, total_col_a) or safe_float(row_a, prima_col_a) or 0
        if imp <= 0:
            continue
        disc_id += 1
        disc_ac.append({
            "id": disc_id,
            "tipo": "Póliza no liquidada por aseguradora",
            "severidad": "Media",
            "fuente": "Liquidación aseguradora",
            "poliza": poliza,
            "recibo": safe_str(row_a, recibo_col_a),
            "aseguradora": safe_str(row_a, aseg_col_a),
            "ramo": safe_str(row_a, ramo_col_a),
            "cliente": safe_str(row_a, cliente_col_a),
            "importe_produccion": imp,
            "importe_contabilidad": None,
            "diferencia": imp,
            "explicacion": (
                f"La póliza {poliza} aparece en estadística de venta ({imp:,.2f}€) "
                f"pero no figura en el fichero de liquidaciones de la aseguradora."
            ),
            "accion_recomendada": (
                "Verificar con la aseguradora si la liquidación está pendiente o ha sido rechazada."
            ),
        })

    # ── Compare matched A vs C ────────────────────────────────────────────
    for poliza in matched_ac:
        row_a = a_by_pol.loc[poliza]
        row_c = c_by_pol.loc[poliza]
        if isinstance(row_a, pd.DataFrame):
            row_a = row_a.iloc[0]
        if isinstance(row_c, pd.DataFrame):
            row_c = row_c.iloc[0]

        p_a = safe_float(row_a, total_col_a) or safe_float(row_a, prima_col_a)
        p_c = safe_float(row_c, c_prima_col)
        aseg = safe_str(row_c, c_aseg_col)
        periodo = safe_str(row_c, c_periodo_col)

        # Prima difference
        if p_a is not None and p_c is not None:
            diff = abs(p_a - p_c)
            if diff > 0.10:
                disc_id += 1
                disc_ac.append({
                    "id": disc_id,
                    "tipo": "Diferencia prima liquidada",
                    "severidad": "Alta" if diff > 100 else "Media",
                    "fuente": "Liquidación aseguradora",
                    "poliza": poliza,
                    "recibo": safe_str(row_a, recibo_col_a),
                    "aseguradora": aseg,
                    "ramo": safe_str(row_a, ramo_col_a),
                    "cliente": safe_str(row_a, cliente_col_a),
                    "importe_produccion": p_a,
                    "importe_contabilidad": p_c,
                    "diferencia": round(p_a - p_c, 2),
                    "explicacion": (
                        f"Prima en estadística de venta: {p_a:,.2f}€. "
                        f"Prima liquidada por {aseg} (periodo {periodo}): {p_c:,.2f}€. "
                        f"Diferencia: {p_a - p_c:,.2f}€."
                    ),
                    "accion_recomendada": (
                        "Reclamar la diferencia a la aseguradora o revisar las condiciones del contrato."
                    ),
                })

        # Commission difference
        c_pct_a = safe_float(row_a, com_pct_col_a)
        c_pct_c = safe_float(row_c, c_com_pct_col)
        c_eur_a = safe_float(row_a, com_eur_col_a)
        c_eur_c = safe_float(row_c, c_com_eur_col)

        if c_pct_a is not None and c_pct_c is not None and abs(c_pct_a - c_pct_c) > 0.5:
            disc_id += 1
            disc_ac.append({
                "id": disc_id,
                "tipo": "Comisión liquidada incorrecta",
                "severidad": "Alta",
                "fuente": "Liquidación aseguradora",
                "poliza": poliza,
                "recibo": safe_str(row_a, recibo_col_a),
                "aseguradora": aseg,
                "ramo": safe_str(row_a, ramo_col_a),
                "cliente": safe_str(row_a, cliente_col_a),
                "importe_produccion": c_eur_a,
                "importe_contabilidad": c_eur_c,
                "diferencia": round((c_eur_a or 0) - (c_eur_c or 0), 2),
                "explicacion": (
                    f"Comisión esperada: {c_pct_a:.1f}% ({c_eur_a:,.2f}€). "
                    f"Liquidada por {aseg}: {c_pct_c:.1f}% ({c_eur_c:,.2f}€)."
                ),
                "accion_recomendada": "Reclamar diferencia de comisión a la aseguradora.",
            })

    # ── Merge and rebuild summary ─────────────────────────────────────────
    all_disc = result["discrepancias"] + disc_ac

    by_type = result.get("resumen_por_tipo", {})
    by_sev  = result.get("resumen_por_severidad", {})
    for d in disc_ac:
        t = d["tipo"]
        if t not in by_type:
            by_type[t] = {"count": 0, "importe_total": 0}
        by_type[t]["count"] += 1
        by_type[t]["importe_total"] += abs(d["diferencia"] or 0)
        s = d["severidad"]
        by_sev[s] = by_sev.get(s, 0) + 1

    result.update({
        "triangular": True,
        "registros_liquidaciones": len(df_c),
        "discrepancias": all_disc,
        "total_discrepancias": len(all_disc),
        "resumen_por_tipo": by_type,
        "resumen_por_severidad": by_sev,
    })
    return result
