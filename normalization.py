"""
Motor de Normalización / Homogeneización de Datos
==================================================
Recibe un fichero Excel de una correduría adquirida,
detecta el esquema, lo mapea al modelo canónico y
genera un informe de calidad.
"""

import pandas as pd
import numpy as np
from io import BytesIO
from datetime import datetime
import re

# ─── CANONICAL MODEL ─────────────────────────────────────────────────────────

CANONICAL_FIELDS = {
    "policy_id": {
        "label": "ID Póliza",
        "type": "string",
        "patterns": ["poliza", "póliza", "apólice", "apolice", "polizza", "policy", "nº poliza", "nº póliza", "n. polizza", "nº apólice", "n poliza"],
    },
    "customer_name": {
        "label": "Nombre Cliente",
        "type": "string",
        "patterns": ["tomador", "cliente", "asegurado", "customer", "contraente", "tomador do seguro", "razón social", "razon social"],
    },
    "tax_id": {
        "label": "ID Fiscal",
        "type": "string",
        "patterns": ["cif", "nif", "nipc", "partita iva", "tax id", "id fiscal", "identificación fiscal"],
    },
    "insurer": {
        "label": "Aseguradora",
        "type": "string",
        "patterns": ["aseguradora", "compañía", "compania", "seguradora", "compagnia", "insurer", "cia", "compañía aseguradora"],
    },
    "product_line": {
        "label": "Ramo / Producto",
        "type": "string",
        "patterns": ["ramo", "producto", "product", "branch", "line", "ramo de seguro"],
    },
    "premium_net": {
        "label": "Prima Neta",
        "type": "decimal",
        "patterns": ["prima neta", "prémio líquido", "premio liquido", "premio imponibile", "net premium", "base imponible", "prima neta €"],
    },
    "taxes": {
        "label": "Impuestos",
        "type": "decimal",
        "patterns": ["impuesto", "recargo", "impostos", "imposte", "tax", "impuestos y recargos"],
    },
    "premium_gross": {
        "label": "Prima Total",
        "type": "decimal",
        "patterns": ["prima total", "prémio total", "premio total", "premio lordo", "gross premium", "prima total €", "premio lordo €"],
    },
    "commission_pct": {
        "label": "% Comisión",
        "type": "decimal",
        "patterns": ["% comis", "comision %", "comisión %", "comissão %", "provvigione %", "commission %", "pct comision"],
    },
    "commission_amount": {
        "label": "Importe Comisión",
        "type": "decimal",
        "patterns": ["comisión €", "comision €", "importe comisión", "valor comissão", "provvigione €", "commission amount"],
    },
    "payment_frequency": {
        "label": "Forma Pago",
        "type": "string",
        "patterns": ["forma de pago", "forma pago", "fracionamento", "frazionamento", "payment", "frecuencia"],
    },
    "inception_date": {
        "label": "Fecha Efecto",
        "type": "date",
        "patterns": ["fecha efecto", "data início", "data inicio", "decorrenza", "inception", "fecha inicio", "date start"],
    },
    "expiry_date": {
        "label": "Fecha Vencimiento",
        "type": "date",
        "patterns": ["fecha vencimiento", "data fim", "scadenza", "expiry", "vencimiento", "fecha fin"],
    },
    "status": {
        "label": "Estado",
        "type": "string",
        "patterns": ["estado", "situação", "situacao", "stato", "status", "estado póliza", "estado poliza"],
    },
    "sector": {
        "label": "Sector",
        "type": "string",
        "patterns": ["sector", "atividade", "settore", "industry", "actividad", "sector actividad"],
    },
    "region": {
        "label": "Región",
        "type": "string",
        "patterns": ["provincia", "distrito", "region", "comunidad", "zona"],
    },
    "sales_agent": {
        "label": "Comercial",
        "type": "string",
        "patterns": ["comercial", "gestor", "produttore", "agent", "vendedor", "delegado"],
    },
    "notes": {
        "label": "Observaciones",
        "type": "string",
        "patterns": ["observacion", "nota", "note", "comentario", "comment"],
    },
}

# Status normalization map
STATUS_MAP = {
    # Spanish
    "en vigor": "active", "anulada": "cancelled", "suspendida": "suspended",
    "activa": "active", "cancelada": "cancelled",
    # Portuguese
    "ativa": "active", "anulada": "cancelled", "suspensa": "suspended",
    # Italian
    "in vigore": "active", "annullata": "cancelled", "sospesa": "suspended",
    "attiva": "active",
    # English
    "active": "active", "cancelled": "cancelled", "suspended": "suspended",
}

# Payment frequency normalization
FREQUENCY_MAP = {
    "anual": "annual", "annual": "annual", "annuale": "annual",
    "semestral": "semiannual", "semiannual": "semiannual", "semestrale": "semiannual",
    "trimestral": "quarterly", "quarterly": "quarterly", "trimestrale": "quarterly",
    "mensual": "monthly", "mensal": "monthly", "monthly": "monthly", "mensile": "monthly",
}

# Product line normalization
PRODUCT_MAP = {
    # Spanish
    "rc general": "rc_general", "rc profesional": "rc_profesional",
    "multirriesgo empresa": "multirriesgo", "flota vehículos": "flota",
    "salud colectivo": "salud_colectivo", "cyber riesgo": "cyber",
    "d&o": "dyo", "accidentes convenio": "accidentes",
    # Portuguese
    "responsabilidade civil geral": "rc_general", "responsabilidade civil profissional": "rc_profesional",
    "multirriscos empresa": "multirriesgo", "frota automóvel": "flota",
    "saúde grupo": "salud_colectivo", "ciber risco": "cyber",
    "acidentes de trabalho": "accidentes",
    # Italian
    "rc generale": "rc_general", "rc professionale": "rc_profesional",
    "multirischio azienda": "multirriesgo", "flotta veicoli": "flota",
    "salute collettiva": "salud_colectivo", "cyber risk": "cyber",
    "infortuni": "accidentes",
}


def run_normalization(file_bytes: bytes, filename: str = "") -> dict:
    """
    Receives an Excel file, detects schema, maps to canonical, validates data.
    Returns results dict with mapping, normalized data, and quality report.
    """
    df = pd.read_excel(BytesIO(file_bytes))
    
    # ── Step 1: Detect language / origin ──────────────────────────────────
    cols_text = " ".join(df.columns).lower()
    if any(w in cols_text for w in ["tomador do seguro", "apólice", "prémio", "nipc", "fracionamento"]):
        detected_lang = "Portugués"
        detected_country = "PT"
    elif any(w in cols_text for w in ["contraente", "polizza", "provvigione", "partita iva", "frazionamento"]):
        detected_lang = "Italiano"
        detected_country = "IT"
    elif any(w in cols_text for w in ["tomador", "póliza", "prima neta", "cif", "aseguradora"]):
        detected_lang = "Español"
        detected_country = "ES"
    else:
        detected_lang = "Desconocido"
        detected_country = "??"

    # ── Step 2: Map columns to canonical ──────────────────────────────────
    # First pass: score all possible matches
    all_matches = []  # (col, canon_field, confidence, pattern_len)
    
    for col in df.columns:
        col_lower = col.lower().strip()
        
        for canon_field, config in CANONICAL_FIELDS.items():
            for pattern in config["patterns"]:
                score = 0
                confidence = "none"
                
                if col_lower == pattern:
                    score = 100
                    confidence = "high"
                elif pattern in col_lower and len(pattern) > 3:
                    # Longer pattern = better match
                    score = 50 + len(pattern)
                    confidence = "high"
                elif col_lower in pattern and len(col_lower) > 3:
                    score = 30 + len(col_lower)
                    confidence = "medium"
                
                if score > 0:
                    all_matches.append((col, canon_field, confidence, score, pattern))
    
    # Sort by score descending — best matches first
    all_matches.sort(key=lambda x: -x[3])
    
    # Assign: each column gets at most one canonical field, each canonical field at most one column
    used_cols = set()
    used_canon = set()
    col_to_canon = {}
    
    for col, canon_field, confidence, score, pattern in all_matches:
        if col not in used_cols and canon_field not in used_canon:
            col_to_canon[col] = (canon_field, confidence)
            used_cols.add(col)
            used_canon.add(canon_field)
    
    # Second pass: try fuzzy for unmapped columns
    for col in df.columns:
        if col in used_cols:
            continue
        col_lower = col.lower().strip()
        col_words = set(re.split(r'[\s_\-\.]+', col_lower))
        
        for canon_field, config in CANONICAL_FIELDS.items():
            if canon_field in used_canon:
                continue
            for pattern in config["patterns"]:
                pattern_words = set(re.split(r'[\s_\-\.]+', pattern))
                overlap = col_words & pattern_words - {"€", "de", "do", "di", "el", "la", "los"}
                if overlap and len(overlap) > 0:
                    col_to_canon[col] = (canon_field, "low")
                    used_cols.add(col)
                    used_canon.add(canon_field)
                    break
            if col in used_cols:
                break
    
    # Build mapping list and mapped_canonical dict
    mapping = []
    mapped_canonical = {}
    
    for col in df.columns:
        if col in col_to_canon:
            canon_field, confidence = col_to_canon[col]
            mapping.append({
                "columna_original": col,
                "campo_canonico": canon_field,
                "label_canonico": CANONICAL_FIELDS[canon_field]["label"],
                "confianza": confidence,
                "tipo_esperado": CANONICAL_FIELDS[canon_field]["type"],
                "ejemplo_valor": str(df[col].dropna().iloc[0]) if len(df[col].dropna()) > 0 else "—",
            })
            mapped_canonical[canon_field] = col
        else:
            mapping.append({
                "columna_original": col,
                "campo_canonico": "—",
                "label_canonico": "No mapeado",
                "confianza": "none",
                "tipo_esperado": "—",
                "ejemplo_valor": str(df[col].dropna().iloc[0]) if len(df[col].dropna()) > 0 else "—",
            })

    # ── Step 3: Normalize data ────────────────────────────────────────────
    normalized = pd.DataFrame()
    
    for canon_field, orig_col in mapped_canonical.items():
        series = df[orig_col].copy()
        
        # Normalize status
        if canon_field == "status":
            series = series.astype(str).str.lower().str.strip().map(STATUS_MAP).fillna("unknown")
        
        # Normalize payment frequency
        elif canon_field == "payment_frequency":
            series = series.astype(str).str.lower().str.strip().map(FREQUENCY_MAP).fillna("unknown")
        
        # Normalize product line
        elif canon_field == "product_line":
            series = series.astype(str).str.lower().str.strip().map(PRODUCT_MAP).fillna(series.astype(str).str.lower().str.strip())
        
        # Normalize dates to DD/MM/YYYY
        elif canon_field in ("inception_date", "expiry_date"):
            normalized_dates = []
            for val in series:
                val_str = str(val).strip()
                parsed = None
                for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d.%m.%Y", "%d-%m-%Y", "%Y/%m/%d"):
                    try:
                        parsed = datetime.strptime(val_str[:10], fmt)
                        break
                    except:
                        continue
                if parsed:
                    normalized_dates.append(parsed.strftime("%d/%m/%Y"))
                else:
                    normalized_dates.append(f"ERROR: {val_str}")
            series = pd.Series(normalized_dates)
        
        # Numeric fields
        elif CANONICAL_FIELDS[canon_field]["type"] == "decimal":
            series = pd.to_numeric(series, errors="coerce")
        
        normalized[canon_field] = series

    # ── Step 4: Quality analysis ──────────────────────────────────────────
    total_records = len(df)
    
    quality_issues = []
    
    # Check for duplicates
    if "policy_id" in normalized.columns:
        dupes = normalized["policy_id"].duplicated()
        n_dupes = dupes.sum()
        if n_dupes > 0:
            quality_issues.append({
                "tipo": "Duplicados",
                "campo": "policy_id",
                "cantidad": int(n_dupes),
                "severidad": "Alta",
                "detalle": f"{n_dupes} pólizas duplicadas detectadas.",
            })
    
    # Check for empty required fields
    required_fields = ["policy_id", "customer_name", "tax_id", "insurer", "product_line", "premium_net"]
    for field in required_fields:
        if field in normalized.columns:
            empties = normalized[field].isna() | (normalized[field].astype(str).str.strip() == "") | (normalized[field].astype(str) == "nan")
            n_empty = empties.sum()
            if n_empty > 0:
                quality_issues.append({
                    "tipo": "Campo vacío",
                    "campo": field,
                    "cantidad": int(n_empty),
                    "severidad": "Alta" if field in ("policy_id", "tax_id") else "Media",
                    "detalle": f"{n_empty} registros con {CANONICAL_FIELDS[field]['label']} vacío.",
                })
    
    # Check for negative premiums
    for field in ["premium_net", "premium_gross"]:
        if field in normalized.columns:
            negatives = pd.to_numeric(normalized[field], errors="coerce") < 0
            n_neg = negatives.sum()
            if n_neg > 0:
                quality_issues.append({
                    "tipo": "Valor negativo",
                    "campo": field,
                    "cantidad": int(n_neg),
                    "severidad": "Alta",
                    "detalle": f"{n_neg} registros con {CANONICAL_FIELDS[field]['label']} negativa.",
                })
    
    # Check for date parsing errors
    for field in ["inception_date", "expiry_date"]:
        if field in normalized.columns:
            errors = normalized[field].astype(str).str.startswith("ERROR:")
            n_errors = errors.sum()
            if n_errors > 0:
                quality_issues.append({
                    "tipo": "Fecha inválida",
                    "campo": field,
                    "cantidad": int(n_errors),
                    "severidad": "Media",
                    "detalle": f"{n_errors} registros con formato de fecha no reconocido.",
                })
    
    # Check for invalid tax IDs
    if "tax_id" in normalized.columns:
        invalid_tax = normalized["tax_id"].apply(
            lambda x: str(x).strip() in ("", "nan", "INVALID", "None") or len(str(x).strip()) < 5
        )
        n_invalid = invalid_tax.sum()
        if n_invalid > 0:
            quality_issues.append({
                "tipo": "ID fiscal inválido",
                "campo": "tax_id",
                "cantidad": int(n_invalid),
                "severidad": "Alta",
                "detalle": f"{n_invalid} registros con identificador fiscal vacío o inválido.",
            })
    
    # Check for zero premiums
    if "premium_net" in normalized.columns:
        zeros = pd.to_numeric(normalized["premium_net"], errors="coerce") == 0
        n_zeros = zeros.sum()
        if n_zeros > 0:
            quality_issues.append({
                "tipo": "Prima cero",
                "campo": "premium_net",
                "cantidad": int(n_zeros),
                "severidad": "Media",
                "detalle": f"{n_zeros} registros con prima neta igual a cero.",
            })

    # Summary stats
    n_mapped = sum(1 for m in mapping if m["confianza"] != "none")
    n_high = sum(1 for m in mapping if m["confianza"] == "high")
    n_medium = sum(1 for m in mapping if m["confianza"] == "medium")
    n_low = sum(1 for m in mapping if m["confianza"] == "low")
    n_unmapped = sum(1 for m in mapping if m["confianza"] == "none")
    
    total_issues = sum(q["cantidad"] for q in quality_issues)
    records_clean = total_records - min(total_issues, total_records)
    pct_clean = round((records_clean / max(total_records, 1)) * 100, 1)

    def _clean(val):
        """Convert numpy types to Python native for JSON serialization."""
        if isinstance(val, (np.integer,)): return int(val)
        if isinstance(val, (np.floating,)): return None if np.isnan(val) else float(val)
        if isinstance(val, (np.bool_,)): return bool(val)
        if isinstance(val, dict): return {k: _clean(v) for k, v in val.items()}
        if isinstance(val, list): return [_clean(v) for v in val]
        return val

    return _clean({
        "filename": filename,
        "detected_language": detected_lang,
        "detected_country": detected_country,
        "total_records": total_records,
        "total_columns": len(df.columns),
        "columns_mapped": n_mapped,
        "columns_unmapped": n_unmapped,
        "mapping_confidence": {
            "high": n_high,
            "medium": n_medium,
            "low": n_low,
            "unmapped": n_unmapped,
        },
        "mapping": mapping,
        "quality_issues": quality_issues,
        "total_quality_issues": total_issues,
        "records_clean": records_clean,
        "pct_clean": pct_clean,
        "normalized_preview": normalized.head(10).replace({np.nan: None}).to_dict(orient="records"),
        "original_columns": list(df.columns),
        "canonical_columns": list(normalized.columns),
    })
