"""
Motor de Data Quality — Sabseg Caso de Uso 1
=============================================
Detecta los 20 tipos de errores definidos por Sabseg en ficheros de recibos.
Genera log de errores, correcciones automáticas donde es viable,
y textos para informar a las corredurías donde no se puede corregir.
"""

import pandas as pd
import numpy as np
import re
import os
from io import BytesIO
from datetime import datetime
from collections import Counter


def _clean(obj):
    """Recursively convert pandas/numpy types to JSON-safe Python types."""
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean(v) for v in obj]
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return None if np.isnan(obj) else float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, float) and (obj != obj):
        return None
    if obj is pd.NaT:
        return None
    try:
        if pd.isna(obj):
            return None
    except (ValueError, TypeError):
        pass
    return obj


# ─── NIF VALIDATION ──────────────────────────────────────────────────────────

NIF_LETTER_TABLE = "TRWAGMYFPDXBNJZSQVHLCKE"

def validate_nif(nif_str):
    """Validate Spanish NIF/CIF/NIE. Returns (is_valid, error_type, suggestion)."""
    try:
        if nif_str is None or pd.isna(nif_str):
            return False, "vacio", "Asignar NIF genérico"
    except (ValueError, TypeError):
        pass
    if not nif_str:
        return False, "vacio", "Asignar NIF genérico"
    
    nif = str(nif_str).strip()
    
    # Remove common bad characters
    cleaned = re.sub(r'[\s\.\-,/]', '', nif)
    
    if len(cleaned) == 0:
        return False, "vacio", "Asignar NIF genérico"
    
    # Check length
    if len(cleaned) < 8:
        return False, "corto", f"NIF demasiado corto ({len(cleaned)} caracteres). Asignar NIF genérico"
    if len(cleaned) > 11:
        return False, "largo", f"NIF demasiado largo ({len(cleaned)} caracteres). Verificar manualmente"
    
    # Check for special characters remaining
    if re.search(r'[^A-Za-z0-9]', cleaned):
        return False, "caracteres_especiales", f"Caracteres no admitidos. Sugerencia: {cleaned}"
    
    cleaned_upper = cleaned.upper()
    
    # CIF (starts with A-H, J, N, P, Q, R, S, U, V, W)
    if cleaned_upper[0] in 'ABCDEFGHJNPQRSUVW':
        if len(cleaned_upper) == 9 and cleaned_upper[1:8].isdigit():
            return True, None, None
        return False, "cif_malformado", f"CIF con formato incorrecto: {nif}"
    
    # NIE (starts with X, Y, Z)
    if cleaned_upper[0] in 'XYZ':
        nie_map = {'X': '0', 'Y': '1', 'Z': '2'}
        num_str = nie_map[cleaned_upper[0]] + cleaned_upper[1:-1]
        if num_str.isdigit() and cleaned_upper[-1].isalpha():
            expected = NIF_LETTER_TABLE[int(num_str) % 23]
            if cleaned_upper[-1] == expected:
                return True, None, None
            return False, "nie_letra_incorrecta", f"Letra incorrecta. Esperada: {expected}. Sugerencia: {cleaned_upper[:-1]}{expected}"
        return False, "nie_malformado", f"NIE malformado: {nif}"
    
    # NIF personal (8 digits + letter)
    if cleaned_upper[-1].isalpha() and cleaned_upper[:-1].isdigit():
        num = int(cleaned_upper[:-1])
        expected = NIF_LETTER_TABLE[num % 23]
        if cleaned_upper[-1] == expected:
            return True, None, None
        return False, "letra_incorrecta", f"Letra incorrecta. Esperada: {expected}. Sugerencia: {cleaned_upper[:-1]}{expected}"
    
    # Numbers only (missing letter)
    if cleaned_upper.isdigit() and len(cleaned_upper) == 8:
        letter = NIF_LETTER_TABLE[int(cleaned_upper) % 23]
        return False, "sin_letra", f"NIF sin letra. Sugerencia: {cleaned_upper}{letter}"
    
    # Two letters
    if cleaned_upper[0].isalpha() and cleaned_upper[-1].isalpha() and not cleaned_upper[0] in 'ABCDEFGHJNPQRSUVWXYZ':
        return False, "dos_letras", f"Formato no reconocido. Verificar manualmente"
    
    return False, "formato_desconocido", f"Formato no reconocido: {nif}. Asignar NIF genérico"


# ─── MAIN VALIDATION ENGINE ──────────────────────────────────────────────────

def validate_file(file_bytes, filename, mapping_data=None):
    """
    Validate a single brokerage data file.
    Returns dict with errors, warnings, corrections, and quality summary.
    """
    
    # Detect file structure
    file_info = detect_structure(file_bytes, filename)
    if 'error' in file_info:
        return file_info
    
    df = file_info['dataframe']
    headers = file_info['headers']
    data_sheet = file_info['data_sheet']
    mapping = file_info.get('mapping', {})
    correduría = file_info.get('correduria', filename.split('.')[0])
    
    errors = []
    warnings = []
    corrections = []
    error_id = 0
    
    # ── ERROR 1: Nuevos productos no mapeados ────────────────────────────
    if 'ramo_mapping' in mapping and 'Producto' in df.columns:
        known_products = set(mapping['ramo_mapping'].keys())
        file_products = set(df['Producto'].dropna().unique())
        unmapped = file_products - known_products
        if unmapped:
            error_id += 1
            errors.append({
                'id': error_id, 'tipo': 'Producto no mapeado', 'error_num': 1,
                'severidad': 'Media', 'campo': 'Producto',
                'cantidad': len(unmapped), 'valores': list(unmapped)[:10],
                'detalle': f"{len(unmapped)} productos sin mapeo a ramo SABSEG: {', '.join(list(unmapped)[:5])}",
                'correccion_auto': False,
                'sugerencia': 'Proponer ramo/grupo de ramo en base al mapeo actual',
                'para_correo': f"Los siguientes productos no están mapeados: {', '.join(list(unmapped)[:10])}. Necesitamos la equivalencia de ramo para incluirlos."
            })
    
    # ── ERROR 2-3: Nombres de compañía no reconocidos ────────────────────
    cia_col = _find_column(df, ['Compania', 'Compañia', 'Compañía', 'Entidad', 'CIA'])
    if cia_col and 'cia_mapping' in mapping:
        known_cias = set(mapping['cia_mapping'].keys())
        file_cias = set(df[cia_col].dropna().astype(str).unique())
        unknown_cias = file_cias - known_cias
        if unknown_cias:
            error_id += 1
            errors.append({
                'id': error_id, 'tipo': 'Compañía no reconocida', 'error_num': 2,
                'severidad': 'Media', 'campo': cia_col,
                'cantidad': len(unknown_cias), 'valores': list(unknown_cias)[:10],
                'detalle': f"{len(unknown_cias)} nombres de compañía no reconocidos",
                'correccion_auto': False,
                'sugerencia': 'Proponer nombre corto de compañía',
                'para_correo': f"Los siguientes nombres de compañía no coinciden con el catálogo: {', '.join(list(unknown_cias)[:10])}"
            })
    
    # ── ERROR 4: Cambio de nombre de columnas ────────────────────────────
    expected_cols = ['Correduria', 'NIF', 'NumeroPolizaCompania', 'NumeroPolizaInterno',
                     'NumeroReciboCompania', 'NumeroReciboInterno', 'Tipo', 'Gestion',
                     'Duracion', 'Situacion', 'FechaFacturacion', 'FechaEfecto',
                     'PrimaNeta', 'ComisionCorreduria', 'ComisionPrimaNeta', 'ComisionComplementaria']
    actual_cols = list(df.columns)
    missing = [c for c in expected_cols if not _find_column(df, [c])]
    extra = [c for c in actual_cols if c not in expected_cols and not c.startswith('_')]
    if missing:
        error_id += 1
        warnings.append({
            'id': error_id, 'tipo': 'Columnas esperadas no encontradas', 'error_num': 4,
            'severidad': 'Alta', 'campo': 'Estructura',
            'cantidad': len(missing), 'valores': missing,
            'detalle': f"Columnas no encontradas: {', '.join(missing)}",
            'sugerencia': 'Verificar si han cambiado de nombre o se han eliminado',
        })
    
    # ── ERROR 5: Valores no normalizados ─────────────────────────────────
    tipo_col = _find_column(df, ['Tipo'])
    if tipo_col:
        vals = df[tipo_col].dropna().unique()
        numeric_vals = [v for v in vals if str(v).strip().isdigit()]
        if numeric_vals:
            error_id += 1
            errors.append({
                'id': error_id, 'tipo': 'Valores no normalizados', 'error_num': 5,
                'severidad': 'Media', 'campo': tipo_col,
                'cantidad': len(numeric_vals), 'valores': [str(v) for v in numeric_vals],
                'detalle': f"Campo '{tipo_col}' contiene valores numéricos en vez de texto: {numeric_vals}",
                'correccion_auto': True,
                'sugerencia': 'Proponer nombre en función del mapeo anterior',
            })
    
    # ── ERROR 6: Información en campo equivocado ─────────────────────────
    dur_col = _find_column(df, ['Duracion'])
    fracciones_col = _find_column(df, ['Fracciones'])
    if dur_col:
        dur_vals = df[dur_col].dropna().unique()
        # If Duracion has numbers like 1,2,3,4,6,11,12 = probably fracciones
        if all(str(v).strip().isdigit() for v in dur_vals if pd.notna(v)):
            int_vals = [int(str(v)) for v in dur_vals if pd.notna(v) and str(v).strip().isdigit()]
            if set(int_vals).issubset({1, 2, 3, 4, 6, 11, 12}):
                error_id += 1
                errors.append({
                    'id': error_id, 'tipo': 'Información en campo equivocado', 'error_num': 6,
                    'severidad': 'Alta', 'campo': dur_col,
                    'cantidad': len(df), 'valores': list(set(int_vals)),
                    'detalle': f"El campo '{dur_col}' contiene lo que parecen fracciones de pago ({sorted(set(int_vals))}) en vez de duración (Renovable/Temporal/etc.)",
                    'correccion_auto': True,
                    'sugerencia': 'Proponer intercambio de columnas basado en coherencia de valores',
                })
    
    # ── ERROR 7-8: NIFs ──────────────────────────────────────────────────
    nif_col = _find_column(df, ['NIF', 'Nif', 'nif', 'CIF'])
    if nif_col:
        nif_issues = {'vacio': [], 'sin_letra': [], 'letra_incorrecta': [], 'caracteres_especiales': [], 
                      'corto': [], 'largo': [], 'formato_desconocido': [], 'cif_malformado': [], 
                      'nie_malformado': [], 'nie_letra_incorrecta': [], 'dos_letras': []}
        
        for idx, nif in df[nif_col].items():
            is_valid, error_type, suggestion = validate_nif(nif)
            if not is_valid and error_type:
                nif_issues[error_type].append({'row': idx, 'value': str(nif), 'suggestion': suggestion})
        
        # Empty NIFs
        empty_count = len(nif_issues['vacio'])
        if empty_count > 0:
            error_id += 1
            errors.append({
                'id': error_id, 'tipo': 'NIF vacío', 'error_num': 7,
                'severidad': 'Alta', 'campo': nif_col,
                'cantidad': empty_count,
                'detalle': f"{empty_count} registros sin NIF",
                'correccion_auto': True,
                'sugerencia': 'Asignar NIF genérico',
            })
        
        # Non-standard NIFs
        non_standard = sum(len(v) for k, v in nif_issues.items() if k != 'vacio')
        if non_standard > 0:
            error_id += 1
            details = []
            for k, v in nif_issues.items():
                if k != 'vacio' and len(v) > 0:
                    details.append(f"{k}: {len(v)}")
            errors.append({
                'id': error_id, 'tipo': 'NIF no normalizado', 'error_num': 8,
                'severidad': 'Media', 'campo': nif_col,
                'cantidad': non_standard,
                'detalle': f"{non_standard} NIFs con formato incorrecto. {'; '.join(details)}",
                'correccion_auto': True,
                'sugerencia': 'Eliminar caracteres especiales, calcular letra, o asignar NIF genérico',
                'ejemplos': [nif_issues[k][0] for k in nif_issues if k != 'vacio' and len(nif_issues[k]) > 0][:5],
            })
    
    # ── ERROR 9: Valores numéricos con puntos por comas ──────────────────
    numeric_cols = _find_columns(df, ['PrimaNeta', 'Prima', 'ComisionCorreduria', 'ComisionPrimaNeta',
                                       'ComisionComplementaria', 'ComisionColaborador1'])
    dot_comma_issues = 0
    for col in numeric_cols:
        str_vals = df[col].astype(str)
        # Values like "1.234,56" or "1234.56" with dots as thousands separator
        has_dot_comma = str_vals.str.match(r'^\d{1,3}\.\d{3}').sum()
        dot_comma_issues += has_dot_comma
    
    if dot_comma_issues > 0:
        error_id += 1
        errors.append({
            'id': error_id, 'tipo': 'Formato numérico incorrecto', 'error_num': 9,
            'severidad': 'Baja', 'campo': 'Campos numéricos',
            'cantidad': dot_comma_issues,
            'detalle': f"{dot_comma_issues} valores con posible confusión punto/coma en separador decimal",
            'correccion_auto': True,
            'sugerencia': 'Cambiar puntos por comas en separadores de miles',
        })
    
    # ── ERROR 10: Comisiones erróneas (% muy bajo para el ramo) ──────────
    prima_col = _find_column(df, ['PrimaNeta', 'Prima neta', 'Prima'])
    com_col = _find_column(df, ['ComisionCorreduria', 'ComisionPrimaNeta', 'Comisión prima neta', 'Com, Bruta'])
    if prima_col and com_col:
        prima = pd.to_numeric(df[prima_col], errors='coerce')
        com = pd.to_numeric(df[com_col], errors='coerce')
        pct = (com / prima * 100).where(prima > 0)
        very_low = ((pct > 0) & (pct < 2)).sum()
        if very_low > 0:
            error_id += 1
            warnings.append({
                'id': error_id, 'tipo': 'Comisión inusualmente baja', 'error_num': 10,
                'severidad': 'Baja', 'campo': com_col,
                'cantidad': int(very_low),
                'detalle': f"{very_low} recibos con comisión < 2% de la prima neta",
                'sugerencia': 'Verificar si el % de comisión es correcto para el ramo',
            })
    
    # ── ERROR 11: Descuadre CB = CPN + CC ────────────────────────────────
    cb_col = _find_column(df, ['ComisionCorreduria', 'Com, Bruta', 'Comisión Bruta'])
    cpn_col = _find_column(df, ['ComisionPrimaNeta', 'Comisión prima neta', 'Comisión Prima Neta'])
    cc_col = _find_column(df, ['ComisionComplementaria', 'Comisión complementaria'])
    
    if cb_col and cpn_col and cc_col:
        cb = pd.to_numeric(df[cb_col], errors='coerce').fillna(0)
        cpn = pd.to_numeric(df[cpn_col], errors='coerce').fillna(0)
        cc = pd.to_numeric(df[cc_col], errors='coerce').fillna(0)
        diff = abs(cb - (cpn + cc))
        descuadre = (diff > 0.01).sum()
        if descuadre > 0:
            error_id += 1
            errors.append({
                'id': error_id, 'tipo': 'Descuadre de comisiones', 'error_num': 11,
                'severidad': 'Media', 'campo': f'{cb_col} vs {cpn_col}+{cc_col}',
                'cantidad': int(descuadre),
                'detalle': f"{descuadre} recibos donde Comisión Bruta ≠ Comisión Prima Neta + Comisión Complementaria",
                'correccion_auto': True,
                'sugerencia': 'Recalcular CB = CPN + CC',
            })
    
    # ── ERROR 13: Recibos con comisión 0 ─────────────────────────────────
    if com_col:
        com_vals = pd.to_numeric(df[com_col], errors='coerce')
        zero_com = ((com_vals == 0) | com_vals.isna()).sum()
        if prima_col:
            prima_vals = pd.to_numeric(df[prima_col], errors='coerce')
            zero_com_with_prima = ((com_vals == 0) & (prima_vals > 0)).sum()
            if zero_com_with_prima > 0:
                error_id += 1
                warnings.append({
                    'id': error_id, 'tipo': 'Recibos con comisión 0', 'error_num': 13,
                    'severidad': 'Media', 'campo': com_col,
                    'cantidad': int(zero_com_with_prima),
                    'detalle': f"{zero_com_with_prima} recibos con prima > 0 pero comisión = 0",
                    'sugerencia': 'Informar de los casos. No se puede corregir automáticamente.',
                    'para_correo': f"Hemos detectado {zero_com_with_prima} recibos con prima positiva pero sin comisión. ¿Podéis verificar si es correcto?"
                })
    
    # ── ERROR 15: Formatos de fecha erróneos ─────────────────────────────
    date_cols = _find_columns(df, ['FechaFacturacion', 'FechaEfecto', 'FechaCobro', 'FechaAnulacion',
                                    'F. Producc.', 'F. Efecto'])
    for dcol in date_cols:
        dates = pd.to_datetime(df[dcol], errors='coerce')
        bad_dates = dates.isna().sum() - df[dcol].isna().sum()  # non-null but unparseable
        if bad_dates > 0:
            error_id += 1
            # Get examples
            bad_mask = dates.isna() & df[dcol].notna()
            examples = df.loc[bad_mask, dcol].head(5).tolist()
            errors.append({
                'id': error_id, 'tipo': 'Formato de fecha erróneo', 'error_num': 15,
                'severidad': 'Media', 'campo': dcol,
                'cantidad': int(bad_dates),
                'detalle': f"{bad_dates} valores con formato de fecha no reconocido en '{dcol}'",
                'valores': [str(v) for v in examples],
                'correccion_auto': True,
                'sugerencia': 'Cambiar a formato de fecha estándar DD/MM/YYYY',
            })
    
    # ── ERROR 17: Recibos duplicados sin anular ──────────────────────────
    poliza_col = _find_column(df, ['NumeroPolizaCompania', 'Nº Póliza', 'NumPoliza'])
    fecha_fac_col = _find_column(df, ['FechaFacturacion', 'F. Producc.', 'Fecha facturación'])
    situacion_col = _find_column(df, ['Situacion', 'Est', 'Estado'])
    
    if poliza_col and fecha_fac_col:
        dupes = df.duplicated(subset=[poliza_col, fecha_fac_col], keep=False)
        if situacion_col:
            # Only flag if neither is anulado
            anulado_mask = df[situacion_col].astype(str).str.lower().str.contains('anulad', na=False)
            problematic_dupes = dupes & ~anulado_mask
            n_dupes = problematic_dupes.sum()
        else:
            n_dupes = dupes.sum()
        
        if n_dupes > 0:
            error_id += 1
            warnings.append({
                'id': error_id, 'tipo': 'Posibles recibos duplicados', 'error_num': 17,
                'severidad': 'Media', 'campo': f'{poliza_col} + {fecha_fac_col}',
                'cantidad': int(n_dupes),
                'detalle': f"{n_dupes} recibos con misma póliza y fecha sin estar anulados",
                'sugerencia': 'Verificar si son duplicados o recibos legítimos',
            })
    
    # ── ERROR 18: Fecha anulación antes de emisión ───────────────────────
    anulacion_col = _find_column(df, ['FechaAnulacion'])
    if fecha_fac_col and anulacion_col:
        f_fac = pd.to_datetime(df[fecha_fac_col], errors='coerce')
        f_anu = pd.to_datetime(df[anulacion_col], errors='coerce')
        incoherent = ((f_anu < f_fac) & f_anu.notna() & f_fac.notna()).sum()
        if incoherent > 0:
            error_id += 1
            errors.append({
                'id': error_id, 'tipo': 'Fecha anulación incoherente', 'error_num': 18,
                'severidad': 'Media', 'campo': anulacion_col,
                'cantidad': int(incoherent),
                'detalle': f"{incoherent} recibos con fecha de anulación anterior a la fecha de emisión",
                'sugerencia': 'Posible error por cambio de año. Verificar manualmente.',
                'para_correo': f"Hemos detectado {incoherent} recibos donde la fecha de anulación es anterior a la fecha de emisión. ¿Podéis revisar?"
            })
    
    # ── ERROR 19: Espacios en blanco ─────────────────────────────────────
    space_issues = 0
    for col in df.columns:
        if df[col].dtype == object:
            has_spaces = df[col].astype(str).str.match(r'^\s+|\s+$').sum()
            space_issues += has_spaces
    
    if space_issues > 0:
        error_id += 1
        corrections.append({
            'id': error_id, 'tipo': 'Espacios en blanco', 'error_num': 19,
            'severidad': 'Baja', 'campo': 'Varios',
            'cantidad': int(space_issues),
            'detalle': f"{space_issues} celdas con espacios en blanco al inicio o final",
            'correccion_auto': True,
            'sugerencia': 'Eliminar espacios en blanco. Corrección aplicada automáticamente.',
        })
    
    # ── ERROR 20: Recibos fuera de periodo ───────────────────────────────
    if fecha_fac_col:
        dates = pd.to_datetime(df[fecha_fac_col], errors='coerce')
        # Assuming period is Jan-Feb 2026
        out_of_period = ((dates.dt.year != 2026) | ((dates.dt.month != 1) & (dates.dt.month != 2))).sum()
        if out_of_period > 0 and out_of_period < len(df):  # Don't flag if ALL are out of period
            error_id += 1
            warnings.append({
                'id': error_id, 'tipo': 'Recibos fuera de periodo', 'error_num': 20,
                'severidad': 'Baja', 'campo': fecha_fac_col,
                'cantidad': int(out_of_period),
                'detalle': f"{out_of_period} recibos con fecha fuera del periodo Enero-Febrero 2026",
                'sugerencia': 'Verificar si corresponden al cierre o son de otro periodo',
            })
    
    # ── SUMMARY ──────────────────────────────────────────────────────────
    total_errors = len(errors)
    total_warnings = len(warnings)
    total_corrections = len(corrections)
    total_records = len(df)
    
    auto_correctable = sum(1 for e in errors + corrections if e.get('correccion_auto'))
    needs_review = total_errors + total_warnings - auto_correctable
    
    return _clean({
        'filename': filename,
        'correduria': correduría,
        'data_sheet': data_sheet,
        'total_records': total_records,
        'total_columns': len(df.columns),
        'columns': list(df.columns),
        'total_errors': total_errors,
        'total_warnings': total_warnings,
        'total_corrections': total_corrections,
        'auto_correctable': auto_correctable,
        'needs_review': needs_review,
        'pct_clean': round((1 - (total_errors + total_warnings) / max(total_records, 1)) * 100, 1),
        'errors': errors,
        'warnings': warnings,
        'corrections': corrections,
        'mapping_available': bool(mapping),
        'preview': df.head(5).replace({np.nan: None}).to_dict(orient='records'),
    })


# ─── STRUCTURE DETECTION ─────────────────────────────────────────────────────

def detect_structure(file_bytes, filename):
    """Detect file structure, find data sheet, read headers correctly."""
    
    fn_upper = filename.upper()
    
    try:
        import openpyxl
        wb = openpyxl.load_workbook(BytesIO(file_bytes), read_only=True)
        sheets = wb.sheetnames
        wb.close()
    except Exception as e:
        return {'error': f'No se pudo abrir el fichero: {str(e)}'}
    
    # Determine data sheet and header row
    data_sheet = None
    header_row = 0  # 0-indexed for pandas
    mapping = {}
    correduria = None
    
    if 'SEGURETXE' in fn_upper:
        data_sheet = 'Data'
        header_row = 2
        correduria = 'Seguretxe'
        # Read mapping
        try:
            df_map = pd.read_excel(BytesIO(file_bytes), sheet_name='Mapeo Ramos')
            mapping['ramo_mapping'] = {}  # Would parse mapping here
        except:
            pass
            
    elif 'ARAYTOR' in fn_upper:
        data_sheet = 'Plantilla Report Recibos'
        header_row = 2  # Row 1=Recibos, Row 2=Obligatorio, Row 3=headers
        correduria = 'Araytor'
        try:
            df_map = pd.read_excel(BytesIO(file_bytes), sheet_name='Mapeo Ramos')
            if 'Ramo Araytor' in df_map.columns and 'Grupo Ramo SABSEG' in df_map.columns:
                mapping['ramo_mapping'] = dict(zip(df_map['Ramo Araytor'].dropna(), df_map['Grupo Ramo SABSEG'].dropna()))
        except:
            pass
            
    elif 'ZURRIOLA' in fn_upper:
        data_sheet = 'Plantilla Report Recibos'
        header_row = 2
        correduria = 'Zurriola'
        try:
            df_map = pd.read_excel(BytesIO(file_bytes), sheet_name='Mapeo')
            if 'Productos' in df_map.columns:
                mapping['ramo_mapping'] = dict(zip(df_map['Productos'].dropna(), df_map.get('Equivalencia 08/2025', pd.Series()).dropna()))
        except:
            pass
            
    elif 'ARRENTA' in fn_upper:
        # ARRENTA has different structures in different months
        if '202602' in fn_upper:
            data_sheet = '202602_ARRENTA_Recibos' if '202602_ARRENTA_Recibos' in sheets else sheets[0]
            header_row = 0
        else:
            data_sheet = 'Plantilla Report Recibos' if 'Plantilla Report Recibos' in sheets else sheets[0]
            header_row = 0
        correduria = 'Arrenta'
        try:
            map_sheet = 'Mapeo Ramos' if 'Mapeo Ramos' in sheets else None
            if map_sheet:
                df_map = pd.read_excel(BytesIO(file_bytes), sheet_name=map_sheet)
                if 'Producto' in df_map.columns and 'Grupo RAmo' in df_map.columns:
                    mapping['ramo_mapping'] = dict(zip(df_map['Producto'].dropna(), df_map['Grupo RAmo'].dropna()))
        except:
            pass
    else:
        # Generic: find the sheet with most rows
        data_sheet = sheets[0]
        header_row = 0
    
    # Read data
    try:
        df = pd.read_excel(BytesIO(file_bytes), sheet_name=data_sheet, header=header_row)
        # Clean column names
        df.columns = [str(c).strip() if pd.notna(c) else f'col_{i}' for i, c in enumerate(df.columns)]
        # Remove fully empty rows
        df = df.dropna(how='all')
    except Exception as e:
        return {'error': f'Error leyendo hoja {data_sheet}: {str(e)}'}
    
    return {
        'dataframe': df,
        'headers': list(df.columns),
        'data_sheet': data_sheet,
        'mapping': mapping,
        'correduria': correduria,
    }


# ─── ARRENTA COMPARISON ─────────────────────────────────────────────────────

def compare_arrenta_periods(file_jan_bytes, file_feb_bytes):
    """Compare Arrenta January vs February to validate accumulated data."""
    
    try:
        df_jan = pd.read_excel(BytesIO(file_jan_bytes), sheet_name=0, header=0)
        df_feb = pd.read_excel(BytesIO(file_feb_bytes), sheet_name=0, header=0)
    except Exception as e:
        return {'error': f'Error leyendo ficheros: {str(e)}'}
    
    df_jan.columns = [str(c).strip() for c in df_jan.columns]
    df_feb.columns = [str(c).strip() for c in df_feb.columns]
    
    issues = []
    
    # Check that Feb has more or equal records than Jan (accumulated)
    if len(df_feb) < len(df_jan):
        issues.append({
            'tipo': 'Menos registros en febrero',
            'severidad': 'Alta',
            'detalle': f"Febrero tiene {len(df_feb)} registros vs Enero {len(df_jan)}. Los datos deberían ser acumulados.",
        })
    
    # Check totals
    prima_col = _find_column(df_jan, ['PrimaNeta', 'Prima neta', 'Prima'])
    if prima_col and prima_col in df_feb.columns:
        total_jan = pd.to_numeric(df_jan[prima_col], errors='coerce').sum()
        total_feb = pd.to_numeric(df_feb[prima_col], errors='coerce').sum()
        
        if total_feb < total_jan:
            issues.append({
                'tipo': 'Total primas febrero menor que enero',
                'severidad': 'Alta',
                'detalle': f"Prima total Enero: {total_jan:,.2f}€, Febrero: {total_feb:,.2f}€. Los acumulados deberían crecer.",
            })
        
        diff = total_feb - total_jan
        if diff > 0 and diff < total_jan * 0.01:
            issues.append({
                'tipo': 'Incremento muy pequeño entre periodos',
                'severidad': 'Media',
                'detalle': f"Incremento de solo {diff:,.2f}€ ({diff/total_jan*100:.2f}%) entre enero y febrero. Verificar si faltan recibos.",
            })
    
    return {
        'records_jan': len(df_jan),
        'records_feb': len(df_feb),
        'issues': issues,
    }


# ─── BATCH VALIDATION ────────────────────────────────────────────────────────

def run_data_quality(files):
    """
    Validate multiple files and return combined results.
    files: list of (filename, bytes) tuples
    """
    results = []
    arrenta_files = {}
    
    for filename, file_bytes in files:
        fn_upper = filename.upper()
        
        # Collect Arrenta files for comparison
        if 'ARRENTA' in fn_upper:
            if '01' in fn_upper or 'ENE' in fn_upper or 'ENERO' in fn_upper:
                arrenta_files['jan'] = (filename, file_bytes)
            elif '02' in fn_upper or 'FEB' in fn_upper:
                arrenta_files['feb'] = (filename, file_bytes)
        
        # Validate each file
        result = validate_file(file_bytes, filename)
        results.append(result)
    
    # Arrenta comparison
    arrenta_comparison = None
    if 'jan' in arrenta_files and 'feb' in arrenta_files:
        arrenta_comparison = compare_arrenta_periods(
            arrenta_files['jan'][1], 
            arrenta_files['feb'][1]
        )
    
    # Summary
    total_records = sum(r.get('total_records', 0) for r in results if 'error' not in r)
    total_errors = sum(r.get('total_errors', 0) for r in results if 'error' not in r)
    total_warnings = sum(r.get('total_warnings', 0) for r in results if 'error' not in r)
    auto_correctable = sum(r.get('auto_correctable', 0) for r in results if 'error' not in r)
    
    return _clean({
        'fecha_analisis': datetime.now().strftime('%d/%m/%Y %H:%M'),
        'total_files': len(results),
        'total_records': total_records,
        'total_errors': total_errors,
        'total_warnings': total_warnings,
        'auto_correctable': auto_correctable,
        'needs_manual_review': total_errors + total_warnings - auto_correctable,
        'results': results,
        'arrenta_comparison': arrenta_comparison,
    })


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def _find_column(df, candidates):
    """Find a column by trying multiple name variants."""
    for c in candidates:
        if c in df.columns:
            return c
        # Try case-insensitive
        for col in df.columns:
            if str(col).lower().strip() == c.lower().strip():
                return col
    return None

def _find_columns(df, candidates):
    """Find all matching columns."""
    found = []
    for c in candidates:
        col = _find_column(df, [c])
        if col:
            found.append(col)
    return found
