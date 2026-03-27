"""
Motor de Correcciones Automáticas + Generador de Informes
=========================================================
Aplica correcciones a los ficheros y genera informes para corredurías.
"""

import pandas as pd
import numpy as np
import re
import io
from datetime import datetime
from data_quality import detect_structure, validate_nif, NIF_LETTER_TABLE, _find_column, _find_columns, _clean


def apply_corrections(file_bytes, filename):
    """
    Apply all auto-fixable corrections to a file.
    Returns: {
        'corrected_file': bytes (Excel),
        'corrections_applied': list of corrections,
        'remaining_issues': list of issues that couldn't be fixed,
        'stats': summary stats
    }
    """
    
    file_info = detect_structure(file_bytes, filename)
    if 'error' in file_info:
        return file_info
    
    df = file_info['dataframe'].copy()
    mapping = file_info.get('mapping', {})
    correduria = file_info.get('correduria', filename)
    
    corrections_applied = []
    remaining_issues = []
    
    # ── FIX 19: Trim whitespace from all string columns ──────────────────
    space_fixes = 0
    for col in df.columns:
        if df[col].dtype == object:
            original = df[col].copy()
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace('nan', pd.NA)
            changed = (original.astype(str) != df[col].astype(str)).sum()
            space_fixes += changed
    
    if space_fixes > 0:
        corrections_applied.append({
            'tipo': 'Espacios en blanco eliminados',
            'campo': 'Todos los campos de texto',
            'cantidad': int(space_fixes),
            'detalle': f'Se eliminaron espacios al inicio/final en {space_fixes} celdas.',
        })
    
    # ── FIX 7: Empty NIFs → generic NIF ──────────────────────────────────
    nif_col = _find_column(df, ['NIF', 'Nif', 'nif', 'CIF'])
    nif_fixes_empty = 0
    nif_fixes_format = 0
    nif_unfixable = []
    
    if nif_col:
        for idx in df.index:
            nif_val = df.at[idx, nif_col]
            is_valid, error_type, suggestion = validate_nif(nif_val)
            
            if not is_valid:
                if error_type == 'vacio':
                    df.at[idx, nif_col] = 'X0000000T'  # Generic NIF
                    nif_fixes_empty += 1
                    
                elif error_type == 'caracteres_especiales':
                    cleaned = re.sub(r'[\s\.\-,/]', '', str(nif_val).strip())
                    is_valid2, _, _ = validate_nif(cleaned)
                    if is_valid2:
                        df.at[idx, nif_col] = cleaned.upper()
                        nif_fixes_format += 1
                    else:
                        df.at[idx, nif_col] = 'X0000000T'
                        nif_fixes_format += 1
                        
                elif error_type == 'sin_letra':
                    num = str(nif_val).strip()
                    if num.isdigit() and len(num) == 8:
                        letter = NIF_LETTER_TABLE[int(num) % 23]
                        df.at[idx, nif_col] = f'{num}{letter}'
                        nif_fixes_format += 1
                    else:
                        nif_unfixable.append({'row': idx, 'value': str(nif_val), 'reason': suggestion or 'Formato no reconocido'})
                        
                elif error_type == 'letra_incorrecta' or error_type == 'nie_letra_incorrecta':
                    # Recalculate correct letter
                    nif_str = str(nif_val).strip().upper()
                    if suggestion and ':' in suggestion:
                        corrected = suggestion.split(':')[-1].strip()
                        df.at[idx, nif_col] = corrected
                        nif_fixes_format += 1
                    else:
                        nif_unfixable.append({'row': idx, 'value': str(nif_val), 'reason': suggestion or error_type})
                else:
                    nif_unfixable.append({'row': idx, 'value': str(nif_val), 'reason': suggestion or error_type})
        
        if nif_fixes_empty > 0:
            corrections_applied.append({
                'tipo': 'NIFs vacíos → NIF genérico',
                'campo': nif_col,
                'cantidad': nif_fixes_empty,
                'detalle': f'{nif_fixes_empty} NIFs vacíos reemplazados por NIF genérico (X0000000T).',
            })
        
        if nif_fixes_format > 0:
            corrections_applied.append({
                'tipo': 'NIFs corregidos',
                'campo': nif_col,
                'cantidad': nif_fixes_format,
                'detalle': f'{nif_fixes_format} NIFs corregidos (caracteres especiales, letra calculada).',
            })
        
        if nif_unfixable:
            remaining_issues.append({
                'tipo': 'NIFs no corregibles',
                'campo': nif_col,
                'cantidad': len(nif_unfixable),
                'detalle': f'{len(nif_unfixable)} NIFs que requieren revisión manual.',
                'ejemplos': nif_unfixable[:10],
            })
    
    # ── FIX 9: Numeric format (dots as thousands separator) ──────────────
    numeric_cols = _find_columns(df, ['PrimaNeta', 'Prima', 'ComisionCorreduria', 'ComisionPrimaNeta',
                                       'ComisionComplementaria', 'ComisionColaborador1', 'Prima neta',
                                       'Com, Bruta', 'Com,Auxi,1'])
    num_fixes = 0
    for col in numeric_cols:
        for idx in df.index:
            val = str(df.at[idx, col])
            # Pattern: 1.234,56 (Spanish format with dot as thousands)
            if re.match(r'^\d{1,3}\.\d{3}', val):
                cleaned = val.replace('.', '').replace(',', '.')
                try:
                    df.at[idx, col] = float(cleaned)
                    num_fixes += 1
                except:
                    pass
    
    if num_fixes > 0:
        corrections_applied.append({
            'tipo': 'Formato numérico corregido',
            'campo': 'Campos numéricos',
            'cantidad': num_fixes,
            'detalle': f'{num_fixes} valores corregidos (puntos como separador de miles → formato numérico estándar).',
        })
    
    # ── FIX 11: Commission mismatch CB = CPN + CC ────────────────────────
    cb_col = _find_column(df, ['ComisionCorreduria', 'Com, Bruta', 'Comisión Bruta'])
    cpn_col = _find_column(df, ['ComisionPrimaNeta', 'Comisión prima neta', 'Comisión Prima Neta'])
    cc_col = _find_column(df, ['ComisionComplementaria', 'Comisión complementaria'])
    
    com_fixes = 0
    if cb_col and cpn_col and cc_col:
        cb = pd.to_numeric(df[cb_col], errors='coerce').fillna(0)
        cpn = pd.to_numeric(df[cpn_col], errors='coerce').fillna(0)
        cc = pd.to_numeric(df[cc_col], errors='coerce').fillna(0)
        diff = abs(cb - (cpn + cc))
        mask = diff > 0.01
        
        # Recalculate CB = CPN + CC
        df.loc[mask, cb_col] = cpn[mask] + cc[mask]
        com_fixes = int(mask.sum())
        
        if com_fixes > 0:
            corrections_applied.append({
                'tipo': 'Comisión Bruta recalculada',
                'campo': cb_col,
                'cantidad': com_fixes,
                'detalle': f'{com_fixes} registros donde se recalculó Comisión Bruta = Com. Prima Neta + Com. Complementaria.',
            })
    
    # ── FIX 15: Date format standardization ──────────────────────────────
    date_cols = _find_columns(df, ['FechaFacturacion', 'FechaEfecto', 'FechaCobro', 
                                    'FechaAnulacion', 'FechaDevolucion',
                                    'F. Producc.', 'F. Efecto'])
    date_fixes = 0
    date_unfixable = []
    for dcol in date_cols:
        for idx in df.index:
            val = df.at[idx, dcol]
            if pd.isna(val) or val is None or str(val).strip() == '':
                continue
            
            # Try to parse
            parsed = pd.to_datetime(val, errors='coerce')
            if pd.isna(parsed):
                # Try common non-standard formats
                val_str = str(val).strip()
                for fmt in ['%d-%m-%Y', '%Y/%m/%d', '%d.%m.%Y', '%m/%d/%Y']:
                    try:
                        parsed = datetime.strptime(val_str, fmt)
                        df.at[idx, dcol] = parsed
                        date_fixes += 1
                        break
                    except:
                        continue
                
                if pd.isna(parsed) if isinstance(parsed, type(pd.NaT)) else parsed is None:
                    date_unfixable.append({'row': idx, 'campo': dcol, 'value': str(val)})
    
    if date_fixes > 0:
        corrections_applied.append({
            'tipo': 'Formato de fecha estandarizado',
            'campo': 'Campos de fecha',
            'cantidad': date_fixes,
            'detalle': f'{date_fixes} fechas convertidas a formato estándar.',
        })
    
    if date_unfixable:
        remaining_issues.append({
            'tipo': 'Fechas no interpretables',
            'campo': 'Campos de fecha',
            'cantidad': len(date_unfixable),
            'detalle': f'{len(date_unfixable)} fechas que no se pudieron interpretar.',
            'ejemplos': date_unfixable[:10],
        })
    
    # ── Generate corrected Excel ─────────────────────────────────────────
    output = io.BytesIO()
    df.to_excel(output, index=False, engine='openpyxl')
    corrected_bytes = output.getvalue()
    
    total_corrections = sum(c['cantidad'] for c in corrections_applied)
    total_remaining = sum(r['cantidad'] for r in remaining_issues)
    
    return _clean({
        'filename': filename,
        'correduria': correduria,
        'total_records': len(df),
        'total_corrections': total_corrections,
        'total_remaining': total_remaining,
        'corrections_applied': corrections_applied,
        'remaining_issues': remaining_issues,
        'corrected_file': corrected_bytes,
    })


def generate_broker_report(validation_result):
    """
    Generate a report/email text for the broker based on validation issues.
    Takes the output of validate_file() and produces structured text.
    """
    
    if 'error' in validation_result:
        return {'error': validation_result['error']}
    
    correduria = validation_result.get('correduria', 'la correduría')
    filename = validation_result.get('filename', '')
    
    # Collect issues that need broker attention
    broker_issues = []
    
    all_issues = validation_result.get('errors', []) + validation_result.get('warnings', [])
    
    for issue in all_issues:
        if issue.get('para_correo'):
            broker_issues.append({
                'asunto': issue['tipo'],
                'texto': issue['para_correo'],
                'severidad': issue['severidad'],
                'cantidad': issue['cantidad'],
            })
        elif not issue.get('correccion_auto'):
            # Issues that can't be auto-fixed need broker attention
            broker_issues.append({
                'asunto': issue['tipo'],
                'texto': f"Hemos detectado {issue['cantidad']} caso(s) de '{issue['tipo']}' en el campo '{issue.get('campo', 'varios')}'. {issue.get('detalle', '')} {issue.get('sugerencia', '')}",
                'severidad': issue['severidad'],
                'cantidad': issue['cantidad'],
            })
    
    if not broker_issues:
        return {
            'correduria': correduria,
            'tiene_incidencias': False,
            'asunto_email': f'Fichero de recibos {filename} - Validado correctamente',
            'cuerpo_email': f'Hola,\n\nHemos revisado el fichero de recibos y no se han encontrado incidencias que requieran vuestra atención. Todos los datos son correctos.\n\nUn saludo.',
            'incidencias': [],
        }
    
    # Build email
    asunto = f'Fichero de recibos {filename} - {len(broker_issues)} incidencia(s) detectada(s)'
    
    lineas = [
        f'Hola,',
        f'',
        f'Hemos revisado el fichero de recibos que nos enviaste y hemos detectado las siguientes incidencias que necesitamos que reviseis:',
        f'',
    ]
    
    for i, issue in enumerate(broker_issues, 1):
        lineas.append(f'{i}. {issue["asunto"]} ({issue["cantidad"]} caso(s)):')
        lineas.append(f'   {issue["texto"]}')
        lineas.append(f'')
    
    lineas.extend([
        f'Os agradeceríamos que revisarais estos puntos y nos confirmarais o corrigierais los datos.',
        f'',
        f'Quedamos a vuestra disposición para cualquier aclaración.',
        f'',
        f'Un saludo.',
    ])
    
    cuerpo = '\n'.join(lineas)
    
    return {
        'correduria': correduria,
        'tiene_incidencias': True,
        'total_incidencias': len(broker_issues),
        'asunto_email': asunto,
        'cuerpo_email': cuerpo,
        'incidencias': broker_issues,
    }


def generate_all_reports(validation_results):
    """Generate reports for all files."""
    reports = []
    for result in validation_results:
        if 'error' not in result:
            report = generate_broker_report(result)
            reports.append(report)
    return reports
