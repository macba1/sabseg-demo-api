# SABSEG — Plataforma de Inteligencia Operativa

## Descripcion general

Plataforma B2B SaaS para aseguradoras y corredurias de seguros en Espana. Automatiza la reconciliacion contable y la validacion de calidad de datos de recibos de corredurias.

Dos modulos principales:
1. **Reconciliacion Contable** — Cruza saldos contables (Business Central) contra estadisticas de venta de cada correduria, detectando discrepancias por empresa/mes/cuenta.
2. **Data Quality** — Valida ficheros de recibos de corredurias, detecta 20+ tipos de error, aplica correcciones automaticas y genera informes para las corredurias.

---

## Arquitectura

```
┌─────────────────────┐     ┌──────────────────────────┐
│   Frontend (React)  │────▶│   Backend (FastAPI)       │
│   Vercel / Vite     │     │   Railway                 │
│   puerto 5173 (dev) │     │   puerto 8000             │
└─────────────────────┘     └──────────────────────────┘
```

**Repositorios:**
- Frontend: `https://github.com/macba1/sabseg-demo-app.git`
- Backend: `https://github.com/macba1/sabseg-demo-api.git`

**URL de produccion del backend:** `https://web-production-3f46b.up.railway.app`

---

## Backend

### Stack
- Python 3.11+
- FastAPI 0.115
- Pandas 2.2 + OpenPyXL 3.1
- Desplegado en Railway (Nixpacks)

### Modulos

| Modulo | Descripcion |
|--------|-------------|
| `main.py` | Servidor FastAPI con 27 endpoints; orquesta todos los flujos |
| `reconciliation_sabseg.py` | Motor de reconciliacion real: saldos contables vs estadisticas de venta, multi-empresa/mes |
| `data_quality.py` | Valida 20+ tipos de error en ficheros de recibos; validacion de NIF; deteccion de estructura |
| `logged_processing.py` | Wrappers que capturan logs de actividad de agentes para el frontend |
| `corrections.py` | Aplica correcciones automaticas (NIF, formatos, comisiones); genera informes por correo para corredurias |
| `qa_agents.py` | Orquestador QA con 6 agentes especializados; genera puntuaciones de confianza |
| `audit_report.py` | Verificacion de auditoria: compara datos extraidos vs ficheros fuente; spot checks |
| `detailed_log.py` | Log fila-por-fila de errores y correcciones; exportable a Excel |
| `agent_logger.py` | Captura actividad en tiempo real de cada agente durante el procesamiento |
| `json_cleaner.py` | Convierte tipos no serializables (numpy, pandas, datetime) a JSON seguro |
| `normalization.py` | Stub de normalizacion de datos (pendiente de implementar) |
| `reconciliation.py` | Stub de reconciliacion generica para demo sintetica |

### Endpoints principales

#### Reconciliacion
| Metodo | Ruta | Descripcion |
|--------|------|-------------|
| POST | `/api/demo-sabseg-logged` | Demo reconciliacion con ficheros precargados + log de agentes |
| POST | `/api/reconcile-logged` | Reconciliacion con ficheros subidos por el usuario (FormData) |
| POST | `/api/export-reconciliation` | Exporta resultados de reconciliacion como Excel |
| POST | `/api/resolve-reconciliation` | Registra decisiones de cierre (aprobar/ajustar/investigar) |

#### Data Quality
| Metodo | Ruta | Descripcion |
|--------|------|-------------|
| POST | `/api/data-quality-demo-logged` | Demo validacion con ficheros piloto + log de agentes |
| POST | `/api/data-quality-logged` | Validacion con ficheros subidos (FormData) |
| POST | `/api/apply-corrections-demo` | Aplica correcciones automaticas a ficheros piloto |
| POST | `/api/generate-reports-demo` | Genera informes/emails para corredurias |
| POST | `/api/detailed-log-demo` | Log detallado fila-por-fila (JSON) |
| POST | `/api/download-log-demo` | Descarga log detallado como fichero Excel |

#### Otros
| Metodo | Ruta | Descripcion |
|--------|------|-------------|
| GET | `/` | Health check |
| GET | `/health` | Estado del servicio |
| GET | `/api/audit-report` | Informe de auditoria manual |

### Tipos de error detectados (Data Quality)

| # | Tipo | Descripcion |
|---|------|-------------|
| 1 | Producto no mapeado | Producto sin correspondencia con ramo SABSEG |
| 2 | Compania no reconocida | Nombre de compania no identificado |
| 5 | Valores no normalizados | Campo texto contiene valores numericos |
| 6 | Info en campo equivocado | Fracciones de pago en campo de duracion |
| 7 | NIF vacio | Campo NIF sin datos |
| 8 | NIF invalido | Formato de NIF/CIF/NIE incorrecto |
| 9 | Formato numerico | Separador de miles incorrecto |
| 11 | Comision incoherente | CB != CPN + CC |
| 12 | Comision baja | Comision <1% de prima |
| 15 | Fecha no interpretable | Formato de fecha no reconocido |
| 17 | Recibos duplicados | Misma poliza + recibo + fecha sin anular |
| 18 | Fecha anulacion incoherente | Anulacion anterior a emision |
| 19 | Espacios en blanco | Espacios al inicio/final de campos |

---

## Frontend

### Stack
- React 18.3
- Vite 5.4
- SheetJS (xlsx) para generacion de Excel en cliente
- Sin libreria de UI externa — todo custom styled components
- Desplegado en Vercel (inferido)

### Componentes

| Componente | Ruta | Descripcion |
|------------|------|-------------|
| `App.jsx` | `src/` | Shell principal; gestiona pantallas, navegacion, llamadas API |
| `LoginScreen.jsx` | `components/` | Pantalla de login (password: `sabseg2026`, sessionStorage) |
| `Sidebar.jsx` | `components/` | Sidebar izquierda fija con logo SABSEG y navegacion de modulos |
| `TopBar.jsx` | `components/` | Barra superior con breadcrumbs y badge "Sistema de Agentes v1.0" |
| `RecWorkspace.jsx` | `components/rec/` | Repositorio de ficheros para reconciliacion (drag & drop + demo) |
| `RecResults.jsx` | `components/rec/` | Resultados de reconciliacion: KPIs, tabla, cierre de partidas, Excel |
| `DQWorkspace.jsx` | `components/dq/` | Repositorio de ficheros para Data Quality (drag & drop + demo) |
| `DQResults.jsx` | `components/dq/` | Resultados DQ: KPIs, cards por correduria, correcciones, informes |
| `AgentPanel.jsx` | `components/shared/` | Stream de actividad de agentes en tiempo real (estilo log de servidor) |
| `FileRepository.jsx` | `components/shared/` | Componente compartido de upload: drag & drop, input file, demo files |
| `StatusBadge.jsx` | `components/shared/` | Badges de estado (cuadrado/warning/discrepancia) |
| `SeverityBadge.jsx` | `components/shared/` | Badges de severidad (error/warning/auto-fix) |
| `ErrorBanner.jsx` | `components/shared/` | Banner de errores dismissable |
| `ProcessingSpinner.jsx` | `components/shared/` | Indicador de carga con animacion |

### Flujo de pantallas

```
Login → [Sidebar siempre visible]
         ├── Reconciliacion
         │   ├── rec-workspace (repositorio de ficheros)
         │   └── rec-processing (AgentPanel → resultados → cierre)
         └── Data Quality
             ├── dq-workspace (repositorio de ficheros)
             └── dq-processing (AgentPanel → resultados → correcciones)
```

### Flujo del AgentPanel

1. Usuario pulsa "Ejecutar" → pantalla cambia a processing
2. Se muestra AgentPanel vacio con "Procesando..."
3. Mientras el API trabaja: lineas simuladas cada 1.5s (Coordinador, Analista, etc.)
4. API responde con `agent_log` real → se borran simuladas → stream rapido a 120ms/linea
5. Al terminar: linea de QA completion + resultados aparecen debajo

### Flujo de Reconciliacion (cierre)

1. Resultados se muestran en tabla con KPIs (Pendiente 705, Pendiente 623)
2. Cada fila tiene botones "Cerrar" (verde) y "Pendiente" (naranja)
3. Barra de progreso: "Cierre: X/82 partidas resueltas (Y%)"
4. Boton "Cerrar todas las cuadradas" marca match+warning de golpe
5. KPIs se recalculan dinamicamente (useMemo) al cerrar partidas
6. "Descargar informe de cierre (Excel)" genera Excel en frontend con SheetJS:
   - Pestana "Resumen": fecha, periodo, conteos, % completado
   - Pestana "Detalle": todas las partidas con columna "Decision"

### Flujo de Data Quality

1. Resultados con KPIs (errores, warnings, auto-fix por tipo, no por registro)
2. Cards expandibles por correduria con issues detallados
3. Acciones:
   - "Aplicar correcciones automaticas" → POST /api/apply-corrections-demo
   - "Generar informes para corredurias" → POST /api/generate-reports-demo
   - "Descargar log detallado (Excel)" → POST /api/download-log-demo

---

## Paleta de colores

| Token | Valor | Uso |
|-------|-------|-----|
| Navy | `#1B2A4A` | Sidebar, textos principales |
| Naranja | `#E8721A` | Botones primarios, acentos, badges activos |
| Fondo principal | `#F7F8FA` | Background del area de trabajo |
| Fondo cards | `#FFFFFF` | Cards y tablas |
| AgentPanel | `#0F172A` | Fondo del stream de actividad |
| Success | `#059669` | Estados positivos, cerrada |
| Warning | `#D97706` | Advertencias, pendiente |
| Error | `#DC2626` | Errores, discrepancias |

### Colores de agentes (AgentPanel)
| Agente | Color |
|--------|-------|
| Coordinador de procesos | `#94A3B8` |
| Analista de datos | `#3B82F6` |
| Especialista en esquemas | `#14B8A6` |
| Analista de conciliacion | `#6366F1` |
| Inspector de calidad | `#F59E0B` |
| Auditor de resultados | `#10B981` |

---

## Variables de entorno

| Variable | Default | Descripcion |
|----------|---------|-------------|
| `VITE_API_URL` | `http://localhost:8000` | URL del backend (frontend) |
| `PORT` | `8000` | Puerto del servidor (backend, Railway) |

---

## Desarrollo local

```bash
# Backend
cd ~/Desktop/sabseg-demo/backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8000

# Frontend
cd ~/Desktop/sabseg-demo/frontend
npm install
npm run dev
```

---

## Ficheros de datos (backend/data/)

Ficheros de prueba precargados para los endpoints de demo:

**Reconciliacion:**
- `Saldos_Contables_Ene_y_Feb_2026.xlsx` — Saldos contables de Business Central
- `2026_02_*.xlsx` — Estadisticas de venta por correduria (Feb 2026)
- `02_2026_*.xlsx` — Estadisticas agropecuarias (Feb 2026)
- `MAURA.pdf` — Estadistica en formato PDF

**Data Quality (ficheros piloto):**
- `PILOT_202602_Araytor.xlsx`
- `PILOT_202602_Zurriola.xlsx`
- `PILOT_2026_02_SEGURETXE.xlsx`
- `PILOT_2026_01_ARRENTA.xlsx`
- `PILOT_202602_ARRENTA.xlsx`

---

## Autenticacion

Login simple con password hardcodeada en el frontend:
- Password: `sabseg2026`
- Persistencia: `sessionStorage` (sobrevive refresh, no sobrevive cerrar pestana)
- Sin backend de auth — solo gate visual para demos

---

## Notas tecnicas

- **JSON serialization**: Todos los endpoints usan `clean_for_json()` antes de devolver `JSONResponse` para evitar crashes con tipos numpy/pandas/datetime.
- **pd.NA bug**: `validate_nif()` requiere check explicito `nif_str is pd.NA` antes de cualquier operacion booleana, porque `not pd.NA` lanza `TypeError`.
- **rowKey consistency**: En RecResults, `rowKey` debe devolver siempre `String()` porque las claves de objetos JS son strings pero `r.id` del backend es number.
- **KPI reactivity**: Los KPIs de "Pendiente 705/623" usan `useMemo` con `closedKeys` (derivado de `decisions` state) como dependencia.
