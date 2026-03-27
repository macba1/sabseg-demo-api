"""
Agent Activity Logger
======================
Captura la actividad real de cada agente durante el procesamiento.
Cada entrada tiene: timestamp, agente, acción, datos reales.
Se devuelve al frontend para mostrar en el panel de agentes.
"""

import time
from datetime import datetime


class AgentLogger:
    """Singleton logger that captures agent activity."""
    
    def __init__(self):
        self.entries = []
        self.start_time = None
    
    def start(self, context):
        self.entries = []
        self.start_time = time.time()
        self.log('orquestador', 'inicio', f'Iniciando {context}...', icon='🔵')
    
    def log(self, agent, action, detail, data=None, icon='🟢'):
        elapsed = round(time.time() - self.start_time, 2) if self.start_time else 0
        entry = {
            'timestamp': elapsed,
            'agent': agent,
            'agent_label': AGENT_LABELS.get(agent, agent),
            'action': action,
            'detail': detail,
            'icon': icon,
        }
        if data:
            entry['data'] = data
        self.entries.append(entry)
    
    def finish(self, summary):
        self.log('orquestador', 'completado', summary, icon='✅')
        return self.entries
    
    def get_entries(self):
        return self.entries


AGENT_LABELS = {
    'orquestador': 'Orquestador',
    'ingestor': 'Agente Ingestor',
    'detector': 'Agente Detector de Esquema',
    'matching': 'Agente Matching',
    'clasificador': 'Agente Clasificador',
    'validador_estructura': 'Agente Validador Estructura',
    'validador_datos': 'Agente Validador Datos',
    'explicador': 'Agente Explicador',
    'corrector': 'Agente Corrector',
    'qa': 'Agente QA',
    'informe': 'Agente Generador Informe',
}


# Global logger instance
_current_logger = None

def get_logger():
    global _current_logger
    if _current_logger is None:
        _current_logger = AgentLogger()
    return _current_logger

def new_logger():
    global _current_logger
    _current_logger = AgentLogger()
    return _current_logger
