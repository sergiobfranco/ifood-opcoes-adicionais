"""
Configuração do Celery para processamento em background.
Inclui configuração de tarefas agendadas (Beat).
"""

from celery import Celery
from celery.schedules import crontab
from config.settings import CELERY_BROKER_URL, CELERY_RESULT_BACKEND

celery_app = Celery(
    'ifood_pipeline',
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
    include=['tasks']
)

celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='America/Sao_Paulo',
    enable_utc=False,  # Usar timezone local
    task_track_started=True,
    task_time_limit=3600 * 6,  # 6 horas
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=10,
    beat_schedule_filename='/var/lib/celery/celerybeat-schedule',
)

# Configuração de tarefas agendadas (Celery Beat)
celery_app.conf.beat_schedule = {
    'processar-noticias-diarias': {
        'task': 'tasks.process_pipeline_scheduled',
        # Agendado para executar todo dia às 00:01 (horário de São Paulo)
        'schedule': crontab(hour=0, minute=1),
        'options': {
            'expires': 3600,  # Task expira em 1h se não executar
        }
    },
}