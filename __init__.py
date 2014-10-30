from .scheduler import *
from .models import *
from . import models
from celery import current_app, schedules

def register_task(name, task, schedule, args=[], kwargs={}, relative=False, options={}, celery_app=None):
    if not isinstance(schedule, (schedules.crontab, schedules.schedule)):
        raise TypeError
    if celery_app is None:
        celery_app = current_app._get_current_object()
    if task not in celery_app.tasks:
        raise ValueError('task \'%s\' is not in %s' % (task, celery_app))

    models.session.begin()
    try:
        entry = CaerusEntry(
                name=name,
                task=task,
                args=args,
                kwargs=kwargs,
                relative=relative,
                options=options,
                schedule=schedule)
        models.session.add(entry)
        CaerusEntryMeta.set_latest_modified(celery_app.now())
        models.session.commit()
    except Exception, e:
        models.session.rollback()
        raise
