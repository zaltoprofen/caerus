from .models import CaerusEntry, CaerusEntryMeta
from . import models
from celery.beat import Scheduler

__all__ = ['CaerusScheduler']


class CaerusScheduler(Scheduler):
    Entry = CaerusEntry

    def __init__(self, *args, **kwargs):
        self.last_read_entries = None
        super(CaerusScheduler, self).__init__(*args, **kwargs)

    def setup_schedule(self):
        self._schedule = self.read_entries()

    def schedule_changed(self):
        if self.last_read_entries is None:
            return True
        latest_modified = self.Entry.get_latest_modified()
        if latest_modified is None:
            return True
        return self.Entry.get_latest_modified() > self.last_read_entries

    def read_entries(self):
        s = {}
        for entry in models.session.query(CaerusEntry).filter(CaerusEntry.enable):
            s[entry.name] = entry
        self.last_read_entries = self.app.now()
        return s

    @property
    def schedule(self):
        if self.last_read_entries is None or self.schedule_changed():
            self._schedule = self.read_entries()
        return self._schedule
