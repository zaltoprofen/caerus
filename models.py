from celery import current_app, schedules
from celery.beat import ScheduleEntry
from datetime import timedelta, datetime
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import sessionmaker, relationship, scoped_session
from sqlalchemy.schema import Column, ForeignKey
from sqlalchemy.types import String, Boolean, Integer, DateTime, Enum, Text
import json

__all__ = ['setup', 'CaerusEntryMeta', 'CaerusEntry', 'CronSchedule', 'IntervalSchedule']

Base = declarative_base()
session = None

# XXX: think better way to inject MetaData to caerus
def setup(metadata):
    global Base
    global session
    if isinstance(metadata, MetaData):
        metadata.tables = metadata.tables.union(Base.metadata.tables)
        Base.metadata = metadata
        session = scoped_session(sessionmaker(bind=metadata.bind, autocommit=True))
    else:
        raise TypeError('metadata is not a MetaData')


class CaerusEntryMeta(Base):
    __tablename__ = 'schedule_entry_meta'
    name = Column(String(length=64), primary_key=True)
    value = Column(String(length=128))

    @classmethod
    def get_latest_modified(cls):
        lm = session.query(cls.value).filter(cls.name=="latest_modified").first()
        if lm is None:
            return None
        return datetime.strptime(lm.value, '%Y-%m-%d %H:%M:%S')

    @classmethod
    def set_latest_modified(cls, now):
        with session.begin(subtransactions=True) as t:
            lm = session.query(cls).filter(cls.name=="latest_modified").first()
            if lm is None:
                lm = cls(name='latest_modified')
            lm.value = now.strftime('%Y-%m-%d %H:%M:%S')
            session.add(lm)


class CaerusEntry(Base, ScheduleEntry):
    __tablename__ = 'schedule_entry'
    id = Column(Integer, primary_key=True)
    name = Column(String(length=255), nullable=False, unique=True)
    task = Column(String(length=256), nullable=False)
    _args = Column('args', Text, nullable=False, default="[]")
    _kwargs = Column('kwargs', Text, nullable=False, default="{}")
    _options = Column('options', Text, nullable=False, default="{}")
    relative = Column(Boolean, nullable=False, default=False)
    enable = Column(Boolean, nullable=False, default=True)
    last_run_at = Column(DateTime)
    _schedule = relationship('Schedule', uselist=False, backref='task')

    def __init__(self, **kwargs):
        super(CaerusEntry, self).__init__()
        attrs = self.__dict__.keys()
        self.app = kwargs.pop('app', current_app._get_current_object())

        for akey, aval in kwargs.items():
            setattr(self, akey, aval)

        if self.last_run_at is None:
            self.last_run_at = self._default_now()
 
    def get_args(self):
        return json.loads(self._args) or []
    def set_args(self, value):
        self._args = json.dumps(value)
    args = property(get_args, set_args)

    def get_kwargs(self):
        return json.loads(self._kwargs) or {}
    def set_kwargs(self, value):
        self._kwargs = json.dumps(value)
    kwargs = property(get_kwargs, set_kwargs)

    def get_options(self):
        return json.loads(self._options) or {}
    def set_options(self, value):
        self._options = json.dumps(value)
    options = property(get_options, set_options)

    def get_schedule(self):
        if self._schedule:
            return self._schedule.schedule
        else:
            return None
    def set_schedule(self, value):
        if self._schedule:
            self._schedule.delete()
        if isinstance(value, schedules.crontab):
            self._schedule = CronSchedule.build(value)
        elif isinstance(value, schedules.schedule):
            self._schedule = IntervalSchedule.build(value)
        else:
            raise TypeError
    schedule = property(get_schedule, set_schedule)

    def is_due(self):
        if not self.enable:
            return False, 5.0
        return self.schedule.is_due(self.last_run_at)

    def __next__(self):
        with session.begin() as t:
            self.last_run_at = self._default_now()
            session.add(self)
        return self
    next = __next__

    @classmethod
    def get_latest_modified(cls):
        return CaerusEntryMeta.get_latest_modified()


class Schedule(Base):
    __tablename__ = 'schedule'

    id = Column(Integer, ForeignKey('schedule_entry.id', onupdate='CASCADE', ondelete='CASCADE'), primary_key=True)
    type_ = Column(Enum('cron', 'interval'), nullable=False)

    __mapper_args__ = {'polymorphic_on':type_}

    def delete(self):
        session.delete(self)


class CronSchedule(Schedule):
    __tablename__ = 'cron_schedule'
    __mapper_args__ = {'polymorphic_identity': 'cron'}

    id = Column(Integer, ForeignKey('schedule.id', onupdate='CASCADE', ondelete='CASCADE'), primary_key=True)
    minute = Column(String(length=32), default='*')
    hour = Column(String(length=32), default='*')
    day_of_week = Column(String(length=32), default='*')
    day_of_month = Column(String(length=32), default='*')
    month_of_year = Column(String(length=32), default='*')

    @classmethod
    def build(cls, crontab):
        return cls(minute=crontab._orig_minute,
                   hour=crontab._orig_hour,
                   day_of_week=crontab._orig_day_of_week,
                   day_of_month=crontab._orig_day_of_month,
                   month_of_year=crontab._orig_month_of_year)

    @property
    def schedule(self):
        return schedules.crontab(
                minute=self.minute,
                hour=self.hour,
                day_of_week=self.day_of_week,
                day_of_month=self.day_of_month,
                month_of_year=self.month_of_year)
    

class IntervalSchedule(Schedule):
    __tablename__ = 'interval_schedule'
    __mapper_args__ = {'polymorphic_identity': 'interval'}

    enum_units = Enum(
        'days', 'seconds',
        'microseconds', 'milliseconds',
        'minutes', 'hours', 'weeks')

    id = Column(Integer, ForeignKey('schedule.id', onupdate='CASCADE', ondelete='CASCADE'), primary_key=True)
    unit = Column(enum_units, nullable=False)
    value = Column(Integer, nullable=False)

    @classmethod
    def build(cls, schedule):
        return cls(unit='seconds', value=schedule.seconds)

    @property
    def schedule(self):
        return schedules.schedule(timedelta(**{self.unit: self.value}))
