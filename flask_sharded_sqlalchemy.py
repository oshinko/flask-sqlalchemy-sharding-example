import re

from flask_sqlalchemy import Lock, Model, SQLAlchemy, _include_sqlalchemy
from sqlalchemy import orm
from sqlalchemy.ext.horizontal_shard import ShardedQuery, ShardedSession


class BindKeyPattern:
    def __init__(self, pattern):
        self.pattern = re.compile(pattern)

    def __eq__(self, other):
        try:
            return self.pattern.match(other)
        except TypeError:
            return NotImplemented


class ShardedSQLAlchemy(SQLAlchemy):
    __default_database_bind_key__ = '__default__'

    def __init__(self, app=None, use_native_unicode=True, session_options=None,
                 metadata=None, query_class=ShardedQuery, model_class=Model,
                 engine_options=None):

        self.use_native_unicode = use_native_unicode
        self.Query = query_class
        self.Model = self.make_declarative_base(model_class, metadata)
        self._engine_lock = Lock()
        self.app = app
        self._engine_options = engine_options or {}
        _include_sqlalchemy(self, query_class)

        if app is not None:
            self.init_app(app)

        self.session = self.create_scoped_session(session_options)

    def _get_binds_contains_default(self, app=None):
        if not app:
            app = self.get_app()
        default = app.config.get('SQLALCHEMY_DATABASE_URI')
        binds = app.config.get('SQLALCHEMY_BINDS', {}).copy()
        assert default or binds
        if default:
            binds.update({self.__default_database_bind_key__: default})
        return binds

    def _shard_chooser(self, mapper, instance, clause=None):
        if instance:
            binds = self._get_binds_contains_default()
            if hasattr(instance.__class__, '__bind_key__'):
                bind_key = instance.__bind_key__
            else:
                bind_key = self.__default_database_bind_key__
            matched_binds = {k: v for k, v in binds.items() if k == bind_key}
            shard_keys = sorted(matched_binds.keys())
            _, ident, _ = mapper.identity_key_from_instance(instance)
            if hasattr(instance.__class__, '__hash_id__'):
                h = instance.__class__.__hash_id__(ident)
                r = shard_keys[h % len(shard_keys)]
            else:
                assert len(shard_keys) == 1
                r = shard_keys[0]
            return r

    def _id_chooser(self, query, ident):
        binds = self._get_binds_contains_default()
        if len(query.column_descriptions) == 1:
            column_description, = query.column_descriptions
            t = column_description['type']
            if hasattr(t, '__bind_key__'):
                bind_key = t.__bind_key__
            else:
                bind_key = self.__default_database_bind_key__
            matched_binds = {k: v for k, v in binds.items() if k == bind_key}
            shard_keys = sorted(matched_binds.keys())
            if hasattr(t, '__hash_id__'):
                h = t.__hash_id__(ident)
                r = [shard_keys[h % len(shard_keys)]]
            else:
                r = shard_keys
        else:
            r = sorted(binds.keys())
        return r

    def _query_chooser(self, query):
        return self._get_binds_contains_default().keys()

    def shard_chooser(self, fn):
        self._shard_chooser = fn
        return fn

    def id_chooser(self, fn):
        self._id_chooser = fn
        return fn

    def query_chooser(self, fn):
        self._query_chooser = fn
        return fn

    def create_session(self, options):
        """Override.
        """
        app = self.get_app()
        binds = self._get_binds_contains_default(app)
        shards = {}
        for key in binds.keys():
            if key == self.__default_database_bind_key__:
                shards[key] = self.get_engine(app, None)
            else:
                shards[key] = self.get_engine(app, key)
        options = options.copy()
        options.update({
            'shards': shards,
            'shard_chooser': self._shard_chooser,
            'id_chooser': self._id_chooser,
            'query_chooser': self._query_chooser
        })
        return orm.sessionmaker(class_=ShardedSession, **options)
