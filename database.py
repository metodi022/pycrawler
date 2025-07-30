from datetime import datetime
from typing import Optional

from peewee import AutoField, BlobField, BooleanField, CharField, DatabaseProxy, DateTimeField, DeferredForeignKey, ForeignKeyField, IntegerField, Model, PostgresqlDatabase, SqliteDatabase, TextField

import utils
from config import Config


_database_proxy: DatabaseProxy = DatabaseProxy()
_database: SqliteDatabase | PostgresqlDatabase = None

def load_database() -> SqliteDatabase | PostgresqlDatabase:
    global _database

    if _database and (not _database.is_closed()) and (not _database.in_transaction()):
        _database.close()

    if _database is None:
        if Config.SQLITE:
            _database = SqliteDatabase(
                Config.SQLITE,
                pragmas={
                    'journal_mode': 'wal',
                    'busy_timeout': 10000
                }
            )
        else:
            _database = PostgresqlDatabase(
                Config.DATABASE,
                user=Config.USER,
                password=Config.PASSWORD,
                host=Config.HOST,
                port=Config.PORT,
                sslmode="prefer",
                autorollback=False,
            )

    _database_proxy.initialize(_database)

    if _database.is_closed():
        assert _database.connect(reuse_if_open=True)

    return _database


class BaseModel(Model):
    class Meta:
        database = _database_proxy

    @classmethod
    def create_table(cls, safe: bool = False, **options) -> None:
        ...

class Entity(BaseModel):
    name = CharField(primary_key=True, null=False, index=True, unique=True)
    adult = BooleanField(default=None, null=True, index=True)
    tracking = BooleanField(default=None, null=True, index=True)
    fingerprinting = BooleanField(default=None, null=True, index=True)
    malicious = BooleanField(default=None, null=True, index=True)

    @classmethod
    def create_table(cls, safe: bool = False, **options) -> None:
        database = load_database()
        if database.table_exists('entity'):
            return

        with database.atomic():
            database.execute_sql("""
                CREATE TABLE entity (
                name VARCHAR PRIMARY KEY,
                adult BOOLEAN DEFAULT NULL,
                tracking BOOLEAN DEFAULT NULL,
                fingerprinting BOOLEAN DEFAULT NULL,
                malicious BOOLEAN DEFAULT NULL);
            """)

            database.execute_sql("CREATE INDEX idx_entity_adult ON entity(adult);")
            database.execute_sql("CREATE INDEX idx_entity_tracking ON entity(tracking);")
            database.execute_sql("CREATE INDEX idx_entity_fingerprinting ON entity(fingerprinting);")
            database.execute_sql("CREATE INDEX idx_entity_malicious ON entity(malicious);")

class Site(BaseModel):
    id = AutoField()
    scheme = CharField(default='https', null=False, index=True)
    site = CharField(index=True, null=False)
    entity = ForeignKeyField(Entity, default=None, null=True, index=True)
    rank = IntegerField(default=None, null=True, index=True)
    adult = BooleanField(default=None, null=True, index=True)
    tracking = BooleanField(default=None, null=True, index=True)
    fingerprinting = BooleanField(default=None, null=True, index=True)
    malicious = BooleanField(default=None, null=True, index=True)

    @classmethod
    def create_table(cls, safe: bool = False, **options) -> None:
        database = load_database()
        if database.table_exists('site'):
            return

        with database.atomic():
            database.execute_sql(f"""
                CREATE TABLE site (
                id {"INTEGER" if Config.SQLITE is not None else "SERIAL"} PRIMARY KEY {"AUTOINCREMENT" if Config.SQLITE is not None else ""},
                scheme VARCHAR NOT NULL DEFAULT 'https',
                site VARCHAR NOT NULL,
                entity_id VARCHAR REFERENCES entity(name) DEFAULT NULL,
                rank INTEGER DEFAULT NULL,
                adult BOOLEAN DEFAULT NULL,
                tracking BOOLEAN DEFAULT NULL,
                fingerprinting BOOLEAN DEFAULT NULL,
                malicious BOOLEAN DEFAULT NULL,
                UNIQUE (scheme, site)
                );
            """)

            database.execute_sql("CREATE INDEX idx_site_entity ON site(entity_id);")
            database.execute_sql("CREATE INDEX idx_site_rank ON site(rank);")
            database.execute_sql("CREATE INDEX idx_site_adult ON site(adult);")
            database.execute_sql("CREATE INDEX idx_site_tracking ON site(tracking);")
            database.execute_sql("CREATE INDEX idx_site_fingerprinting ON site(fingerprinting);")
            database.execute_sql("CREATE INDEX idx_site_malicious ON site(malicious);")

class Task(BaseModel):
    id = AutoField()
    created = DateTimeField(default=datetime.now)
    updated = DateTimeField(default=datetime.now)
    job = CharField(index=True, null=False)
    site = ForeignKeyField(Site, index=True, null=False)
    crawler = IntegerField(default=None, null=True, index=True)
    landing = DeferredForeignKey("URL", default=None, null=True, backref='tasks', index=True)
    state = CharField(default="free", index=True)
    error = TextField(default=None, null=True)
    crawlerstate = BlobField(default=None, null=True)

    @classmethod
    def create_table(cls, safe: bool = False, **options) -> None:
        database = load_database()
        if database.table_exists('task'):
            return

        with database.atomic():
            database.execute_sql(f"""
                CREATE TABLE task (
                id {"INTEGER" if Config.SQLITE is not None else "SERIAL"} PRIMARY KEY {"AUTOINCREMENT" if Config.SQLITE is not None else ""},
                job VARCHAR NOT NULL,
                site_id INTEGER NOT NULL REFERENCES site(id),
                created TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                state VARCHAR NOT NULL DEFAULT 'free',
                crawler INTEGER DEFAULT NULL,
                landing_id INTEGER DEFAULT NULL,
                error TEXT DEFAULT NULL,
                crawlerstate {"BLOB" if Config.SQLITE is not None else "BYTEA"} DEFAULT NULL);
            """)

            database.execute_sql("CREATE INDEX idx_task_job ON task(job);")
            database.execute_sql("CREATE INDEX idx_task_site ON task(site_id);")
            database.execute_sql("CREATE INDEX idx_task_state ON task(state);")
            database.execute_sql("CREATE INDEX idx_task_crawler ON task(crawler);")
            database.execute_sql("CREATE INDEX idx_task_landing ON task(landing_id);")

class URL(BaseModel):
    id = AutoField()
    task = ForeignKeyField(Task, backref='urls', index=True, null=False)
    site = ForeignKeyField(Site, index=True, null=False)
    fromurl = ForeignKeyField("self", default=None, null=True, backref="from_url", index=True)
    redirect = ForeignKeyField("self", default=None, null=True, backref="from_url", index=True)
    url = TextField(null=True)
    urlfinal = TextField(default=None, null=True)
    depth = IntegerField(index=True, null=False)
    repetition = IntegerField(index=True, null=False)
    state = CharField(default="free", index=True, null=False)
    method = CharField(default=None, null=True, index=True)
    code = IntegerField(default=None, null=True, index=True)
    codetext = CharField(default=None, null=True, index=True)
    resource = CharField(default=None, null=True, index=True)
    content = CharField(default=None, null=True, index=True)
    referer = TextField(default=None, null=True)
    location = TextField(default=None, null=True)
    reqheaders = TextField(default=None, null=True)
    resheaders = TextField(default=None, null=True)
    metaheaders = TextField(default=None, null=True)
    reqbody = BlobField(default=None, null=True)
    resbody = BlobField(default=None, null=True)

    @classmethod
    def create_table(cls, safe: bool = False, **options) -> None:
        database = load_database()
        if database.table_exists('url'):
            return

        with database.atomic():
            database.execute_sql(f"""
                CREATE TABLE url (
                id {"INTEGER" if Config.SQLITE is not None else "SERIAL"} PRIMARY KEY {"AUTOINCREMENT" if Config.SQLITE is not None else ""},
                task_id INTEGER NOT NULL REFERENCES task(id),
                site_id INTEGER NOT NULL REFERENCES site(id),
                fromurl_id INTEGER REFERENCES url(id) DEFAULT NULL,
                redirect_id INTEGER REFERENCES url(id) DEFAULT NULL,
                url TEXT,
                urlfinal TEXT DEFAULT NULL,
                depth INTEGER NOT NULL,
                repetition INTEGER NOT NULL,
                state VARCHAR NOT NULL DEFAULT 'free',
                method VARCHAR DEFAULT NULL,
                code INTEGER DEFAULT NULL,
                codetext VARCHAR DEFAULT NULL,
                resource VARCHAR DEFAULT NULL,
                content VARCHAR DEFAULT NULL,
                referer TEXT DEFAULT NULL,
                location TEXT DEFAULT NULL,
                reqheaders TEXT DEFAULT NULL,
                resheaders TEXT DEFAULT NULL,
                metaheaders TEXT DEFAULT NULL,
                reqbody {"BLOB" if Config.SQLITE is not None else "BYTEA"} DEFAULT NULL,
                resbody {"BLOB" if Config.SQLITE is not None else "BYTEA"} DEFAULT NULL);
            """)

            database.execute_sql("CREATE INDEX idx_url_task ON url(task_id);")
            database.execute_sql("CREATE INDEX idx_url_site ON url(site_id);")
            database.execute_sql("CREATE INDEX idx_url_fromurl ON url(fromurl_id);")
            database.execute_sql("CREATE INDEX idx_url_redirect ON url(redirect_id);")
            database.execute_sql("CREATE INDEX idx_url_depth ON url(depth);")
            database.execute_sql("CREATE INDEX idx_url_repetition ON url(repetition);")
            database.execute_sql("CREATE INDEX idx_url_state ON url(state);")
            database.execute_sql("CREATE INDEX idx_url_method ON url(method);")
            database.execute_sql("CREATE INDEX idx_url_code ON url(code);")
            database.execute_sql("CREATE INDEX idx_url_codetext ON url(codetext);")
            database.execute_sql("CREATE INDEX idx_url_resource ON url(resource);")
            database.execute_sql("CREATE INDEX idx_url_content ON url(content);")

            database.execute_sql("ALTER TABLE task ADD CONSTRAINT task_landing_fk FOREIGN KEY (landing_id) REFERENCES url(id) ON DELETE SET NULL;")

class URLDB:
    def __init__(self, crawler) -> None:
        from crawler import Crawler
        self.crawler: Crawler = crawler

        self._seen: set[str] = self.crawler.state.get('URLDB', set())
        self.crawler.state['URLDB'] = self._seen

    def get_url(self, repetition: int) -> Optional[URL]:
        url: Optional[URL] = None

        query = [
            URL.task == self.crawler.task,
            URL.repetition == repetition
        ]

        if repetition == 1:
            query.append(URL.state == "free")

            if Config.BREADTHFIRST:
                query.append(URL.depth == self.crawler.depth)

            url = URL.get_or_none(*query)

            if (not url) and Config.BREADTHFIRST:
                url = URL.get_or_none(*(query[:-1]))
        else:
            query.append(URL.state == "waiting")
            query.append(URL.url == self.crawler.url.url)
            query.append(URL.depth == self.crawler.depth)
            url = URL.get_or_none(*query)

        if url:
            url.state = "progress"  # type: ignore
            url.save()

        return url

    def get_seen(self, url: str) -> bool:
        return utils.normalize_url(url) in self._seen

    def add_seen(self, url: str) -> None:
        self._seen.add(utils.normalize_url(url))

    def add_url(self, url: str, depth: int, fromurl: Optional[URL], force: bool = False) -> None:
        if (not force) and self.get_seen(url):
            return

        self.add_seen(url)

        url_parsed = utils.get_tld_object(url)
        if url_parsed is None:
            return

        site = Site.get_or_create(
            scheme=utils.get_url_scheme(url_parsed),
            site=utils.get_url_site(url_parsed)
        )[0]

        url_data = {
            "task": self.crawler.task,
            "site": site,
            "url": url,
            "fromurl": fromurl,
            "depth": depth
        }

        URL.create(**url_data, repetition=1)

        for repetition in range(2, Config.REPETITIONS + 1):
            URL.create(**url_data, repetition=repetition, state="waiting")
