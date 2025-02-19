import json
import traceback

from peewee import ProgrammingError

import utils
from config import Config
from database import URL, Entity, Site, Task, database


def _load_disconnect():
    # Load disconnect entities
    try:
        with open('disconnect-tracking-protection/services.json', 'r', encoding='utf-8') as file:
            entities = json.load(file)['categories']
    except Exception as error:
        print('prepare_database.py:%s %s', traceback.extract_stack()[-1].lineno, error)
        return

    # Fingerprinting
    with database.atomic():
        for entity in entities['FingerprintingInvasive']:
            entity, sites = next(iter(entity.items()))
            site, sites = next(iter(sites.items()))
            sites = set(sites)
            try:
                sites.add(utils.get_url_site(utils.get_tld_object(site)))
            except Exception:
                pass

            entity = Entity.get_or_create(name=entity)[0]
            entity.tracking=True
            entity.fingerprinting=True
            entity.save()

            for site in sites:
                site = Site.get_or_create(site=site)[0]
                site.entity = entity
                site.tracking=True
                site.fingerprinting = True
                site.save()

    with database.atomic():
        for entity in entities['FingerprintingGeneral']:
            entity, sites = next(iter(entity.items()))
            site, sites = next(iter(sites.items()))
            sites = set(sites)
            try:
                sites.add(utils.get_url_site(utils.get_tld_object(site)))
            except Exception:
                pass

            entity = Entity.get_or_create(name=entity)[0]
            entity.tracking=True
            entity.fingerprinting=True
            entity.save()

            for site in sites:
                site = Site.get_or_create(site=site)[0]
                site.entity = entity
                site.tracking=True
                site.fingerprinting = True
                site.save()

    # Malicious
    with database.atomic():
        for entity in entities['Cryptomining']:
            entity, sites = next(iter(entity.items()))
            site, sites = next(iter(sites.items()))
            sites = set(sites)
            try:
                sites.add(utils.get_url_site(utils.get_tld_object(site)))
            except Exception:
                pass

            entity = Entity.get_or_create(name=entity)[0]
            entity.malicious=True
            entity.save()

            for site in sites:
                site = Site.get_or_create(site=site)[0]
                site.entity = entity
                site.malicious = True
                site.save()

    # Tracking
    with database.atomic():
        for category, entities_category in entities.items():
            if category in {'FingerprintingInvasive', 'FingerprintingGeneral', 'Cryptomining'}:
                continue

            for entity in entities_category:
                entity, sites = next(iter(entity.items()))
                site, sites = next(iter(sites.items()))
                sites = set(sites)
                try:
                    sites.add(utils.get_url_site(utils.get_tld_object(site)))
                except Exception:
                    pass

                entity = Entity.get_or_create(name=entity)[0]
                entity.tracking=True
                entity.save()

                for site in sites:
                    site = Site.get_or_create(site=site)[0]
                    site.entity = entity
                    site.tracking = True
                    site.save()


if __name__ == "__main__":
    # Create tables
    with database.atomic():
        database.create_tables([Entity])
        database.create_tables([Site])
        database.create_tables([Task])
        database.create_tables([URL])

        if not Config.SQLITE:
            try:
                Task._schema.create_foreign_key(Task.landing)
            except ProgrammingError:
                pass

    # Load disconnect data
    _load_disconnect()
