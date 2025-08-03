import json
import traceback

import utils
from config import Config
from database import URL, Entity, Site, Task, load_database


def _save_entity_sites(entity, sites, adult=False, tracking=False, fingerprinting=False, malicious=False):
    entity: Entity = Entity.get_or_create(name=entity)[0]
    entity.adult = entity.adult or adult
    entity.tracking = entity.tracking or tracking
    entity.fingerprinting = entity.fingerprinting or fingerprinting
    entity.malicious = entity.malicious or malicious
    entity.save()

    for site in sites:
        site: Site = Site.get_or_create(site=site)[0]
        site.entity = site.entity or entity
        site.adult = site.adult or adult
        site.tracking= site.tracking or tracking
        site.fingerprinting = site.fingerprinting or fingerprinting
        site.malicious = site.malicious or malicious
        site.save()

def _load_disconnect(database):
    # Load disconnect entities
    try:
        with open('disconnect-tracking-protection/services.json', 'r', encoding='utf-8') as file:
            entities = json.load(file)['categories']
    except Exception as error:
        print(f'WARNING:prepare_database.py:{traceback.extract_stack()[-1].lineno} {error}')
        return

    # Fingerprinting
    with database.atomic():
        for entity in entities['FingerprintingInvasive']:
            entity, sites = next(iter(entity.items()))
            site, sites = next(iter(sites.items()))
            sites = set(sites)
            try:
                sites.add(utils.get_url_site(utils.get_tld_object(site)))
            except Exception as error:
                print(f'WARNING:prepare_database.py:{traceback.extract_stack()[-1].lineno} {error}')

            _save_entity_sites(entity, sites, tracking=True, fingerprinting=True)

    with database.atomic():
        for entity in entities['FingerprintingGeneral']:
            entity, sites = next(iter(entity.items()))
            site, sites = next(iter(sites.items()))
            sites = set(sites)
            try:
                sites.add(utils.get_url_site(utils.get_tld_object(site)))
            except Exception:
                pass

            _save_entity_sites(entity, sites, tracking=True, fingerprinting=True)

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

            _save_entity_sites(entity, sites, tracking=True, malicious=True)

    # Tracking
    for category, entities_category in entities.items():
        if category in {'FingerprintingInvasive', 'FingerprintingGeneral', 'Cryptomining'}:
            continue

        with database.atomic():
            for entity in entities_category:
                entity, sites = next(iter(entity.items()))
                site, sites = next(iter(sites.items()))
                sites = set(sites)
                try:
                    sites.add(utils.get_url_site(utils.get_tld_object(site)))
                except Exception:
                    pass

                _save_entity_sites(entity, sites, tracking=True)


if __name__ == "__main__":
    # Create tables
    database = load_database()
    with database.atomic():
        database.create_tables([Entity])
        database.create_tables([Site])
        database.create_tables([Task])
        database.create_tables([URL])

    # Load disconnect data
    _load_disconnect(database)

    database.close()
