# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from rugby_scraper import models, items, settings

class RugbyScraperPipeline(object):
    def __init__(self):
        """"""
        # Connect to DB
        self.engine = create_engine(settings.SQLITE_URL)
        self.session = sessionmaker(bind = self.engine)
        models.create_tables(self.engine)

    def open_spider(self, spider):
        self.logger = spider.logger

    def process_item(self, item, spider):
        """"""
        session = self.session()
        instance = None
        try:
            if isinstance(item, items.Match):
                self._unique_insert(session, models.Match, item)
            elif isinstance(item, items.Player):
                self._unique_insert(session, models.Player, item)
            elif isinstance(item, items.Team):
                self._unique_insert(session, models.Team, item)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return item

    def _generic_insert(self, session, model, item):
        if not model or not item:
            return
        self.logger.info("Inserting entry of type \"{}\" with id {}".format(item.__class__.__name__, model.id))
        return session.add(model(**item))

    def _unique_insert(self, session, model, item):
        if not model or not item:
            return
        instance = model(**item)

        if not session.query(model).filter(model.id == instance.id).first():
            return self._generic_insert(session, model, item)
