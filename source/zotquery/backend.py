#!/usr/bin/python
# encoding: utf-8
#
# Copyright (c) 2014 stephen.margheim@gmail.com
#
# MIT Licence. See http://opensource.org/licenses/MIT
#
from __future__ import unicode_literals

from collections import OrderedDict
from contextlib import closing
import os
import re
from shutil import copyfile
import sqlite3
import struct
from time import time


# Internal Dependencies
from lib import pashua, utils
from zotero import zot
from zotquery import config
from zotquery.cache import Cache
from zotquery.config import PropertyBase, stored_property

# Alfred-Workflow
from workflow import Workflow

# create global methods from `Workflow()`
WF = Workflow()
log = WF.logger
decode = WF.decode
fold = WF.fold_to_ascii


#------------------------------------------------------------------------------
# :class:`ZotqueryBackend` ----------------------------------------------------
#------------------------------------------------------------------------------

class ZotqueryBackend(PropertyBase):
    """Contains all relevant information about this workflow.

    |       Key       |                 Description                  |
    |-----------------|----------------------------------------------|
    | `cloned_sqlite` | ZotQuery's clone of Zotero's sqlite database |
    | `json_data`     | ZotQuery's JSON clone of Zotero's sqlite     |
    | `fts_sqlite`    | ZotQuery's Full Text Search database         |
    | `folded_sqlite` | ZotQuery's ASCII-only FTS database           |

    Expects information to be stored in :file:`zotquery_data.json`.
    If file does not exist, it creates and stores dictionary.

    """

    persistent_properties = ('zotero_app', 'csl_style', 'output_format')

    def __init__(self, wf):
        """Initialize class instance.

        :param wf: a new :class:`Workflow` instance.
        :type wf: :class:`object`
        """
        self.wf = wf

        self._upgrade()  # remove data stored by old versions

        # Paths to workflow data files
        self._clone_path = wf.datafile('zotero.sqlite3')
        self._cache_path = wf.datafile('entries-cache.sqlite3')
        # self._json_path = wf.datafile('zotquery.json')
        self._fts_path = wf.datafile('search.sqlite3')
        self._fts_ascii_path = wf.datafile('search-ascii.sqlite3')

        self._cache = None
        # initialize :class:`LocalZotero`
        self.zotero = zot(self.wf)
        # initialize base class, for access to `properties` dict
        PropertyBase.__init__(self, self.wf, secured=False)
        self._con = None


    # Properties --------------------------------------------------------------

    @property
    def con(self):
        """Connection to clone database."""
        if not self._con:
            self._con = sqlite3.connect(self.cloned_sqlite)

        return self._con

    @property
    def cache(self):
        """SQLite-based cache of Zotero items."""
        if not self._cache:
            populate = not os.path.exists(self._cache_path)
            self._cache = Cache(self._cache_path)
            if populate:
                with closing(sqlite3.connect(self.cloned_sqlite)) as con:
                    for k, v in self.get_all_items().items():
                        self._cache.set(k, v)

        return self._cache


    @property
    def cloned_sqlite(self):
        """Return path to ZotQuery's cloned sqlite database.

        :returns: full path to file
        :rtype: :class:`unicode`

        """
        if not os.path.exists(self._clone_path):
            copyfile(self.zotero.original_sqlite, self._clone_path)
            log.info('Created clone SQLite database')

        return self._clone_path

    @property
    def fts_sqlite(self):
        """Return path to ZotQuery's Full Text Search sqlite database.

        :returns: full path to file
        :rtype: :class:`unicode`

        """
        if not os.path.exists(self._fts_path):
            self.create_index_db(self._fts_path)
            self.update_index_db(self._fts_path)
        return self._fts_path

    @property
    def folded_sqlite(self):
        """Return path to ZotQuery's Full Text Search sqlite database
        where all text is ASCII only.

        :returns: full path to file
        :rtype: :class:`unicode`

        """
        if not os.path.exists(self._fts_ascii_path):
            self.create_index_db(self._fts_ascii_path)
            self.update_index_db(self._fts_ascii_path, folded=True)
        return self._fts_ascii_path

    # ZotQuery Formatting Properties ------------------------------------------

    @stored_property
    def zotero_app(self):
        return self.check_storage('app', self.formatting_properties_setter)

    @stored_property
    def csl_style(self):
        return self.check_storage('csl', self.formatting_properties_setter)

    @stored_property
    def output_format(self):
        return self.check_storage('fmt', self.formatting_properties_setter)

    def formatting_properties_setter(self):
        """Configure ZotQuery formatting perferences."""
        # Check if values have already been set
        defaults = self.wf.cached_data('output_settings', max_age=0)
        if defaults is None:
            defaults = {'app': 'Standalone',
                        'csl': 'chicago-author-date',
                        'fmt': 'Markdown'}
        # Prepare `pashua` config string
        conf = """
            # Set window title
            *.title = ZotQuery Output Preferences
            # Define Zotero app
            app.type = radiobutton
            app.label = Select your Zotero application client:
            app.option = Standalone
            app.option = Firefox
            app.default = {app}
            # Define CSL style
            csl.type = radiobutton
            csl.label = Select your your desired CSL style:
            csl.option = chicago-author-date
            csl.option = apa
            csl.option = modern-language-association
            csl.option = rtf-scan
            csl.option = bibtex
            csl.option = odt-scannable-cites
            csl.default = {csl}
            # Define output format
            fmt.type = radiobutton
            fmt.label = Select your desired output format:
            fmt.option = Markdown
            fmt.option = Rich Text
            fmt.default = {fmt}
            # Add a cancel button with default label
            cb.type=cancelbutton
        """.format(**defaults)

        # Run `pashua` dialog and save results to storage file
        res_dict = pashua.run(conf, encoding='utf8', pashua_path=config.PASHUA)
        if res_dict['cb'] != 1:
            del res_dict['cb']
            self.wf.cache_data('output_settings', res_dict)

    # Utility methods ---------------------------------------------------------

    def is_fresh(self):
        """Is ZotQuery up-to-date with Zotero?

        Two specific questions:
            + is ``cloned_sqlite`` parallel to :attr:`Zotero.original_sqlite`?
            + is ``json_data`` parallel to ``cloned_sqlite``?

        :returns: tuple with Boolean answer and rotten file
        :rtype: :class:`tuple`

        """
        update, spot = False, None
        zotero_mod = os.stat(self.zotero.original_sqlite).st_mtime
        clone_mod = os.stat(self.cloned_sqlite).st_mtime
        cache_mod = self.cache.updated

        # Check if cloned sqlite database is up-to-date with `zotero` database
        if zotero_mod > clone_mod:
            update, spot = True, "Clone"

        # Check if cache is up-to-date with the cloned database
        elif (cache_mod - clone_mod) > 10:
            update, spot = True, "Cache"

        if update:
            log.debug('Update %s? %s', spot, update)

        return update, spot

    def update_clone(self):
        """Update `cloned_sqlite` so that it's current with `original_sqlite`.

        """
        copyfile(self.zotero.original_sqlite, self._clone_path)
        log.info('Updated clone SQLite file')

    def update_cache(self):
        """Update cache of Zotero items."""
        if os.path.exists(self._cache_path):
            os.rename(self._cache_path, self._cache_path + '.backup')

        # Rebuild cache
        self._cache = None
        self.cache
        log.info('Updated item cache')

    ## JSON to FTS sub-methods ------------------------------------------------

    @staticmethod
    def create_index_db(db):
        """Create FTS virtual table with data from ``json_data``

        :param db: path to `.db` file
        :type db: :class:`unicode`

        """
        with closing(sqlite3.connect(db)) as con:
            with con as cur:
                # get search columns from `general` scope
                columns = config.FILTERS.get('general', None)
                if columns:
                    # convert list to string
                    columns = ', '.join(columns)
                    sql = """CREATE VIRTUAL TABLE zotquery
                             USING fts3({cols})""".format(cols=columns)
                    cur.execute(sql)
                    log.debug('Created FTS database: {}'.format(db))

    def update_index_db(self, fts_path, folded=False):
        """Update ``fts_sqlite`` with JSON data from ``json_data``.

        Reads in data from ``json_data`` and adds it to the FTS database.

        :param fts_path: path to `.db` file
        :type fts_path: :class:`unicode`
        :param folded: should all text be ASCII-normalized?
        :type folded: :class:`boolean`

        """
        # grab start time
        start = time()
        count = 0
        with closing(sqlite3.connect(fts_path)) as con:
            with con as cur:
                # iterate over every item in library
                for d in self.generate_data():
                    # names of all keys for item (cf. `FILTERS['general']`)
                    columns = ', '.join(d.keys())
                    values = d.values()
                    # fold to ASCII-only?
                    if folded:
                        values = [fold(s) for s in values]

                    sql = """INSERT OR IGNORE INTO zotquery
                             ({columns}) VALUES ({data})
                            """.format(columns=columns,
                                       data=','.join(['?'] * len(values)))
                    cur.execute(sql, values)
                    count += 1

        log.debug('Added/Updated %d items in %0.3fs', count, time() - start)

    def generate_data(self):
        """Create a genererator with dictionaries for each item
        in ``json_data``.

        :yields: ``dict`` with all item's data as ``strings``
        :rtype: :class:`genererator`

        """
        # json_data = utils.read_json(self.json_data)
        # for item in json_data.itervalues():

        # for each `item`, get its data in dict format
        for item in self.cache.values():
            array = []
            # get search columns from scope
            columns = config.FILTERS.get('general', None)
            if columns:
                data = OrderedDict()
                for column in columns:
                    # get search map from column
                    json_map = config.FILTERS_MAP.get(column, None)
                    if json_map:
                        # get data from `item` using search map
                        data[column] = self.get_datum(item, json_map)

                yield data

    @staticmethod
    def get_datum(item, val_map):
        """Retrieve content of key ``val_map`` from ``item``.

        :param val_map: mapping of where to find certain data in `item` JSON
        :type val_map: :class:`unicode` OR :class:`list`
        :returns: all of ``item``'s values for the key
        :rtype: :class:`unicode`

        """
        if isinstance(val_map, unicode):  # highest JSON level
            result = item[val_map]
            if isinstance(result, unicode):
                result = [result]
        elif isinstance(val_map, list):   # JSON sub-level
            if isinstance(val_map[0], unicode) and len(val_map) == 2:
                [key, val] = val_map
                try:                    # key, val result is string
                    result = [item[key][val]]
                except TypeError:       # key, val result is list
                    result = [x[val] for x in item[key]]
                except KeyError:
                    result = []
            elif isinstance(val_map[0], list):  # list of possible k, v pairs
                check = None
                for pair in val_map:
                    [key, val] = pair
                    try:
                        check = [item[key][val]]
                    except KeyError:
                        pass
                result = check if check else []
        else:
            result = []

        return ' '.join(result)

    @staticmethod
    def make_rank_func(weights):
        """Search ranking function.

        Use floats (1.0 not 1) for more accurate results. Use 0 to ignore a
        column.

        Adapted from <http://goo.gl/4QXj25> and <http://goo.gl/fWg25i>

        :param weights: list or tuple of the relative ranking per column.
        :type weights: :class:`tuple` OR :class:`list`
        :returns: a function to rank SQLITE FTS results
        :rtype: :class:`function`

        """
        def rank(matchinfo):
            """
            `matchinfo` is defined as returning 32-bit unsigned integers in
            machine byte order (see http://www.sqlite.org/fts3.html#matchinfo)
            and `struct` defaults to machine byte order.
            """
            bufsize = len(matchinfo)  # Length in bytes.
            matchinfo = [struct.unpack(b'I', matchinfo[i:i + 4])[0]
                         for i in range(0, bufsize, 4)]
            it = iter(matchinfo[2:])
            return sum(x[0] * w / x[1]
                       for x, w in zip(zip(it, it, it), weights)
                       if x[1])
        return rank

    ## SQLITE to JSON sub-methods ---------------------------------------------

    def get_all_items(self):
        """Return a `dict` wherein each item's Zotero key
        is the dictionary key. The value for each item is itself a
        dictionary with all of that item's information, organized
        under these sub-keys:
            key
            library
            type
            creators
            data
            zot-collections
            zot-tags
            attachments
            notes
        *Note:* singular sub-keys (key, library, type, data) have a
        ``string`` or a ``dictionary`` as their value; plural sub-keys
        (creators, zot-collections, zot-tags, attachments, notes) all
        have a ``list`` as their value.

        Here's an example item:
        ```
        "C3KEUQJW": {
            "key": "C3KEUQJW",
            "library": "0",
            "type": "journalArticle",
            "creators": [
                {
                    "index": 0,
                    "given": "Stephen",
                    "type": "author",
                    "family": "Margheim"
                }
            ],
            "data": {
                "volume": "1",
                "issue": "1",
                "pages": "1-14",
                "publicationTitle": "A Sample Publication",
                "date": "2013",
                "title": "Test Item"
            },
            "zot-collections": [],
            "zot-tags": [],
            "attachments": [
                {
                    "path": "path/to/some/file/test_item.pdf",
                    "name": "test_item.pdf",
                    "key": "GTDIDHW4"
                }
            ],
            "notes": []
        }
        ```
        Adapted from: <https://github.com/pkeane/zotero_hacks>

        """
        start = time()
        items = {}
        # get key data for each Zotero item
        info_sql = """
            SELECT key, itemID, itemTypeID, libraryID
            FROM items
            WHERE
                itemTypeID not IN (1, 13, 14)
            ORDER BY dateAdded DESC
        """
        with closing(sqlite3.connect(self.cloned_sqlite)) as con:
            with con as cur:
                # iterate thru every item
                for row in cur.execute(info_sql):
                    key, id_, type_id, library_id = row

                    item = OrderedDict()
                    # If user only wants personal library
                    if config.PERSONAL_ONLY is True and library_id is not None:
                        continue
                    library_id = library_id if library_id is not None else '0'
                    # place key ids in item's root dict
                    item['key'] = key
                    item['library'] = library_id
                    item['type'] = self._item_type_name(type_id)
                    # add list of dicts with each creator's info to root dict
                    item['creators'] = self._item_creators(id_)
                    # add list of dicts with item's metadata to root dict
                    item['data'] = self._item_metadata(id_)
                    # add list of dicts with item's collections to root dict
                    item['zot-collections'] = self._item_collections(id_)
                    # add list of dicts with item's tags to root dict
                    item['zot-tags'] = self._item_tags(id_)
                    # add list of dicts with item's attachments to root dict
                    item['attachments'] = self._item_attachments(id_)
                    # add list of dicts with item's notes to root dict
                    item['notes'] = self._item_notes(id_)
                    # add all data as value of `key`
                    items[key] = item

        return items

    # TODO: Create a JSON db class to house all this code
    def to_json(self):
        """Convert Zotero's sqlite database to structured JSON.

        The data are provided by :meth:`get_all_items`.
        """
        start = time()
        self.wf.store_data('zotquery', self.get_all_items(), serializer='json')
        log.info('Created JSON file in {:0.3}s'.format(time() - start))

    # def _execute(self, sql):
    #     """Execute sqlite query and return sqlite object.

    #     :param sql: SQL or SQLITE query string
    #     :type sql: :class:`unicode`
    #     :returns: SQLITE object of executed query
    #     :rtype: :class:`object`

    #     """
    #     cur = self.con.cursor()
    #     return cur.execute(sql)

    def _select(self, parts):
        """Prepare standard sqlite query string.

        :param parts: SQL or SQLITE query string
        :type parts: :class:`unicode`
        :returns: SQLITE object of executed query
        :rtype: :class:`object`

        """
        sel, src, mtch, _id = parts
        sql = """SELECT {} FROM {} WHERE {} = ?"""
        sql = sql.format(sel, src, mtch)
        with self.con as cur:
            return cur.execute(sql, (_id,))

    ### Individual Item Data --------------------------------------------------

    def _item_type_name(self, item_type_id):
        """Get name of type from `item_type_id`

        :param item_type_id: ID number of item type in Zotero SQLITE
        :type item_type_id: :class:`int`
        :returns: name of specified type
        :rtype: :class:`unicode`

        """
        sql = "SELECT typeName FROM itemTypes WHERE itemTypeID = ?"
        with self.con as cur:
            r = cur.execute(sql, (item_type_id,)).fetchone()

        return r[0]

    def _item_creators(self, item_id):
        """Generate array of dicts with item's creators' information.

        :param item_id: ID number of item in Zotero SQLITE
        :type item_id: :class:`int`
        :returns: creator information for `item_id`
        :rtype: :class:`list`

        """
        creators = []

        sql = """
        SELECT creators.firstName, creators.lastName,
            creatorTypes.creatorType, itemCreators.orderIndex
        FROM itemCreators
            LEFT JOIN creators
                ON itemCreators.creatorID = creators.creatorID
            LEFT JOIN creatorTypes
                ON itemCreators.creatorTypeID = creatorTypes.creatorTypeID
        WHERE itemCreators.itemID = ?
        """

        with self.con as cur:
            for row in cur.execute(sql, (item_id,)):
                firstname, lastname, typ, order = row
                log.debug('[%s/%s] %s %s', item_id, typ, firstname, lastname)
                creators.append({'family': lastname,
                                 'given': firstname,
                                 'type': typ,
                                 'index': order})

        return creators

    def _item_metadata(self, item_id):
        """Generate array of dicts with all item's metadata.

        :param item_id: ID number of item in Zotero SQLITE
        :type item_id: :class:`int`
        :returns: metadata information for `item_id`
        :rtype: :class:`dict`

        """
        item_meta = OrderedDict()
        # get all metadata for item
        sql = """
            SELECT itemData.fieldID, itemData.valueID,
                    fields.fieldName, itemDataValues.value
                FROM itemData
                LEFT JOIN fields
                    ON itemData.fieldID = fields.fieldID
                LEFT JOIN itemDataValues
                    ON itemData.valueID = itemDataValues.valueID
            WHERE itemData.itemID =  ?
        """
        with self.con as cur:
            # iterate thru metadata
            for row in cur.execute(sql, (item_id,)):
                field_id, value_id, field_name, value_name = row

                if field_name not in item_meta:
                    item_meta[field_name] = ''

                    if field_name == 'date':
                        item_meta[field_name] = value_name[0:4]
                    else:
                        item_meta[field_name] = value_name

        return item_meta

    def _item_collections(self, item_id):
        """Generate an array or dicts with all of the `zotero` collections
        in which the item resides.

        :param item_id: ID number of item in Zotero SQLITE
        :type item_id: :class:`int`
        :returns: collection information for `item_id`
        :rtype: :class:`list`

        """
        collections = []

        sql = """
        SELECT collections.collectionName, collections.key
            FROM collections
                LEFT JOIN collectionItems
                ON collections.collectionID = collectionItems.collectionID
        WHERE collectionItems.itemID = ?
        """
        with self.con:
            cur = self.con.cursor()
            for row in cur.execute(sql, (item_id,)):
                name, key = row
                log.debug('[%s/collection] %s', item_id, name)
                collections.append({'name': name, 'key': key,
                                    'library_id': '0', 'group': 'personal'})

        return collections

    def _item_tags(self, item_id):
        """Generate an array of dicts with all of the `zotero` tags
        assigned to the item.

        :param item_id: ID number of item in Zotero SQLITE
        :type item_id: :class:`int`
        :returns: tag information for `item_id`
        :rtype: :class:`list`

        """
        tags = []
        sql = """
            SELECT tags.name, tags.tagID
                FROM itemTags
                LEFT JOIN tags ON itemTags.tagID = tags.tagID
            WHERE itemTags.itemID = ?
        """
        with self.con as cur:
            for row in cur.execute(sql, (item_id,)):
                name, id_ = row
                tags.append({'name': name, 'id': id_})

        return tags

    def _item_attachments(self, item_id):
        """Generate an array or dicts with all of the item's attachments.

        :param item_id: ID number of item in Zotero SQLITE
        :type item_id: :class:`int`
        :returns: attachment information for `item_id`
        :rtype: :class:`list`

        """
        attachments = []

        sql = """
        SELECT itemAttachments.path, itemAttachments.itemID, items.key
            FROM itemAttachments
                LEFT JOIN items ON itemAttachments.itemID = items.itemID
        WHERE itemAttachments.parentItemID = ?
        """

        with self.con:
            cur = self.con.cursor()

            for row in cur.execute(sql, (item_id,)):
                name, id_, key = row
                log.debug('[%s/attachment] %s', item_id, name)

                for prefix in ('attachment:', 'storage:'):
                    if name.startswith(prefix):
                        name = name[len(prefix):]
                        for x in config.ATTACH_EXTS:
                            if name.endswith(x):
                                path = os.path.join(
                                    self.zotero.internal_storage, key, name)

                                attachments.append({'name': name, 'key': key,
                                                    'path': path})

        return attachments

    def _item_notes(self, item_id):
        """Generate an array of dicts with all of the item's notes.

        :param item_id: ID number of item in Zotero SQLITE
        :type item_id: :class:`int`
        :returns: note information for `item_id`
        :rtype: :class:`list`

        """
        from lib.utils import HTMLText
        notes = []
        sql = """SELECT note FROM itemNotes WHERE parentItemID = ?"""
        with self.con:
            cur = self.con.cursor()
            for row in cur.execute(sql, (item_id,)):
                note = HTMLText.strip(row[0])
                log.debug('[%s/note] %s', item_id, note)
                notes.append(note)

        return notes

    def _upgrade(self):
        """Remove old databases and force rebuild."""
        sentinel = '_upgrade_1'
        senpath = self.wf.datafile(sentinel)

        if not os.path.exists(senpath):
            log.info('removing old data for upgrade ...')
            for fn in os.listdir(self.wf.datadir):
                if fn == sentinel:
                    continue

                p = self.wf.datafile(fn)
                log.debug('removing %s ...', p)
                os.unlink(p)

            with open(senpath, 'wb') as fp:
                fp.write('')

#-----------------------------------------------------------------------------
# Alias
#-----------------------------------------------------------------------------

data = ZotqueryBackend
