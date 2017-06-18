# -*- coding: utf-8 -*-
import codecs
import bs4 as BeautifulSoup
import json
from urlparse import urlparse


class UnknownPageTypeException(Exception):
    pass


class BaseProvider(object):
    """Base Provider.

    Base Class for a provider

    """
    provider_uid = ''

    def __init__(self, fpath):
        self.source_file_path = fpath
        self.num_of_lines = None

    def get_provider_uid(self):
        """Get Provider Unique ID"""
        return self.provider_uid

    def read_file(self):
        """Read File line by line with UTF-8 encoding; Returns generator with data line"""
        with codecs.open(self.source_file_path, "r", "utf-8") as datasrc:
            while True:
                line = datasrc.readline()
                if not line:
                    break
                yield line

    def read_entry(self):
        """Read File line by line with UTF-8 encoding; Returns generator with data line"""
        with codecs.open(self.source_file_path, "r", "utf-8") as datasrc:
            while True:
                line = datasrc.readline()
                if not line:
                    break
                yield self.extract_date_line(line)

    def count_lines(self, recount=False):
        """Calculated the number of lines in data source file.

        The calculated value is cached in the instance and can be re-calculated by
        setting the `recount` param to `True`
        """
        if self.num_of_lines is None or recount:
            self.num_of_lines = sum(1 for line in open(self.source_file_path, 'r'))
        return self.num_of_lines

    def extract_date_line(self, date_line):
        """Extract data from line.

        Reads `data_line` param as json entity and converts to python dictionary.

        The `body` value of the data_line_json is parsed by BeautifulSoup with lxml parser.
        The BeautifulSoup instance of the body will be added to the return entry

        """
        entry = json.loads(date_line)

        # -------------------------------------------------------------------------
        # Prepare HTMLX
        # -------------------------------------------------------------------------
        entry['htmlx'] = None
        entry['_parser_error'] = False
        if 'body' in entry:
            try:
                entry['htmlx'] = BeautifulSoup.BeautifulSoup(entry['body'], 'lxml')
            except:
                entry['_parser_error'] = True

        if not entry['_parser_error']:
            epage_type = entry.get('page_type')
            if epage_type == 'product_detail':
                extracted_data = self.extract_product_detail_info(entry)
            elif epage_type == 'product_listing':
                extracted_data = self.extract_product_listing_items(entry)
            else:
                raise UnknownPageTypeException(
                    "No extractor profile define for page_type: %s" % (epage_type)
                    )

            entry["extracted_data"] = extracted_data
            entry["extract_ok"] = True
        else:
            entry["extract_ok"] = False

        return entry

    def combine_entry_data(self, entry, item_info=None):
        """Combine entry data and item_info to ensure default fields are within the dataset"""
        item_info = item_info or {}

        page_type = entry.get('page_type')
        if page_type == 'product_listing':
            ekeys = [
                ("page_type", None),
                ("page_url", None),
                ("page_number", 1),
                ("crawled_at", None),
                ("product_category", None),
                ("ordering", None),
            ]
        elif page_type == 'product_detail':
            ekeys = [
                ("page_type", None),
                ("page_url", None),
                ("crawled_at", None),
            ]

        for k in ekeys:
            item_info[k[0]] = entry.get(k[0], k[1])

        url = urlparse(entry.get("page_url"))
        item_info["page_domain"] = url.netloc
        item_info["page_path"] = url.path

        if 'page_number' in item_info and item_info['page_number'] is None:
            item_info['page_number'] = 1

        return item_info

    def get_select_path_text(self, xitem, xpath, default=None):
        """BeautifulSoup item select_one with xpaht and return text attribute"""
        try:
            return getattr(xitem.select_one(xpath), 'text', default)
        except:
            return default

    def get_select_path_attr(self, xitem, xpath, attr, default=None):
        """BeautifulSoup item select_one with xpaht and return specified `attr` attribute"""
        try:
            return getattr(xitem.select_one(xpath), 'attrs', {}).get(attr, default)
        except:
            return default


def product_listing_structure(provider_uid):
    """Decorator:
    add `provider_uid` and set default data structure for product listing page info
    """
    def product_listing_structure_decorator(func):
        def func_wrapper(*args, **kwargs):
            structure = {
                "article_name": None,
                "brand_name": None,
                "detail_page_url": None,
                "discount_percentage": 0.0,
                "on_sale": False,
                "sale_price": None,
                "sku": None,
                "listing_props": {},
                "website": provider_uid,
            }

            data = func(*args, **kwargs)
            if isinstance(data, list):
                ret_data = []
                for d in data:
                    if isinstance(d, dict):
                        clone = dict(structure)
                        clone.update(d)
                        ret_data.append(clone)
                return ret_data
            elif isinstance(data, dict):
                structure.update(data)
            return structure
        return func_wrapper
    return product_listing_structure_decorator


def product_detail_structure(provider_uid):
    """Decorator:
    add `provider_uid` and set default data structure for product detail info
    """
    def product_detail_structure_decorator(func):
        def func_wrapper(*args, **kwargs):
            structure = {
                "article_name": None,
                "article_type": None,
                "brand_name": None,
                # "color": None,
                "discount_percentage": 0.0,
                "extra_props": {},
                "on_sale": False,
                "sale_price": None,
                "sku": None,
                # "variants": [],
                "website": provider_uid,
            }

            structure.update(func(*args, **kwargs))

            return structure
        return func_wrapper
    return product_detail_structure_decorator
