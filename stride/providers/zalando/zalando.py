# -*- coding: utf-8 -*-
import logging
import sys


from providers.provider import BaseProvider, product_detail_structure, product_listing_structure
from functools import partial
# from common import util as utils
import utils
import json
import re

LOG = logging.getLogger(__name__)


PROVIDER_UID = "zalando"

class Provider_ProductDetail(BaseProvider):

    item_detail_select_xpaths = {
        "sale_price": 'meta[name="twitter:data1"]',
        "old_price": '#articleOldPrice',
        "properties": "#productDetails div.content > ul > li",
    }

    def extract_product_detail_info(self, entry):
        """Parrse Product Detail Data"""
        docx = entry.get('htmlx', None)
        if not docx or entry.get('_parser_error', False):
            return

        # -------------------------------------------------------------------------
        # Find Main View
        # -------------------------------------------------------------------------
        errmsg = None
        item_info = None
        try:
            item_info = self.combine_entry_data(
                entry=entry, 
                item_info=self.parse_product_detail_item(xitem=docx)
                )
            status = True
        except Exception as error:
            status = False
            errmsg = str(error)
            raise

        return {
            "ok": status,
            "item": item_info,
            "errmsg": errmsg,
        }

    @product_detail_structure(PROVIDER_UID)
    def parse_product_detail_item(self, xitem):
        item_info = {}
        pricing = utils.convert_html_price_to_float
        status = True

        xpaths = self.item_detail_select_xpaths.get
        get_xpath_text = partial(self.get_select_path_text, xitem=xitem, default=None)
        get_xpath_attr = partial(self.get_select_path_attr, xitem=xitem, default=None)

        # -------------------------------------------------------------------------
        # Extract Data from script application/ld+json
        # -------------------------------------------------------------------------
        raw_detail_info = json.loads(
            get_xpath_text(xpath='script[type="application/ld+json"]')
        )

        item_info['sku'] = raw_detail_info.get('sku')
        item_info['article_name'] = raw_detail_info.get('name')
        item_info['brand_name'] = raw_detail_info.get('brand')

        # -------------------------------------------------------------------------
        # Extract From Metadata
        # -------------------------------------------------------------------------
        item_info['sale_price'] = pricing(get_xpath_attr(
            xpath=xpaths('sale_price'),
            attr='content'))
        old_price = pricing(get_xpath_text(
            xpath=xpaths('old_price'),
            ), default=0.0)
        item_info['discount_percentage'] = utils.calcDiscountPercentage(
            new_price=item_info['sale_price'],
            old_price=old_price
            )
        item_info['on_sale'] = item_info['discount_percentage'] > 0.0

        # # -------------------------------------------------------------------------
        # # Extract Variants
        # # -------------------------------------------------------------------------
        # variants = []
        # for offer in raw_detail_info.get('offers', []):
        #     if offer.get('@type') != 'Offer':
        #         continue
        #     ioffer = offer.get('itemOffered', {})
        #     variant = {
        #         "gtin13": ioffer.get('gtin13'),
        #         "sku": ioffer.get('sku'),
        #         "price": pricing(offer.get('price')),
        #     }

        #     variants.append(variant)
        # item_info['variants'] = variants

        # -------------------------------------------------------------------------
        # Extract Properties
        # -------------------------------------------------------------------------
        extra_props = {}

        for prop_line in xitem.select(xpaths('properties')):
            prop_text = getattr(prop_line, 'text', '')
            if not prop_text:
                continue

            try:
                prop_key, prop_value = re.sub(r'\n', ' ', prop_text).split(":")
                extra_props[utils.cleanKeyForDict(prop_key)] = prop_value.strip()
            except:
                continue

        item_info['extra_props'] = extra_props


        return item_info


class Provider_ProductListing(BaseProvider):
    item_listing_select_xpaths = {
        'detail_page_url': 'div.catalogArticlesList_content a.catalogArticlesList_productBox',
        'brand_name': 'div.catalogArticlesList_content div.catalogArticlesList_brandName',
        'article_name': 'div.catalogArticlesList_content div.catalogArticlesList_articleName',
        'price_listing': 'div.catalogArticlesList_content div.catalogArticlesList_priceBox div.catalogArticlesList_price',
        'price_normal': 'div.catalogArticlesList_content div.catalogArticlesList_priceBox div.catalogArticlesList_price-old',
        'price_special': 'div.catalogArticlesList_content div.catalogArticlesList_priceBox div.specialPrice',
        'sku': 'div.catalogArticlesList_content span.sku',
    }

    def extract_product_listing_items(self, entry):
        """Parse Product Listing Data"""
        docx = entry.get('htmlx', None)
        if not docx or entry.get('_parser_error', False):
            return

        # -------------------------------------------------------------------------
        # Find all Article Listings
        # -------------------------------------------------------------------------
        list_containers = docx.find_all('ul', {'class': 'catalogArticlesList'})
        page_position = 0
        failed = 0
        processed = 0
        items = []
        errmsgs = []
        for list_container in list_containers:
            list_items = list_container.find_all('li', {'class': 'catalogArticlesList_item'})
            for list_item in list_items:
                page_position = page_position + 1
                try:
                    item_info = self.parse_product_listing_item(xitem=list_item)
                    item_info['page_position'] = page_position
                    item_info = self.combine_entry_data(entry, item_info)

                    items.append(item_info)
                    processed + processed + 1
                except Exception as error:
                    failed = failed + 1
                    errmsgs.append(str(error))
                    raise error

        return {
            "items": items,
            "failed": failed,
            "processed": processed,
            "errmsgs": errmsgs,
        }

    @product_listing_structure(PROVIDER_UID)
    def parse_product_listing_item(self, xitem):
        item_info = {}

        xpaths = self.item_listing_select_xpaths.get

        get_xpath_text = partial(self.get_select_path_text, xitem=xitem, default=None)
        get_xpath_attr = partial(self.get_select_path_attr, xitem=xitem, default=None)

        item_info['detail_page_url'] = get_xpath_attr(
            xpath=xpaths('detail_page_url'),
            attr='href')
        item_info['sku'] = get_xpath_text(xpath=xpaths('sku'))
        item_info['article_name'] = get_xpath_text(xpath=xpaths('article_name'))
        item_info['brand_name'] = get_xpath_text(xpath=xpaths('brand_name'))

        # -------------------------------------------------------------------------
        # Extract Pricing Info
        # -------------------------------------------------------------------------
        pricing = utils.convert_html_price_to_float

        price_info = {}
        price_info['price_special'] = pricing(get_xpath_text(xpath=xpaths('price_special')))
        price_info['price_listing'] = pricing(get_xpath_text(xpath=xpaths('price_listing')))
        price_info['price_normal'] = pricing(get_xpath_text(xpath=xpaths('price_normal')))

        if price_info['price_special'] and price_info['price_special'] < price_info['price_listing']:
            price_info['price_listing'] = price_info['price_special']

            if price_info['price_normal'] and price_info['price_normal'] > 0.0:
                price_info['price_discount'] = (1.0 - (price_info['price_special'] / price_info['price_normal']) ) * 100.0

        # item_info['pricing'] = price_info
        item_info['sale_price'] = price_info['price_listing']

        item_info['discount_percentage'] = utils.calcDiscountPercentage(
            new_price=item_info['sale_price'],
            old_price=price_info['price_normal']
            )
        item_info['on_sale'] = item_info['discount_percentage'] > 0.0

        return item_info


class ZalandoProvider(
    Provider_ProductListing,
    Provider_ProductDetail,
    BaseProvider
    ):
    provider_uid = PROVIDER_UID
