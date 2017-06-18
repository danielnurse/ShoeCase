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


PROVIDER_UID = "ziengs"

class Provider_ProductDetail(BaseProvider):

    item_detail_select_xpaths = {
        "sku": '#hdnProductId',
        "article_name": 'h1[itemprop="name"]',
        "article_type": 'meta[itemprop="category"]',
        "brand_name": 'meta[itemprop="brand"]',
        "sale_price": 'meta[itemprop="price"]',
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


        item_info['sku'] = get_xpath_attr(xpath=xpaths('sku'), attr='value')
        item_info['article_name'] = get_xpath_text(xpath=xpaths('article_name'))
        item_info['article_type'] = get_xpath_attr(xpath=xpaths('article_type'), attr='content')
        item_info['brand_name'] = get_xpath_attr(xpath=xpaths('brand_name'), attr='content')

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

        # -------------------------------------------------------------------------
        # Extract Properties
        # -------------------------------------------------------------------------
        extra_props = {
            "normal_price": old_price,
        }
        try:
            props_container = [x for x in xitem.select('#detailBottom > div  h3') if x.text == 'Extra kenmerken'][0].parent
        except:
            props_container = None
        if props_container:
            for prop_els in props_container.select('dl'):
                citer = prop_els.children
                data = {}
                while True:
                    try:
                        c = next(citer)
                        if isinstance(c, basestring):
                            continue
                        if c.name == 'dt':
                            title = c.text
                            value = None
                            while True:
                                c = next(citer)
                                if c.name == 'dd':
                                    value = c.text
                                    break
                                elif c.name == 'dt':
                                    # Assign new title incase there is one?
                                    title = c.text

                            # set title and value into data dictionary
                            data[title.lower().strip()] = value.strip()
                    except StopIteration:
                        break
                extra_props.update(data)

        item_info['extra_props'] = extra_props

        if 'categorie' in extra_props:
            item_info['article_type'] = extra_props['categorie']

        return item_info


class Provider_ProductListing(BaseProvider):
    item_listing_select_xpaths = {
        'detail_page_url': 'div.product > a',
        'brand_name': 'div.product > a > strong.merk',
        'article_type': 'div.product > a > em.soort',
        # 'article_name': 'div.catalogArticlesList_content div.catalogArticlesList_articleName',
        
        'price_listing': 'div.product > a > span.prijs',
        'price_special': 'div.product > a > span.prijs > ins',
        'price_normal': 'div.product > a > span.prijs > del',
    }

    def extract_product_listing_items(self, entry):
        """Parse Product Listing Data"""
        docx = entry.get('htmlx', None)
        if not docx or entry.get('_parser_error', False):
            return

        # -------------------------------------------------------------------------
        # Find all Article Listings
        # -------------------------------------------------------------------------
        list_containers = docx.find_all('div', {'class': 'productList'})
        page_position = 0
        failed = 0
        processed = 0
        items = []
        errmsgs = []
        for list_container in list_containers:
            list_items = list_container.find_all('div', {'class': 'item'})
            for list_item in list_items:
                page_position = page_position + 1
                try:
                    item_info = self.parse_product_listing_item(xitem=list_item)
                    # -------------------------------------------------------------------------
                    # Because ziengs has multiple article variants per listing
                    # the return value can be a list
                    # -------------------------------------------------------------------------
                    if isinstance(item_info, list):
                        for i in item_info:
                            if isinstance(i, dict):
                                i['page_position'] = page_position
                                items.append(
                                    self.combine_entry_data(entry, i)
                                )
                    else:
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
            "number_of_items": page_position,
            "failed": failed,
            "processed": processed,
            "errmsgs": errmsgs,
        }

    @product_listing_structure(PROVIDER_UID)
    def parse_product_listing_item(self, xitem):
        items = []

        xitem_attrs = getattr(xitem, 'attrs', {})

        xvariants = [x.attrs.get('data-colorid') for x in xitem.select('div.colorDivItem > ul') if 'data-colorid' in getattr(x, 'attrs', {})]

        pricing = utils.convert_html_price_to_float

        get_xpath_text = partial(self.get_select_path_text, xitem=xitem, default=None)
        get_xpath_attr = partial(self.get_select_path_attr, xitem=xitem, default=None)

        normal_price = pricing(get_xpath_text(xpath='div.content > span.offerText'))
        on_sale = 'vanvoor' in xitem_attrs.get('class', [])

        for xvar in xvariants:
            item_info = {
                "on_sale": on_sale,
            }
            extra_props = {
                "normal_price": normal_price,
            }

            xlink = getattr(xitem.select_one('div.colorDivItem > ul[data-colorid="%s"] a' % (xvar)), 'attrs', {})
            extra_props['color'] = xlink.get('title')
            item_info['detail_page_url'] = re.sub(r'(../)+', '/',
                utils.get_url_path(xlink.get('href'))
                )


            item_info['article_name'] = get_xpath_text(xpath='div.content > div[data-colorid="%s"] > a.title' % (xvar))
            item_info['sale_price'] = pricing(get_xpath_text(xpath='div.content > div[data-colorid="%s"] > span.price' % (xvar)))


            item_info['discount_percentage'] = utils.calcDiscountPercentage(
                new_price=item_info['sale_price'],
                old_price=normal_price,
                )

            item_info['listing_props'] = extra_props

            items.append(dict(item_info))

        return items

        # item_info = {}



        # item_info['detail_page_url'] = utils.get_url_path(get_xpath_attr(
        #     xpath=xpaths('detail_page_url'),
        #     attr='href'))

        # xitem_attrs = getattr(xitem, 'attrs', {})

        # try:
        #     google_data = json.loads(get_xpath_attr(xpath=xpaths('detail_page_url'), attr="data-google"))
        #     item_info['sku'] = google_data.get('id')
        #     item_info['article_name'] = google_data.get('name')
        #     item_info['brand_name'] = google_data.get('brand')
        #     item_info['sale_price'] = pricing(google_data.get('price'))
        # except:
        #     item_info['brand_name'] = get_xpath_text(xpath=xpaths('brand_name'))
        #     item_info['sku'] = xitem_attrs.get('data-artikel')

        # item_info['article_type'] = get_xpath_text(xpath=xpaths('article_type'))

        # # -------------------------------------------------------------------------
        # # Extract Pricing Info
        # # -------------------------------------------------------------------------
        # price_info = {}
        # price_info['price_special'] = pricing(get_xpath_text(xpath=xpaths('price_special')))
        # price_info['price_normal'] = pricing(get_xpath_text(xpath=xpaths('price_normal')))
        # price_info['price_listing'] = pricing(get_xpath_text(xpath=xpaths('price_listing')))

        # if price_info['price_special'] and price_info['price_special'] < price_info['price_listing']:
        #     price_info['price_listing'] = price_info['price_special']

        #     if price_info['price_normal'] and price_info['price_normal'] > 0.0:
        #         price_info['price_discount'] = (1.0 - (price_info['price_special'] / price_info['price_normal']) ) * 100.0

        # if item_info.get('sale_price') is None:
        #     item_info['sale_price'] = price_info['price_listing']

        # item_info['discount_percentage'] = utils.calcDiscountPercentage(
        #     new_price=item_info['sale_price'],
        #     old_price=price_info['price_normal']
        #     )
        
        # item_info['on_sale'] = 'vanvoor' in xitem_attrs.get('class', [])

        # # -------------------------------------------------------------------------
        # # Extra Props
        # # -------------------------------------------------------------------------
        # extra_props = {}
        # extra_props['overview_position'] = xitem_attrs.get('data-position')
        # extra_props['badge'] = get_xpath_text(xpath="span.badge > span.badge-label")
        # extra_props['price_info'] = price_info

        # item_info['listing_props'] = extra_props

        # return item_info


class ZiengsProvider(
    Provider_ProductListing,
    Provider_ProductDetail,
    BaseProvider
    ):
    provider_uid = PROVIDER_UID

