# -*- coding: utf-8 -*-
import os.path
import json
from collections import namedtuple
from providers.zalando.zalando import ZalandoProvider
from providers.omoda.omoda import OmodaProvider
from providers.ziengs.ziengs import ZiengsProvider
import models
import utils
from pymodm.vendor import parse_datetime
import codecs
from custom_log import prepare_logger
from config import (ZIENGS_PROVIDER_DATASETS_FILEPATH,
                    OMODA_PROVIDER_DATASETS_FILEPATH,
                    ZALANDO_PROVIDER_DATASETS_FILEPATH,)


logger = prepare_logger(__name__, __file__)


Dataset = namedtuple('Dataset', 'provider website data_source_path')


datasets = [
    Dataset(ZiengsProvider, 'ziengs', ZIENGS_PROVIDER_DATASETS_FILEPATH),
    Dataset(OmodaProvider, 'omoda', OMODA_PROVIDER_DATASETS_FILEPATH),
    Dataset(ZalandoProvider, 'zalando', ZALANDO_PROVIDER_DATASETS_FILEPATH,),
]


def writeErrorFile(eid, contents):
    """Write contents to error file to analyze"""
    epath = os.path.abspath('../tmp/error-%s.html' % eid)
    logger.error("Writing error to file: %s" % (epath))

    with codecs.open(epath, 'w', 'utf-8') as efile:
        efile.write(contents)


def importer(datasets):
    """Import Datasets. 

    The importer will run 2-passes over the datasets.
    - First pass will parse and extract "product_detail" information. 
    - Second pass will parse and extract "product_listing" information

    During executing the function will print out it's progress.

    """
    for dataset in datasets:
        Provider = dataset.provider
        data_source_path = os.path.abspath(dataset.data_source_path)

        website_name = dataset.website
        website = models.Website(website=website_name).ensure()
        website_pk = website.pk

        # -------------------------------------------------------------------------
        # First Pass
        # -------------------------------------------------------------------------
        provider = Provider(data_source_path)
        num_of_lines = provider.count_lines()
        ilen = len(str(num_of_lines))

        logger.info("Start importing %s for %s" % (data_source_path, website_name))

        ok, failed = 0, 0
        progress_percentage_hit = []
        for pass_idx, loop in enumerate(['product_detail', 'product_listing'], start=1):
            print("[%s] Looping %s-pass %s" % (website_name, pass_idx, loop))

            reader = provider.read_entry()
            for i, entry in enumerate(reader, start=1):
                if entry.get('page_type') != loop:
                    continue
                try:
                    if process_entry(entry, website_pk=website_pk):
                        ok = ok + 1
                    else:
                        failed = failed + 1
                except Exception as e:
                    failed = failed + 1
                    logger.error("Exception: \n%s\n" % (e))

                # -------------------------------------------------------------------------
                # Show Progress every 10th item or every 2 procent
                # -------------------------------------------------------------------------
                progress_percentage = utils.calcPercentage(i, num_of_lines, round_whole=True)
                if i%20 == 0 or (
                    progress_percentage%2 == 0 and progress_percentage not in progress_percentage_hit
                    ) or i == num_of_lines:
                    print "[%s] %s-pass (%s) @ line %s of %s [%s%%] (ok:%s / fail:%s)" % (
                        website_name, 
                        pass_idx, 
                        loop, 
                        str(i).rjust(ilen), 
                        num_of_lines,
                        str(progress_percentage).rjust(3),
                        str(ok).rjust(ilen),
                        str(failed).rjust(ilen),
                        )
                    progress_percentage_hit.append(progress_percentage)
        stats = {
            "total": ok + failed,
            "ok": ok,
            "failed": failed,
            }

        msg = "Finished importing %s for %s:\nok: %s failed: %s total: %s" % (
            data_source_path, 
            website_name,
            stats['ok'],
            stats['failed'],
            stats['total'],
            )
        logger.info(msg)
        print msg


def process_entry(entry, website_pk):
    """Process entry data.

    params:
        - entry: parsed dataset line
        - website_pk: website Mongo <ObjectId> reference

    The function will process the entry data based on the "page type: product_detail or product_listing".

    A boolean value will be returned to mark the process ended successful or failed.

    The process can also raise exception for unrecoverable failures.

    """
    if not entry['extract_ok']:
        return False

    extracted_data = entry['extracted_data']

    if entry['page_type'] == 'product_detail':
        item = extracted_data['item']
        brand = item['brand_name']
        if brand:
            try:
                brand = models.Brand(brand=brand).ensure()
            except:
                raise

        props = {
            "brand": brand,
            "crawled_at": parse_datetime(entry['crawled_at']),
            "discount_percentage": item['discount_percentage'],
            "name": item['article_name'],
            "on_sale": item['on_sale'],
            "price": item['sale_price'],
            "product_type": item['article_type'],
            "properties": item['extra_props'],
            "sku": item['sku'],
            "url": entry['page_url'],
            "website": website_pk,
            # path=None,
            # listings=[],
        }
        # print(props)
        ## Clean None values
        props = utils.removeNoneValuesFromDict(props)
        # print(props)
        p = models.Product(**props)
        try:
            # p.save()
            p.ensure()
        except models.DuplicateKeyError as error:
            logger.debug("Item already exists: %s - %s - %s [%s]" % (
                props.get("sku"),
                props.get("name"),
                props.get("url"),
                props.get("crawled_at"),
                ))
            return False
        except Exception as e:
            writeErrorFile('detail-%s' % (website_pk), entry['body'])
            raise e

    elif entry['page_type'] == 'product_listing':
        status = True

        number_of_items = extracted_data['number_of_items']
        # number_of_items = len(extracted_data['items'])

        props = {
            "page_number": entry['page_number'],
            "page_listing_size": number_of_items,
            "category": entry['product_category'],
            "sorted_by": entry['ordering'],
            "url": entry['page_url'],
            "crawled_at": parse_datetime(entry['crawled_at']),
            "website": website_pk,
        }

        props = utils.removeNoneValuesFromDict(props)

        pl = models.ProductListingPage(**props)
        try:
            pl.ensure()
            pl_pk = pl.pk
        except models.DuplicateKeyError as error:
            pl = models.ProductListingPage.objects.get(
                dict([(k,v) for k,v in props.items() if k in ('url', 'crawled_at')])
                )
            pl_pk = pl.pk
        except:
            raise

        # -------------------------------------------------------------------------
        # Assign Items
        # -------------------------------------------------------------------------
        total_items = 0
        not_found_products = 0
        listing_added_total = 0
        insufficent_data = 0
        for i, item in enumerate(extracted_data['items']):
            # -------------------------------------------------------------------------
            # Find Item first
            # -------------------------------------------------------------------------
            detail_page_url = item.get('detail_page_url')
            if not detail_page_url:
                continue

            total_items = total_items + 1
            # -------------------------------------------------------------------------
            # Find matching Product based on detail_page_url
            # -------------------------------------------------------------------------
            try:
                product = models.Product.objects.get({'path': detail_page_url})
            except models.Product.DoesNotExist:
                logger.debug("No Product match found for %s" % (detail_page_url))
                not_found_products = not_found_products + 1
                continue

            try:
                li_props = {
                    "position": i+1,
                    "price": item['sale_price'],
                    "on_sale": item['on_sale'],
                    "discount_percentage": item['discount_percentage'],
                    "listing_props": item['listing_props'],
                    "listing": pl_pk,
                }
               # -------------------------------------------------------------------------
                # Create Listing Item
                # -------------------------------------------------------------------------
                li = models.ProductListingItem(**li_props)
            except Exception as e:
                writeErrorFile('listing-%s' % (pl_pk), entry['body'])
                logger.error(e)
                insufficent_data = insufficent_data + 1
                continue

            if any([True for l in product.listings if l.listing._id == pl_pk]):
                # print("Listing already added to product")
                listing_added_total = listing_added_total + 1
                continue

            # -------------------------------------------------------------------------
            # Add New Listing ot Product listings
            # -------------------------------------------------------------------------
            product.listings.append(li)

            try:
                product.save()
                listing_added_total = listing_added_total + 1
            except Exception as e:
                logger.error(e)

                writeErrorFile('listing-%s-%s' % (pl_pk, i), entry['body'])

        # -------------------------------------------------------------------------
        # Debug stats
        # -------------------------------------------------------------------------
        logger.debug("""%s: stats (ok:%s/missing:%s/nodata:%s/total:%s)""" % (
            utils.get_url_path(entry['page_url']),
            listing_added_total,
            not_found_products,
            insufficent_data,
            total_items,
            ))

        return True
    else:
        logger.error("Unknown page_type")
        return False

    return True


# -------------------------------------------------------------------------
# Standalone runner
# -------------------------------------------------------------------------
def main():
    """Main Application"""
    logger.info("Start import task")
    importer(datasets)


if __name__ == "__main__":
    main()
