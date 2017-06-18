import unicodedata
import os.path
import re
from math import ceil
from urlparse import urlparse

# -------------------------------------------------------------------------
# Pre-Compiled Regexp
# -------------------------------------------------------------------------
CLEAN_UID_REGEXP = re.compile(r'[^.\ a-zA-Z0-9_-]')
CLEAN_DICT_KEY_REGEXP = re.compile(r'[^a-zA-Z0-9_-]')


def validate_path(fpath):
    """Check is file path is a file"""
    if not fpath or not os.path.isfile(fpath):
        raise ValueError("Could not find %s" % (fpath))
    return os.path.abspath(fpath)


def convert_html_price_to_float(value, default=None):
    """Convert value input into float"""
    try:
        if isinstance(value, basestring):
            # -------------------------------------------------------------------------
            # Remove all except [0-9,.]. Replace all comma's with dots. split once on 
            # most right dot. join all entry and remove all remain dots
            # -------------------------------------------------------------------------
            value = ".".join([re.sub(r'[^0-9]','', n) for n in re.sub(r',', '.', re.sub(r'[^0-9.,]', '', value)).rsplit('.', 1)])
        return float(value)
    except:
        return default


def convertConfigIntoMongoURI(config):
    """Convert config object properties into mongodb uri"""
    return "mongodb://%(host)s:%(port)d/%(db_name)s" % {
        "host": getattr(config, 'MONGO_HOST', 'localhost'),
        "port": getattr(config, 'MONGO_PORT', 27017),
        "db_name": getattr(config, 'MONGO_DBNAME', "shoecase"),
    }


def cleanStringForUID(value):
    """Clean string into lowercased string with nospecial characters"""
    try:
        # -------------------------------------------------------------------------
        # Remove all exotic characters and if possible replace with ascii equivalent
        # -------------------------------------------------------------------------
        if not isinstance(value, unicode):
            value = unicode(value, 'utf-8')
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
    except:
        value = str(value)

    # -------------------------------------------------------------------------
    # Lower Case and remove multiple, leading and trailing spaces
    # -------------------------------------------------------------------------
    return re.sub(' +', ' ', CLEAN_UID_REGEXP.sub('', value.lower().strip()))


def cleanKeyForDict(value):
    """Clean string into lowercased string with nospecial characters"""
    try:
        # -------------------------------------------------------------------------
        # Remove all exotic characters and if possible replace with ascii equivalent
        # -------------------------------------------------------------------------
        if not isinstance(value, unicode):
            value = unicode(value, 'utf-8')
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
    except:
        value = str(value)

    # -------------------------------------------------------------------------
    # Lower Case and remove multiple, leading and trailing spaces
    # -------------------------------------------------------------------------
    return re.sub(' +', '', CLEAN_DICT_KEY_REGEXP.sub('', value.lower().strip()))


def calcDiscountPercentage(new_price, old_price):
    """Calculate discount percentage"""
    if isinstance(new_price, (int, float)) and isinstance(old_price, (int, float)) and \
        old_price != 0.0 and old_price != new_price:
        try:
            return float((1.0 - (new_price / old_price)) * 100.0)
        except ZeroDivisionError:
            pass
    # -------------------------------------------------------------------------
    # Default return 0.0
    # -------------------------------------------------------------------------
    return 0.0


def removeNoneValuesFromDict(d):
    """Remove None values from dictionary"""
    return dict([(k, v) for k, v in d.items() if v is not None])


def calcPercentage(part, total, round_whole=False):
    percentage = (float(part)/float(total))*100.0
    if round_whole:
        percentage = int(ceil(percentage))
    return percentage


def get_url_path(url):
    """Get URL Path from url"""
    try:
        parsed_url = urlparse(url)
        return parsed_url.path
    except:
        return url
