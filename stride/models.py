from pymodm import (
    MongoModel as PyMongoModel, 
    EmbeddedMongoModel, 
    fields, 
    connect, 
    errors as pymodm_errors,
    )
from pymongo import IndexModel, TEXT
from pymongo.errors import DuplicateKeyError
from bson.objectid import ObjectId
from bson.json_util import dumps
import config
import utils
from urlparse import urlparse
from pymodm.vendor import parse_datetime
import pdb
import re


# -------------------------------------------------------------------------
# Make connection to the database.
# -------------------------------------------------------------------------
connect(utils.convertConfigIntoMongoURI(config))


class MongoModel(PyMongoModel):
    """Extending Default MongoModel. Adding extra functions"""
    # -------------------------------------------------------------------------
    # Helper Functions
    # -------------------------------------------------------------------------
    def to_dict(self, *args, **kwargs):
        """Convert Mongo item structure into a python dictionary"""
        return model_to_dict(self, *args, **kwargs)

    def to_json(self, *args, **kwargs):
        """Convert Mongo item structure into a json variable"""
        return dumps(self.to_dict(*args, **kwargs))


class EnsureEntry(object):
    """Class to add ensure entity are created"""
    ensure_fields = []

    def ensure(self, *args, **kwargs):
        """Function to ensure the model instance exists.

        Default save function will raise exception if already exists.
        
        This function will first check if similar entry exists and return existing
        entry.
        
        If entry does not exists the function will create an entry and return the created entry
        """
        try:
            self.full_clean()
            ensure_fields = self.__class__.ensure_fields
            lookup_props = self.to_son()

            if len(ensure_fields) > 0:
                lookup_props = dict([i for i in lookup_props.items() if i[0] in ensure_fields])

            item = self.__class__.objects.get(lookup_props)
            self.pk = item.pk
            return item
        except pymodm_errors.DoesNotExist as e:
            try:
                return self.save(*args, **kwargs)
            except DuplicateKeyError as e:
                item = self.__class__.objects.get(self.to_son())
                self.pk = item.pk
        return self

    @classmethod
    def lookup(cls, value):
        """Helper function to lookup up entity by other unique fields then just the primary key field"""
        manager = cls._find_manager()
        if manager:
            lookup_value = utils.cleanStringForUID(value)
            for field_id in cls._mongometa.fields_attname_dict.keys():
                field = getattr(cls, field_id, None)
                if field and getattr(field, '_ensure_lookup_field', False):
                    try:
                        match = manager.get({
                            field_id: lookup_value
                        })
                    except pymodm_errors.DoesNotExist as e:
                        continue
                    if match:
                        return match
            # return manager.get({cls._ensure_field: )
        return None


class EnsureReferencedLookup(object):
    """Class to help lookup references of fields before validating the fields"""

    def clean_fields(self, *args, **kwargs):
        """Adjust Fields before cleaning"""

        # -------------------------------------------------------------------------
        # Get all attribute and mongo field of the designed structure
        # -------------------------------------------------------------------------
        for attr_id, field in self._mongometa.fields_attname_dict.items():
            if isinstance(field, fields.ReferenceField):
                # -------------------------------------------------------------------------
                # Look for set '_ref_lookup'
                # -------------------------------------------------------------------------
                _ref_lookup = getattr(field, '_ref_lookup', False)
                if not _ref_lookup:
                    continue

                set_value = getattr(self, attr_id, None)  # Current set value for attribute
                ref_model = getattr(field, 'related_model', None)  # Get referenced model

                if set_value and ref_model:
                    # -------------------------------------------------------------------------
                    # If current set value is not an Object Id or Instance of referenced model
                    # then do a lookup for referenced model instance with current set value and
                    # set results primary key as new set value to be processed
                    # -------------------------------------------------------------------------
                    if not isinstance(set_value, (ObjectId, ref_model)):
                        match = ref_model.lookup(set_value)
                        try:
                            match = match.pk
                        except:
                            pass
                        if match is not None:
                            setattr(self, attr_id, match)

        super(EnsureReferencedLookup, self).clean_fields(*args, **kwargs)


class Website(
    EnsureEntry,
    MongoModel
    ):
    """Website Model"""
    ensure_fields = ['website_uid']

    # -------------------------------------------------------------------------
    # Model Field Definitions
    # -------------------------------------------------------------------------
    website_uid = fields.CharField(required=True)
    website_uid._ensure_lookup_field = True  # Make field a lookup field
    website = fields.CharField(required=True)
    uri = fields.URLField()

    # -------------------------------------------------------------------------
    # Document Version to keep track of model migrations
    # -------------------------------------------------------------------------
    doc_version = fields.FloatField(required=True, default=1.0)

    def clean_fields(self, *args, **kwargs):
        """Adjust Fields before cleaning"""
        # -------------------------------------------------------------------------
        # Create Unique Website ID if not set
        # -------------------------------------------------------------------------
        if not self.website_uid and self.website:
            self.website_uid = self.website

        # -------------------------------------------------------------------------
        # Clean Website ID
        # -------------------------------------------------------------------------
        if self.website_uid:
            self.website_uid = utils.cleanStringForUID(self.website_uid)

        # -------------------------------------------------------------------------
        # Continue original function
        # -------------------------------------------------------------------------
        super(Website, self).clean_fields(*args, **kwargs)

    class Meta:
        """Meta class for Website Model"""
        collection_name = "websites"

    # -------------------------------------------------------------------------
    # Helper Functions
    # -------------------------------------------------------------------------
    @classmethod
    def get_lookup_arguments(cls, website_id):
        """Convert website_id into valid ObjectId if exists"""
        lookup_args = {}
        if check_is_valid_object_id(website_id):
            lookup_args['_id'] = ObjectId(website_id)
        elif isinstance(website_id, basestring):
            found = cls.objects.get({'website_uid': website_id.lower().strip()})
            lookup_args['_id'] = found.pk
        else:
            raise ValueError("'%s' is not valid website lookup value" % (website_id))
        return lookup_args


class Brand(
    EnsureEntry,
    MongoModel
    ):
    """Brand Model"""
    ensure_fields = ['brand_uid']

    # -------------------------------------------------------------------------
    # Model Field Definitions
    # -------------------------------------------------------------------------
    brand_uid = fields.CharField(required=True)
    brand_uid._ensure_lookup_field = True  # Make field a lookup field
    brand = fields.CharField(required=True)

    # -------------------------------------------------------------------------
    # Document Version to keep track of model migrations
    # -------------------------------------------------------------------------
    doc_version = fields.FloatField(required=True, default=1.0)

    def clean_fields(self, *args, **kwargs):
        """Adjust Fields before cleaning"""
        # -------------------------------------------------------------------------
        # Create Unique Website ID if not set
        # -------------------------------------------------------------------------
        if not self.brand_uid and self.brand:
            self.brand_uid = self.brand

        # -------------------------------------------------------------------------
        # Clean Brand ID
        # -------------------------------------------------------------------------
        if self.brand_uid:
            self.brand_uid = utils.cleanStringForUID(self.brand_uid)

        # -------------------------------------------------------------------------
        # Continue original function
        # -------------------------------------------------------------------------
        super(Brand, self).clean_fields(*args, **kwargs)

    class Meta:
        """Meta class for Brand Model"""
        collection_name = "brands"

    # -------------------------------------------------------------------------
    # Helper Functions
    # -------------------------------------------------------------------------
    @classmethod
    def get_lookup_arguments(cls, brand_id):
        """Convert brand_id into valid ObjectId if exists"""
        lookup_args = {}
        if check_is_valid_object_id(brand_id):
            lookup_args['_id'] = ObjectId(brand_id)
        elif isinstance(brand_id, basestring):
            found = cls.objects.get({'brand_uid': brand_id.lower().strip()})
            lookup_args['_id'] = found.pk
        else:
            raise ValueError("'%s' is not valid brand lookup value" % (brand_id))
        return lookup_args


class ProductListingPage(
    EnsureEntry,
    EnsureReferencedLookup,
    MongoModel
    ):
    """Product Listing Page Model"""
    ensure_fields = ['url', 'crawled_at']

    # -------------------------------------------------------------------------
    # Model Field Definitions
    # -------------------------------------------------------------------------
    page_number = fields.IntegerField(required=True, default=1)
    page_listing_size = fields.IntegerField(required=True, default=1)

    category = fields.ListField(field=fields.CharField(required=True))
    sorted_by = fields.CharField(required=True)
    url = fields.CharField(required=True)
    crawled_at = fields.DateTimeField(required=True)

    # -------------------------------------------------------------------------
    # Reference Website Model with Cascade if referenced model is deleted
    # -------------------------------------------------------------------------
    website = fields.ReferenceField(
        Website, required=True, verbose_name="Website", on_delete=fields.ReferenceField.CASCADE
        )
    website._ref_lookup = True  # Enable reference lookup if set value is string

    # -------------------------------------------------------------------------
    # Document Version to keep track of model migrations
    # -------------------------------------------------------------------------
    doc_version = fields.FloatField(required=True, default=1.0)

    class Meta:
        """Meta class for Product Listing Page Model"""
        collection_name = "product_listings"

        # -------------------------------------------------------------------------
        # Create Unique Index
        # -------------------------------------------------------------------------
        indexes = [
            IndexModel([('url', 1), ('crawled_at', 1)], name="ulisting_idx", unique=True),
        ]

    # -------------------------------------------------------------------------
    # Patch delete to also remove related listings, because on_delete for
    # EmbeddedDocumentListField >  EmbeddedMongoModel > ReferenceField
    # does not work
    # -------------------------------------------------------------------------
    def delete(self):
        """Override delete function to patch missing feature"""
        qs = self._qs

        # -------------------------------------------------------------------------
        # Build Reference List to find Matching products containing ProductListingItem
        # with this ProductListingPage Reference
        # -------------------------------------------------------------------------
        refs = [doc['_id'] for doc in self._qs.values()]

        # -------------------------------------------------------------------------
        # Get Manager
        # -------------------------------------------------------------------------
        product_manager = Product._mongometa.default_manager

        # -------------------------------------------------------------------------
        # Find all matching
        # -------------------------------------------------------------------------
        related_qs = product_manager.raw({"listings.listing": {"$in": refs}}).values()
        if related_qs.count() > 0:
            # -------------------------------------------------------------------------
            # Loop Products with matching ProductListingPage reference
            # -------------------------------------------------------------------------
            for p_ref in related_qs.all():
                # -------------------------------------------------------------------------
                # Get Product Item
                # -------------------------------------------------------------------------
                prod = product_manager.get({"_id": p_ref['_id']})

                # -------------------------------------------------------------------------
                # Pop Matching ProductListPages
                # -------------------------------------------------------------------------
                changed = False
                for l in prod.listings:
                    if l.listing.pk in refs:
                        index = prod.listings.index(l)
                        prod.listings.pop(index)
                        changed = True

                # -------------------------------------------------------------------------
                # Save Changed product item
                # -------------------------------------------------------------------------
                if changed:
                    prod.save()

        super(ProductListingPage, self).delete()


class ProductListingItem(EmbeddedMongoModel):
    """Embedded Product Listing Model"""
    position = fields.IntegerField(required=True, default=1)
    price = fields.FloatField(required=True)
    on_sale = fields.BooleanField(required=True, default=False)
    discount_percentage = fields.FloatField(default=0.0)

    # -------------------------------------------------------------------------
    # Contain interesting properties
    # -------------------------------------------------------------------------
    listing_props = fields.DictField()

    # -------------------------------------------------------------------------
    # Reference Listing
    # on_delete not supported for embedded documents
    # Fixed with customizing delete function
    # -------------------------------------------------------------------------
    listing = fields.ReferenceField(
        ProductListingPage, required=True#, on_delete=fields.ReferenceField.CASCADE
    )

    # -------------------------------------------------------------------------
    # Document Version to keep track of model migrations
    # -------------------------------------------------------------------------
    doc_version = fields.FloatField(required=True, default=1.0)

    def clean(self):
        """Custom Clean values."""

        # -------------------------------------------------------------------------
        # Determine if discount was set and that on_sale is correctly set
        # -------------------------------------------------------------------------
        if self.discount_percentage and isinstance(self.discount_percentage, float) and self.discount_percentage > 0.0:
            self.on_sale = True

        # -------------------------------------------------------------------------
        # Make sure that discount_percentage is zero if not on_sale
        # -------------------------------------------------------------------------
        if not self.on_sale:
            self.discount_percentage = 0.0


class Product(
    EnsureEntry,
    EnsureReferencedLookup,
    MongoModel
    ):
    """Product Model"""
    ensure_fields = ['sku', 'website', 'crawled_at']

    # -------------------------------------------------------------------------
    # Model Field Definitions
    # -------------------------------------------------------------------------
    sku = fields.CharField(required=True)
    name = fields.CharField(required=True)
    product_type = fields.CharField(default='')
    url = fields.URLField(required=True)
    path = fields.CharField()
    crawled_at = fields.DateTimeField(required=True)
    price = fields.FloatField()
    on_sale = fields.BooleanField(required=True, default=False)
    discount_percentage = fields.FloatField(default=0.0)
    properties = fields.DictField(required=True, default={})

    # -------------------------------------------------------------------------
    # Reference Brand Model with Cascade if referenced model is deleted
    # -------------------------------------------------------------------------
    brand = fields.ReferenceField(
        Brand, required=True, verbose_name="Brand", on_delete=fields.ReferenceField.CASCADE
        )
    brand._ref_lookup = True  # Enable reference lookup if set value is string

    # -------------------------------------------------------------------------
    # Reference Website Model with Cascade if referenced model is deleted
    # -------------------------------------------------------------------------
    website = fields.ReferenceField(
        Website, required=True, verbose_name="Website", on_delete=fields.ReferenceField.CASCADE
        )
    website._ref_lookup = True  # Enable reference lookup if set value is string

    listings = fields.EmbeddedDocumentListField(
        ProductListingItem, default=[], required=False,
        )

    # -------------------------------------------------------------------------
    # Document Version to keep track of model migrations
    # -------------------------------------------------------------------------
    doc_version = fields.FloatField(required=True, default=1.0)

    def clean(self):
        """Clean Values"""
        # -------------------------------------------------------------------------
        # Determine if discount was set and that on_sale is correctly set
        # -------------------------------------------------------------------------
        if self.discount_percentage and isinstance(self.discount_percentage, float) and self.discount_percentage > 0.0:
            self.on_sale = True

        # -------------------------------------------------------------------------
        # Make sure that discount_percentage is zero if not on_sale
        # -------------------------------------------------------------------------
        if not self.on_sale:
            self.discount_percentage = 0.0

        # -------------------------------------------------------------------------
        # Extract path from url if path wasn't set
        # -------------------------------------------------------------------------
        if self.url and not self.path:
            parsed_url = urlparse(self.url)
            self.path = parsed_url.path

        # # -------------------------------------------------------------------------
        # # Clean Extra Properties
        # # -------------------------------------------------------------------------
        # clean_props = {}
        # for pk, pv in self.properties.items():
        #     clean_props[utils.cleanKeyForDict(pk)] = pv
        # self.properties = clean_props

    class Meta:
        """Meta class for Product Model"""
        collection_name = "products"

        # # -------------------------------------------------------------------------
        # # Create Unique Index
        # # -------------------------------------------------------------------------
        # indexes = [
        #     IndexModel([('sku', TEXT), ('brand', TEXT), ('website', TEXT)], name="uprod_idx", unique=True),
        # ]

    # -------------------------------------------------------------------------
    # Helper Functions
    # -------------------------------------------------------------------------
    @classmethod
    def get_lookup_arguments(cls, product_id, website_id=None):
        """Convert product_id into valid ObjectId if exists"""
        lookup_args = {}
        if check_is_valid_object_id(product_id):
            lookup_args['_id'] = ObjectId(product_id)
        elif isinstance(product_id, basestring) and check_is_valid_object_id(website_id):
            regexp_lookup = r'^(%s)$' % (re.escape(product_id))
            found = cls.objects.get({
                'sku': re.compile(regexp_lookup, re.IGNORECASE),
                'website': ObjectId(website_id),
                })
            lookup_args['_id'] = found.pk
        else:
            raise ValueError("'%s' is not valid product lookup value" % (product_id))
        return lookup_args


def model_to_dict(item, **kwargs):
    """Convert Mongo Model entity into python dictionary"""
    # -------------------------------------------------------------------------
    # Boolean whether or not to include the primary key. Most cases the '_id'
    # -------------------------------------------------------------------------
    include_pk = kwargs.get('include_pk', True)
    # -------------------------------------------------------------------------
    # Boolean to convert ObjectId into string variable instead of ObjectId
    # -------------------------------------------------------------------------
    stringify_objectid = kwargs.get('stringify_objectid', True)

    # -------------------------------------------------------------------------
    # Exclude fields from being included in the python dictionary
    # -------------------------------------------------------------------------
    exclude_fields = kwargs.get('exclude_fields', ['doc_version'])

    data = {}
    try:
        if isinstance(item, list):
            data = []
            for i in item:
                # -------------------------------------------------------------------------
                # Convert entries within the list
                # -------------------------------------------------------------------------
                data.append(model_to_dict(i))
            return data
        else:
            for field in item._mongometa.get_fields():
                if not include_pk and field.primary_key and item._mongometa.implicit_id:
                    continue
                field_id = field.attname
                if field_id in exclude_fields:
                    continue

                default = getattr(field, 'default', None)
                # field_value = getattr(item, field_id, default)
                field_value = getattr(item, field_id, default)

                # -------------------------------------------------------------------------
                # Handel Reference Object and make python dictionary save
                # -------------------------------------------------------------------------
                try:
                    if issubclass(field.__class__, fields.RelatedModelFieldsBase):
                        if isinstance(field_value, (list, fields.EmbeddedDocumentListField)):
                            list_values = []
                            for list_item in field_value:
                                list_values.append(model_to_dict(list_item, **kwargs))
                            field_value = list_values
                        else:
                            field_value = model_to_dict(field_value, **kwargs)
                except:
                    raise

                if isinstance(field_value, ObjectId) and stringify_objectid:
                    field_value = str(field_value)

                data[field_id] = field_value
    except Exception as e:
        raise

    return data


def check_is_valid_object_id(value):
    """Check if value is a valid ObjectId or valid hex-string"""
    return ObjectId.is_valid(value)
