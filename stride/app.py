# -*- coding: utf-8 -*-
from flask import Flask, jsonify, url_for, redirect, request, abort
from flask_restful import Resource, Api
import config
import models
from functools import partial


app = Flask(__name__)
app.config.from_object(config)

# -------------------------------------------------------------------------
# Definitions
# -------------------------------------------------------------------------
API_ITEM_URL_KEY = '_api_item_url'
API_RESULTS_LIMITER = 100


api = Api(app)


def cap_limit(limit):
    """Restrict Max results"""
    if API_RESULTS_LIMITER:
        if limit == 0 or limit > API_RESULTS_LIMITER:
            limit = API_RESULTS_LIMITER
    return limit


# -------------------------------------------------------------------------
# Index
# -------------------------------------------------------------------------
class Index(Resource):
    def get(self):
        return redirect(url_for("websites"))

api.add_resource(Index, '/', endpoint="index")


# -------------------------------------------------------------------------
# Website
# -------------------------------------------------------------------------
class WebsiteListAPI(Resource):
    def get(self):
        websites = []
        for website in models.Website.objects.all():
            data = website.to_dict()

            # -------------------------------------------------------------------------
            # Assign API item URL
            # -------------------------------------------------------------------------
            data[API_ITEM_URL_KEY] = url_for('website', website_id=data['_id'])

            websites.append(data)

        return jsonify({"websites": websites})

api.add_resource(WebsiteListAPI, '/api/websites', endpoint="websites")


class WebsiteAPI(Resource):
    def get(self, website_id):
        lookup_args = models.Website.get_lookup_arguments(website_id=website_id)
        try:
            website = models.Website.objects.get(lookup_args).to_dict()

            # -------------------------------------------------------------------------
            # Assign API item URL
            # -------------------------------------------------------------------------
            website[API_ITEM_URL_KEY] = url_for('website', website_id=website['_id'])
        except:
            abort(404)

        return jsonify(website)

api.add_resource(WebsiteAPI, '/api/websites/<string:website_id>', endpoint="website")


# -------------------------------------------------------------------------
# Website Products
# -------------------------------------------------------------------------
class WebsiteProductListAPI(Resource):
    def get(self, website_id, limit=10, skip=0):
        lookup_args = models.Website.get_lookup_arguments(website_id=website_id)
        results = models.Product.objects.raw({'website': lookup_args['_id']})
        total = results.all().count()
        limit = cap_limit(limit)

        products = []
        for product in results.skip(skip).limit(limit):
            data = product.to_dict()

            # -------------------------------------------------------------------------
            # Assign API item URL
            # -------------------------------------------------------------------------
            data[API_ITEM_URL_KEY] = url_for('product', product_id=data['_id'])

            products.append(data)

        return jsonify({
            "limit": limit,
            "products": products,
            "skip": skip,
            "total": total,
            })

# -------------------------------------------------------------------------
# Created shorthand
# -------------------------------------------------------------------------
WPLA = partial(api.add_resource, WebsiteProductListAPI)
WPLA('/api/websites/<string:website_id>/products', endpoint="website_products")
WPLA('/api/websites/<string:website_id>/products/<int:skip>', endpoint="website_products_offset")
WPLA('/api/websites/<string:website_id>/products/<int:skip>/<int:limit>'
    , endpoint="website_products_offset_limited")


# -------------------------------------------------------------------------
# Brands
# -------------------------------------------------------------------------
class BrandListAPI(Resource):
    def get(self, limit=10, skip=0):
        results = models.Brand.objects.all().order_by([('brand', 1)])
        total = results.count()
        limit = cap_limit(limit)

        brands = []
        for brand in results.skip(skip).limit(limit):
            data = brand.to_dict()
            brands.append(data)

        return jsonify({
            "brands": brands,
            "limit": limit,
            "skip": skip,
            "total": total,
            })

# -------------------------------------------------------------------------
# Create Shorthand
# -------------------------------------------------------------------------
BLA = partial(api.add_resource, BrandListAPI)
BLA('/api/brands', endpoint="brands")
BLA('/api/brands/<int:skip>', endpoint="brands_offset")
BLA('/api/brands/<int:skip>/<int:limit>', endpoint="brands_offset_limited")

class BrandProductListAPI(Resource):
    def get(self, brand_id, limit=10, skip=0):

        lookup_args = models.Brand.get_lookup_arguments(brand_id)
        results = models.Product.objects.raw({'brand': lookup_args['_id']})
        total = results.count()
        limit = cap_limit(limit)

        products = []
        for product in results.skip(skip).limit(limit):
            data = product.to_dict()

            # -------------------------------------------------------------------------
            # Assign API item URL
            # -------------------------------------------------------------------------
            data[API_ITEM_URL_KEY] = url_for('product', product_id=data['_id'])

            products.append(data)

        return jsonify({
            "limit": limit,
            "products": products,
            "skip": skip,
            "total": total,
            })


# -------------------------------------------------------------------------
# Create Shorthand
# -------------------------------------------------------------------------
BPLA = partial(api.add_resource, BrandProductListAPI)
BPLA('/api/brand/<string:brand_id>/products', endpoint="brand_products")
BPLA('/api/brand/<string:brand_id>/products/<int:skip>', endpoint="brand_products_offset")
BPLA('/api/brand/<string:brand_id>/products/<int:skip>/<int:limit>'
    , endpoint="brand_products_offset_limited")


# -------------------------------------------------------------------------
# Products
# -------------------------------------------------------------------------
class ProductAPI(Resource):
    def get(self, product_id, website_id=None):
        try:
            if website_id:
                # -------------------------------------------------------------------------
                # Validate website_id param
                # -------------------------------------------------------------------------
                try:
                    website_id = models.Website.get_lookup_arguments(website_id).get('_id', None)
                except:
                    website_id = None

            product_lookup_args = models.Product.get_lookup_arguments(product_id, website_id=website_id)
            product = models.Product.objects.get({'_id': product_lookup_args['_id']}).to_dict()

            # -------------------------------------------------------------------------
            # Assign API item URL
            # -------------------------------------------------------------------------
            product[API_ITEM_URL_KEY] = url_for('product', product_id=product['_id'])
        except:
            abort(404)

        return jsonify(product)

api.add_resource(ProductAPI, '/api/product/<string:product_id>', endpoint="product")
api.add_resource(ProductAPI, '/api/website/<string:website_id>/product/<string:product_id>', endpoint="website_product")


def main():
    """Main application"""
    app.run(
        host=app.config.get('HOST'),
        port=app.config.get('PORT'),
        )


if __name__ == '__main__':
    main()
