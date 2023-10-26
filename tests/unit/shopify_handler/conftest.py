import json

import pandas
import pytest
import requests
import shopify

from mindsdb.integrations.handlers.shopify_handler import Handler
from mindsdb.integrations.handlers.shopify_handler.shopify_tables import (ProductsTable, CustomersTable, OrdersTable,
                                                                          LocationTable, CarrierServiceTable,
                                                                          SalesChannelTable, ShippingZoneTable)

connection_data = {
    'shop_url': 'some-shopify-store.myshopify.com',
    'access_token': 'shpat_abcdefghijklmnopq',
    'yotpo_app_key': 'some_yotpo_app_key',
    'yotpo_access_token': 'some_yotpo_access_token'
}


@pytest.fixture
def empty_handler():
    handler = Handler('shopify_handler')
    return handler


@pytest.fixture
def shopify_handler():
    handler = Handler('shopify_handler', connection_data=connection_data)
    return handler


@pytest.fixture
def shopify_session():
    session = shopify.Session(
        shop_url=connection_data['shop_url'],
        version="2021-10",
        token=connection_data['access_token']
    )
    return session


@pytest.fixture
def response_ok():
    response = requests.Response()
    response.status_code = 200
    return response


@pytest.fixture
def response_400():
    response = requests.Response()
    response.status_code = 400
    return response


@pytest.fixture
def sample_orders():
    order = """{
        "id": 5522106319070,
        "admin_graphql_api_id": "gid://shopify/Order/5522106319070",
        "app_id": 1608003,
        "browser_ip": null,
        "buyer_accepts_marketing": false,
        "cancel_reason": null,
        "cancelled_at": null,
        "cart_token": null,
        "checkout_id": null,
        "checkout_token": null,
        "client_details": null,
        "closed_at": null,
        "company": null,
        "confirmation_number": "J9IFFEHAP",
        "confirmed": true,
        "contact_email": "egnition_sample_99@egnition.com",
        "created_at": "2023-10-13T16:03:35-04:00",
        "currency": "INR",
        "current_subtotal_price": "0.10",
        "current_subtotal_price_set": {
            "shop_money": {
                "amount": "0.10",
                "currency_code": "INR"
            },
            "presentment_money": {
                "amount": "0.10",
                "currency_code": "INR"
            }
        },
        "current_total_additional_fees_set": null,
        "current_total_discounts": "0.00",
        "current_total_discounts_set": {
            "shop_money": {
                "amount": "0.00",
                "currency_code": "INR"
            },
            "presentment_money": {
                "amount": "0.00",
                "currency_code": "INR"
            }
        },
        "current_total_duties_set": null,
        "current_total_price": "0.10",
        "current_total_price_set": {
            "shop_money": {
                "amount": "0.10",
                "currency_code": "INR"
            },
            "presentment_money": {
                "amount": "0.10",
                "currency_code": "INR"
            }
        },
        "current_total_tax": "0.00",
        "current_total_tax_set": {
            "shop_money": {
                "amount": "0.00",
                "currency_code": "INR"
            },
            "presentment_money": {
                "amount": "0.00",
                "currency_code": "INR"
            }
        },
        "customer_locale": null,
        "device_id": null,
        "discount_codes": [],
        "email": "egnition_sample_99@egnition.com",
        "estimated_taxes": false,
        "financial_status": "paid",
        "fulfillment_status": null,
        "landing_site": null,
        "landing_site_ref": null,
        "location_id": null,
        "merchant_of_record_app_id": null,
        "name": "#1019",
        "note": null,
        "note_attributes": [],
        "number": 19,
        "order_number": 1019,
        "order_status_url": "https://quickstart-6d81c745.myshopify.com/67008856286/orders/29ab764a9e620cd9cc590cbc08695390/authenticate?key=4844895014eade1e3fa60fb9bc04e50f",
        "original_total_additional_fees_set": null,
        "original_total_duties_set": null,
        "payment_gateway_names": [
            "bogus"
        ],
        "phone": null,
        "po_number": null,
        "presentment_currency": "INR",
        "processed_at": "2023-10-13T16:03:35-04:00",
        "reference": null,
        "referring_site": null,
        "source_identifier": null,
        "source_name": "1608003",
        "source_url": null,
        "subtotal_price": "0.10",
        "subtotal_price_set": {
            "shop_money": {
                "amount": "0.10",
                "currency_code": "INR"
            },
            "presentment_money": {
                "amount": "0.10",
                "currency_code": "INR"
            }
        },
        "tags": "egnition-sample-data",
        "tax_exempt": false,
        "tax_lines": [],
        "taxes_included": false,
        "test": false,
        "token": "29ab764a9e620cd9cc590cbc08695390",
        "total_discounts": "0.00",
        "total_discounts_set": {
            "shop_money": {
                "amount": "0.00",
                "currency_code": "INR"
            },
            "presentment_money": {
                "amount": "0.00",
                "currency_code": "INR"
            }
        },
        "total_line_items_price": "0.10",
        "total_line_items_price_set": {
            "shop_money": {
                "amount": "0.10",
                "currency_code": "INR"
            },
            "presentment_money": {
                "amount": "0.10",
                "currency_code": "INR"
            }
        },
        "total_outstanding": "0.00",
        "total_price": "0.10",
        "total_price_set": {
            "shop_money": {
                "amount": "0.10",
                "currency_code": "INR"
            },
            "presentment_money": {
                "amount": "0.10",
                "currency_code": "INR"
            }
        },
        "total_shipping_price_set": {
            "shop_money": {
                "amount": "0.00",
                "currency_code": "INR"
            },
            "presentment_money": {
                "amount": "0.00",
                "currency_code": "INR"
            }
        },
        "total_tax": "0.00",
        "total_tax_set": {
            "shop_money": {
                "amount": "0.00",
                "currency_code": "INR"
            },
            "presentment_money": {
                "amount": "0.00",
                "currency_code": "INR"
            }
        },
        "total_tip_received": "0.00",
        "total_weight": 0,
        "updated_at": "2023-10-14T07:23:24-04:00",
        "user_id": null,
        "billing_address": {
            "first_name": "Channing",
            "address1": "954 Dui. St.",
            "phone": "+61423473176",
            "city": "Leganes",
            "zip": "16205",
            "province": null,
            "country": "Australia",
            "last_name": "Marquez",
            "address2": null,
            "company": null,
            "latitude": null,
            "longitude": null,
            "name": "Channing Marquez",
            "country_code": "AU",
            "province_code": null
        },
        "customer": {
            "id": 7092329709790,
            "email": "egnition_sample_99@egnition.com",
            "accepts_marketing": false,
            "created_at": "2023-10-13T15:58:59-04:00",
            "updated_at": "2023-10-14T08:20:31-04:00",
            "first_name": "Channing",
            "last_name": "Marquez",
            "state": "disabled",
            "note": null,
            "verified_email": true,
            "multipass_identifier": null,
            "tax_exempt": false,
            "phone": "+61423476902",
            "email_marketing_consent": {
                "state": "not_subscribed",
                "opt_in_level": "single_opt_in",
                "consent_updated_at": null
            },
            "sms_marketing_consent": {
                "state": "not_subscribed",
                "opt_in_level": "single_opt_in",
                "consent_updated_at": null,
                "consent_collected_from": "OTHER"
            },
            "tags": "egnition-sample-data, VIP",
            "currency": "INR",
            "accepts_marketing_updated_at": "2023-10-13T15:58:59-04:00",
            "marketing_opt_in_level": null,
            "tax_exemptions": [],
            "admin_graphql_api_id": "gid://shopify/Customer/7092329709790",
            "default_address": {
                "id": 8635506491614,
                "customer_id": 7092329709790,
                "first_name": "Channing",
                "last_name": "Marquez",
                "company": null,
                "address1": "954 Dui. St.",
                "address2": null,
                "city": "Leganes",
                "province": null,
                "country": "Australia",
                "zip": "16205",
                "phone": "+61423473176",
                "name": "Channing Marquez",
                "province_code": null,
                "country_code": "AU",
                "country_name": "Australia",
                "default": true
            }
        },
        "discount_applications": [],
        "fulfillments": [],
        "line_items": [
            {
                "id": 13676730286302,
                "admin_graphql_api_id": "gid://shopify/LineItem/13676730286302",
                "attributed_staffs": [],
                "fulfillable_quantity": 1,
                "fulfillment_service": "manual",
                "fulfillment_status": null,
                "gift_card": false,
                "grams": 0,
                "name": "VANS |AUTHENTIC | LO PRO | BURGANDY/WHITE - 5 / burgandy",
                "price": "0.10",
                "price_set": {
                    "shop_money": {
                        "amount": "0.10",
                        "currency_code": "INR"
                    },
                    "presentment_money": {
                        "amount": "0.10",
                        "currency_code": "INR"
                    }
                },
                "product_exists": true,
                "product_id": 8153099141342,
                "properties": [],
                "quantity": 1,
                "requires_shipping": true,
                "sku": "VN-01-burgandy-5",
                "taxable": true,
                "title": "VANS |AUTHENTIC | LO PRO | BURGANDY/WHITE",
                "total_discount": "0.00",
                "total_discount_set": {
                    "shop_money": {
                        "amount": "0.00",
                        "currency_code": "INR"
                    },
                    "presentment_money": {
                        "amount": "0.00",
                        "currency_code": "INR"
                    }
                },
                "variant_id": 44406949216478,
                "variant_inventory_management": "shopify",
                "variant_title": "5 / burgandy",
                "vendor": "VANS",
                "tax_lines": [],
                "duties": [],
                "discount_allocations": []
            }
        ],
        "payment_terms": null,
        "refunds": [],
        "shipping_address": {
            "first_name": "Channing",
            "address1": "954 Dui. St.",
            "phone": "+61423473176",
            "city": "Leganes",
            "zip": "16205",
            "province": null,
            "country": "Australia",
            "last_name": "Marquez",
            "address2": null,
            "company": null,
            "latitude": null,
            "longitude": null,
            "name": "Channing Marquez",
            "country_code": "AU",
            "province_code": null
        },
        "shipping_lines": []
    }"""
    order = json.loads(order)
    return [order]


@pytest.fixture
def products_table(shopify_handler):
    return ProductsTable(shopify_handler)


@pytest.fixture
def sample_products():
    products = """
    [
    {
      "id": 8153100648670,
      "title": "ADIDAS | CLASSIC BACKPACK",
      "body_html": "This women's backpack has a glam look, thanks to a faux-leather build with an allover fur print. The front zip pocket keeps small things within reach, while an interior divider reins in potential chaos.",
      "vendor": "New Vendor",
      "product_type": "ACCESSORIES",
      "created_at": "2023-10-13T15:57:41-04:00",
      "handle": "adidas-classic-backpack",
      "updated_at": "2023-10-14T14:20:00-04:00",
      "published_at": "2023-10-13T15:57:41-04:00",
      "template_suffix": null,
      "published_scope": "global",
      "tags": "adidas, backpack, egnition-sample-data",
      "status": "active",
      "admin_graphql_api_id": "gid://shopify/Product/8153100648670",
      "variants": [
        {
          "id": 44406954885342,
          "product_id": 8153100648670,
          "title": "OS / black",
          "price": "70.00",
          "sku": "AD-03-black-OS",
          "position": 1,
          "inventory_policy": "deny",
          "compare_at_price": null,
          "fulfillment_service": "manual",
          "inventory_management": "shopify",
          "option1": "OS",
          "option2": "black",
          "option3": null,
          "created_at": "2023-10-13T15:57:41-04:00",
          "updated_at": "2023-10-13T15:58:45-04:00",
          "taxable": true,
          "barcode": null,
          "grams": 0,
          "image_id": null,
          "weight": 0,
          "weight_unit": "kg",
          "inventory_item_id": 46504708243678,
          "inventory_quantity": 7,
          "old_inventory_quantity": 7,
          "requires_shipping": true,
          "admin_graphql_api_id": "gid://shopify/ProductVariant/44406954885342"
        }
      ],
      "options": [
        {
          "id": 10425098404062,
          "product_id": 8153100648670,
          "name": "Size",
          "position": 1,
          "values": [
            "OS"
          ]
        },
        {
          "id": 10425098436830,
          "product_id": 8153100648670,
          "name": "Color",
          "position": 2,
          "values": [
            "black"
          ]
        }
      ],
      "images": [
        {
          "id": 39902423482590,
          "product_id": 8153100648670,
          "position": 1,
          "created_at": "2023-10-13T15:57:41-04:00",
          "updated_at": "2023-10-14T14:04:45-04:00",
          "alt": null,
          "width": 635,
          "height": 560,
          "src": "https://cdn.shopify.com/s/files/1/0670/0885/6286/products/85cc58608bf138a50036bcfe86a3a362.jpg?v=1697306685",
          "variant_ids": [],
          "admin_graphql_api_id": "gid://shopify/ProductImage/39902423482590"
        },
        {
          "id": 39902423515358,
          "product_id": 8153100648670,
          "position": 2,
          "created_at": "2023-10-13T15:57:41-04:00",
          "updated_at": "2023-10-13T15:57:41-04:00",
          "alt": null,
          "width": 635,
          "height": 560,
          "src": "https://cdn.shopify.com/s/files/1/0670/0885/6286/products/8a029d2035bfb80e473361dfc08449be.jpg?v=1697227061",
          "variant_ids": [],
          "admin_graphql_api_id": "gid://shopify/ProductImage/39902423515358"
        },
        {
          "id": 39902423548126,
          "product_id": 8153100648670,
          "position": 3,
          "created_at": "2023-10-13T15:57:41-04:00",
          "updated_at": "2023-10-13T15:57:41-04:00",
          "alt": null,
          "width": 635,
          "height": 560,
          "src": "https://cdn.shopify.com/s/files/1/0670/0885/6286/products/ad50775123e20f3d1af2bd07046b777d.jpg?v=1697227061",
          "variant_ids": [],
          "admin_graphql_api_id": "gid://shopify/ProductImage/39902423548126"
        }
      ],
      "image": {
        "id": 39902423482590,
        "product_id": 8153100648670,
        "position": 1,
        "created_at": "2023-10-13T15:57:41-04:00",
        "updated_at": "2023-10-14T14:04:45-04:00",
        "alt": null,
        "width": 635,
        "height": 560,
        "src": "https://cdn.shopify.com/s/files/1/0670/0885/6286/products/85cc58608bf138a50036bcfe86a3a362.jpg?v=1697306685",
        "variant_ids": [],
        "admin_graphql_api_id": "gid://shopify/ProductImage/39902423482590"
      }
    },
    {
      "id": 8153100714206,
      "title": "ADIDAS | CLASSIC BACKPACK | LEGEND INK MULTICOLOUR",
      "body_html": "The adidas BP Classic Cap features a pre-curved brim to keep your face shaded, while a hook-and-loop adjustable closure provides a comfortable fit. With a 3-Stripes design and reflective accents. The perfect piece to top off any outfit.",
      "vendor": "ADIDAS",
      "product_type": "ACCESSORIES",
      "created_at": "2023-10-13T15:57:44-04:00",
      "handle": "adidas-classic-backpack-legend-ink-multicolour",
      "updated_at": "2023-10-13T15:58:46-04:00",
      "published_at": "2023-10-13T15:57:44-04:00",
      "template_suffix": null,
      "published_scope": "global",
      "tags": "adidas, backpack, egnition-sample-data",
      "status": "active",
      "admin_graphql_api_id": "gid://shopify/Product/8153100714206",
      "variants": [
        {
          "id": 44406955049182,
          "product_id": 8153100714206,
          "title": "OS / blue",
          "price": "50.00",
          "sku": "AD-04-blue-OS",
          "position": 1,
          "inventory_policy": "deny",
          "compare_at_price": null,
          "fulfillment_service": "manual",
          "inventory_management": "shopify",
          "option1": "OS",
          "option2": "blue",
          "option3": null,
          "created_at": "2023-10-13T15:57:45-04:00",
          "updated_at": "2023-10-13T15:58:46-04:00",
          "taxable": true,
          "barcode": null,
          "grams": 0,
          "image_id": null,
          "weight": 0,
          "weight_unit": "kg",
          "inventory_item_id": 46504708407518,
          "inventory_quantity": 9,
          "old_inventory_quantity": 9,
          "requires_shipping": true,
          "admin_graphql_api_id": "gid://shopify/ProductVariant/44406955049182"
        }
      ],
      "options": [
        {
          "id": 10425098502366,
          "product_id": 8153100714206,
          "name": "Size",
          "position": 1,
          "values": [
            "OS"
          ]
        },
        {
          "id": 10425098535134,
          "product_id": 8153100714206,
          "name": "Color",
          "position": 2,
          "values": [
            "blue"
          ]
        }
      ],
      "images": [
        {
          "id": 39902423613662,
          "product_id": 8153100714206,
          "position": 1,
          "created_at": "2023-10-13T15:57:45-04:00",
          "updated_at": "2023-10-13T15:57:45-04:00",
          "alt": null,
          "width": 635,
          "height": 560,
          "src": "https://cdn.shopify.com/s/files/1/0670/0885/6286/products/8072c8b5718306d4be25aac21836ce16.jpg?v=1697227065",
          "variant_ids": [],
          "admin_graphql_api_id": "gid://shopify/ProductImage/39902423613662"
        },
        {
          "id": 39902423646430,
          "product_id": 8153100714206,
          "position": 2,
          "created_at": "2023-10-13T15:57:45-04:00",
          "updated_at": "2023-10-13T15:57:45-04:00",
          "alt": null,
          "width": 635,
          "height": 560,
          "src": "https://cdn.shopify.com/s/files/1/0670/0885/6286/products/32b3863554f4686d825d9da18a24cfc6.jpg?v=1697227065",
          "variant_ids": [],
          "admin_graphql_api_id": "gid://shopify/ProductImage/39902423646430"
        },
        {
          "id": 39902423679198,
          "product_id": 8153100714206,
          "position": 3,
          "created_at": "2023-10-13T15:57:45-04:00",
          "updated_at": "2023-10-13T15:57:45-04:00",
          "alt": null,
          "width": 635,
          "height": 560,
          "src": "https://cdn.shopify.com/s/files/1/0670/0885/6286/products/044f848776141f1024eae6c610a28d12.jpg?v=1697227065",
          "variant_ids": [],
          "admin_graphql_api_id": "gid://shopify/ProductImage/39902423679198"
        }
      ],
      "image": {
        "id": 39902423613662,
        "product_id": 8153100714206,
        "position": 1,
        "created_at": "2023-10-13T15:57:45-04:00",
        "updated_at": "2023-10-13T15:57:45-04:00",
        "alt": null,
        "width": 635,
        "height": 560,
        "src": "https://cdn.shopify.com/s/files/1/0670/0885/6286/products/8072c8b5718306d4be25aac21836ce16.jpg?v=1697227065",
        "variant_ids": [],
        "admin_graphql_api_id": "gid://shopify/ProductImage/39902423613662"
      }
    },
    {
      "id": 8153100484830,
      "title": "ADIDAS | KID'S STAN SMITH",
      "body_html": "The Stan Smith owned the tennis court in the '70s. Today it runs the streets with the same clean, classic style. These kids' shoes preserve the iconic look of the original, made in leather with punched 3-Stripes, heel and tongue logos and lightweight step-in cushioning.",
      "vendor": "ADIDAS",
      "product_type": "SHOES",
      "created_at": "2023-10-13T15:57:22-04:00",
      "handle": "adidas-kids-stan-smith",
      "updated_at": "2023-10-13T15:58:43-04:00",
      "published_at": "2023-10-13T15:57:22-04:00",
      "template_suffix": null,
      "published_scope": "global",
      "tags": "adidas, egnition-sample-data, kid",
      "status": "active",
      "admin_graphql_api_id": "gid://shopify/Product/8153100484830",
      "variants": [
        {
          "id": 44406954590430,
          "product_id": 8153100484830,
          "title": "1 / white",
          "price": "90.00",
          "sku": "AD-02-white-1",
          "position": 1,
          "inventory_policy": "deny",
          "compare_at_price": null,
          "fulfillment_service": "manual",
          "inventory_management": "shopify",
          "option1": "1",
          "option2": "white",
          "option3": null,
          "created_at": "2023-10-13T15:57:22-04:00",
          "updated_at": "2023-10-13T15:58:40-04:00",
          "taxable": true,
          "barcode": null,
          "grams": 0,
          "image_id": null,
          "weight": 0,
          "weight_unit": "kg",
          "inventory_item_id": 46504707948766,
          "inventory_quantity": 16,
          "old_inventory_quantity": 16,
          "requires_shipping": true,
          "admin_graphql_api_id": "gid://shopify/ProductVariant/44406954590430"
        },
        {
          "id": 44406954623198,
          "product_id": 8153100484830,
          "title": "2 / white",
          "price": "90.00",
          "sku": "AD-02-white-2",
          "position": 2,
          "inventory_policy": "deny",
          "compare_at_price": null,
          "fulfillment_service": "manual",
          "inventory_management": "shopify",
          "option1": "2",
          "option2": "white",
          "option3": null,
          "created_at": "2023-10-13T15:57:22-04:00",
          "updated_at": "2023-10-13T15:58:42-04:00",
          "taxable": true,
          "barcode": null,
          "grams": 0,
          "image_id": null,
          "weight": 0,
          "weight_unit": "kg",
          "inventory_item_id": 46504707981534,
          "inventory_quantity": 3,
          "old_inventory_quantity": 3,
          "requires_shipping": true,
          "admin_graphql_api_id": "gid://shopify/ProductVariant/44406954623198"
        }
      ],
      "options": [
        {
          "id": 10425098076382,
          "product_id": 8153100484830,
          "name": "Size",
          "position": 1,
          "values": [
            "1",
            "2"
          ]
        },
        {
          "id": 10425098109150,
          "product_id": 8153100484830,
          "name": "Color",
          "position": 2,
          "values": [
            "white"
          ]
        }
      ],
      "images": [
        {
          "id": 39902422663390,
          "product_id": 8153100484830,
          "position": 1,
          "created_at": "2023-10-13T15:57:22-04:00",
          "updated_at": "2023-10-13T15:57:22-04:00",
          "alt": null,
          "width": 635,
          "height": 560,
          "src": "https://cdn.shopify.com/s/files/1/0670/0885/6286/products/7883dc186e15bf29dad696e1e989e914.jpg?v=1697227042",
          "variant_ids": [],
          "admin_graphql_api_id": "gid://shopify/ProductImage/39902422663390"
        },
        {
          "id": 39902422696158,
          "product_id": 8153100484830,
          "position": 2,
          "created_at": "2023-10-13T15:57:22-04:00",
          "updated_at": "2023-10-13T15:57:22-04:00",
          "alt": null,
          "width": 635,
          "height": 560,
          "src": "https://cdn.shopify.com/s/files/1/0670/0885/6286/products/8cd561824439482e3cea5ba8e3a6e2f6.jpg?v=1697227042",
          "variant_ids": [],
          "admin_graphql_api_id": "gid://shopify/ProductImage/39902422696158"
        },
        {
          "id": 39902422728926,
          "product_id": 8153100484830,
          "position": 3,
          "created_at": "2023-10-13T15:57:22-04:00",
          "updated_at": "2023-10-13T15:57:22-04:00",
          "alt": null,
          "width": 635,
          "height": 560,
          "src": "https://cdn.shopify.com/s/files/1/0670/0885/6286/products/2e1f72987692d2dcc3c02be2f194d6c5.jpg?v=1697227042",
          "variant_ids": [],
          "admin_graphql_api_id": "gid://shopify/ProductImage/39902422728926"
        },
        {
          "id": 39902422761694,
          "product_id": 8153100484830,
          "position": 4,
          "created_at": "2023-10-13T15:57:22-04:00",
          "updated_at": "2023-10-13T15:57:22-04:00",
          "alt": null,
          "width": 635,
          "height": 560,
          "src": "https://cdn.shopify.com/s/files/1/0670/0885/6286/products/6216e82660d881e6f2b0e46dc3f8844a.jpg?v=1697227042",
          "variant_ids": [],
          "admin_graphql_api_id": "gid://shopify/ProductImage/39902422761694"
        },
        {
          "id": 39902422794462,
          "product_id": 8153100484830,
          "position": 5,
          "created_at": "2023-10-13T15:57:22-04:00",
          "updated_at": "2023-10-13T15:57:22-04:00",
          "alt": null,
          "width": 635,
          "height": 560,
          "src": "https://cdn.shopify.com/s/files/1/0670/0885/6286/products/e5247cc373e3b61f18013282a6d9c3c0.jpg?v=1697227042",
          "variant_ids": [],
          "admin_graphql_api_id": "gid://shopify/ProductImage/39902422794462"
        }
      ],
      "image": {
        "id": 39902422663390,
        "product_id": 8153100484830,
        "position": 1,
        "created_at": "2023-10-13T15:57:22-04:00",
        "updated_at": "2023-10-13T15:57:22-04:00",
        "alt": null,
        "width": 635,
        "height": 560,
        "src": "https://cdn.shopify.com/s/files/1/0670/0885/6286/products/7883dc186e15bf29dad696e1e989e914.jpg?v=1697227042",
        "variant_ids": [],
        "admin_graphql_api_id": "gid://shopify/ProductImage/39902422663390"
      }
    }
  ]
    """
    return json.loads(products)


@pytest.fixture
def customers_table(shopify_handler):
    return CustomersTable(shopify_handler)


@pytest.fixture
def sample_customers():
    customers = """
        [
        {
            "id": 7093269528798,
            "email": "anonymous@gmail.com",
            "accepts_marketing": true,
            "created_at": "2023-10-14T09:22:51-04:00",
            "updated_at": "2023-10-14T09:22:51-04:00",
            "first_name": "Vee",
            "last_name": "Dee",
            "orders_count": 0,
            "state": "disabled",
            "total_spent": "0.00",
            "last_order_id": null,
            "note": "",
            "verified_email": true,
            "multipass_identifier": null,
            "tax_exempt": false,
            "tags": "",
            "last_order_name": null,
            "currency": "INR",
            "phone": "+919172577546",
            "addresses": [
                {
                    "id": 8636510961886,
                    "customer_id": 7093269528798,
                    "first_name": "Vee",
                    "last_name": "Dee",
                    "company": "NA",
                    "address1": "",
                    "address2": "",
                    "city": "",
                    "province": "",
                    "country": "India",
                    "zip": "",
                    "phone": "",
                    "name": "Vee Dee",
                    "province_code": null,
                    "country_code": "IN",
                    "country_name": "India",
                    "default": true
                }
            ],
            "accepts_marketing_updated_at": "2023-10-14T09:22:51-04:00",
            "marketing_opt_in_level": "single_opt_in",
            "tax_exemptions": [],
            "email_marketing_consent": {
                "state": "subscribed",
                "opt_in_level": "single_opt_in",
                "consent_updated_at": "2023-10-14T09:22:51-04:00"
            },
            "sms_marketing_consent": {
                "state": "subscribed",
                "opt_in_level": "single_opt_in",
                "consent_updated_at": "2023-10-14T09:22:51-04:00",
                "consent_collected_from": "SHOPIFY"
            },
            "admin_graphql_api_id": "gid://shopify/Customer/7093269528798",
            "default_address": {
                "id": 8636510961886,
                "customer_id": 7093269528798,
                "first_name": "Vee",
                "last_name": "Dee",
                "company": "NA",
                "address1": "",
                "address2": "",
                "city": "",
                "province": "",
                "country": "India",
                "zip": "",
                "phone": "",
                "name": "Vee Dee",
                "province_code": null,
                "country_code": "IN",
                "country_name": "India",
                "default": true
            }
        },
        {
            "id": 7092329906398,
            "email": "egnition_sample_19@egnition.com",
            "accepts_marketing": false,
            "created_at": "2023-10-13T15:59:01-04:00",
            "updated_at": "2023-10-13T15:59:01-04:00",
            "first_name": "Cedric",
            "last_name": "Richardson",
            "orders_count": 0,
            "state": "disabled",
            "total_spent": "0.00",
            "last_order_id": null,
            "note": null,
            "verified_email": true,
            "multipass_identifier": null,
            "tax_exempt": false,
            "tags": "egnition-sample-data, VIP",
            "last_order_name": null,
            "currency": "INR",
            "phone": "+256312344612",
            "addresses": [
                {
                    "id": 8635506688222,
                    "customer_id": 7092329906398,
                    "first_name": "Cedric",
                    "last_name": "Richardson",
                    "company": null,
                    "address1": "1768 Fusce St.",
                    "address2": null,
                    "city": "Burlington",
                    "province": null,
                    "country": "Uganda",
                    "zip": "39244",
                    "phone": "+256312341598",
                    "name": "Cedric Richardson",
                    "province_code": null,
                    "country_code": "UG",
                    "country_name": "Uganda",
                    "default": true
                }
            ],
            "accepts_marketing_updated_at": "2023-10-13T15:59:01-04:00",
            "marketing_opt_in_level": null,
            "tax_exemptions": [],
            "email_marketing_consent": {
                "state": "not_subscribed",
                "opt_in_level": "single_opt_in",
                "consent_updated_at": null
            },
            "sms_marketing_consent": {
                "state": "not_subscribed",
                "opt_in_level": "single_opt_in",
                "consent_updated_at": null,
                "consent_collected_from": "OTHER"
            },
            "admin_graphql_api_id": "gid://shopify/Customer/7092329906398",
            "default_address": {
                "id": 8635506688222,
                "customer_id": 7092329906398,
                "first_name": "Cedric",
                "last_name": "Richardson",
                "company": null,
                "address1": "1768 Fusce St.",
                "address2": null,
                "city": "Burlington",
                "province": null,
                "country": "Uganda",
                "zip": "39244",
                "phone": "+256312341598",
                "name": "Cedric Richardson",
                "province_code": null,
                "country_code": "UG",
                "country_name": "Uganda",
                "default": true
            }
        },
        {
            "id": 7092329873630,
            "email": "egnition_sample_48@egnition.com",
            "accepts_marketing": false,
            "created_at": "2023-10-13T15:59:00-04:00",
            "updated_at": "2023-10-13T15:59:00-04:00",
            "first_name": "Walker",
            "last_name": "Mason",
            "orders_count": 0,
            "state": "disabled",
            "total_spent": "0.00",
            "last_order_id": null,
            "note": null,
            "verified_email": true,
            "multipass_identifier": null,
            "tax_exempt": false,
            "tags": "egnition-sample-data, referral",
            "last_order_name": null,
            "currency": "INR",
            "phone": "+298346134",
            "addresses": [
                {
                    "id": 8635506655454,
                    "customer_id": 7092329873630,
                    "first_name": "Walker",
                    "last_name": "Mason",
                    "company": null,
                    "address1": "4497 Pede Av.",
                    "address2": null,
                    "city": "Newcastle",
                    "province": null,
                    "country": "Faroe Islands",
                    "zip": "4510",
                    "phone": "+298340839",
                    "name": "Walker Mason",
                    "province_code": null,
                    "country_code": "FO",
                    "country_name": "Faroe Islands",
                    "default": true
                }
            ],
            "accepts_marketing_updated_at": "2023-10-13T15:59:00-04:00",
            "marketing_opt_in_level": null,
            "tax_exemptions": [],
            "email_marketing_consent": {
                "state": "not_subscribed",
                "opt_in_level": "single_opt_in",
                "consent_updated_at": null
            },
            "sms_marketing_consent": {
                "state": "not_subscribed",
                "opt_in_level": "single_opt_in",
                "consent_updated_at": null,
                "consent_collected_from": "OTHER"
            },
            "admin_graphql_api_id": "gid://shopify/Customer/7092329873630",
            "default_address": {
                "id": 8635506655454,
                "customer_id": 7092329873630,
                "first_name": "Walker",
                "last_name": "Mason",
                "company": null,
                "address1": "4497 Pede Av.",
                "address2": null,
                "city": "Newcastle",
                "province": null,
                "country": "Faroe Islands",
                "zip": "4510",
                "phone": "+298340839",
                "name": "Walker Mason",
                "province_code": null,
                "country_code": "FO",
                "country_name": "Faroe Islands",
                "default": true
            }
        }
        ]
    """
    return json.loads(customers)


@pytest.fixture
def orders_table(shopify_handler):
    return OrdersTable(shopify_handler)


@pytest.fixture
def locations_table(shopify_handler):
    return LocationTable(shopify_handler)


@pytest.fixture
def sample_locations():
    locations = """
        [
        {
            "id": 72001192158,
            "name": "NA",
            "address1": "NA",
            "address2": "NA",
            "city": "NA",
            "zip": "400000",
            "province": "Maharashtra",
            "country": "IN",
            "phone": "0000000000",
            "created_at": "2023-10-13T15:52:54-04:00",
            "updated_at": "2023-10-13T15:52:54-04:00",
            "country_code": "IN",
            "country_name": "India",
            "province_code": "MH",
            "legacy": false,
            "active": true,
            "admin_graphql_api_id": "gid://shopify/Location/72001192158",
            "localized_country_name": "India",
            "localized_province_name": "Maharashtra"
        }
    ]
    """
    return json.loads(locations)


@pytest.fixture
def carrier_service_table(shopify_handler):
    return CarrierServiceTable(shopify_handler)


@pytest.fixture
def sample_carriers():
    carriers = """
        [
            {
              "active": true,
              "callback_url": "http://myapp.com",
              "carrier_service_type": "api",
              "id": 14079244,
              "format": "json",
              "name": "My Carrier Service",
              "service_discovery": true,
              "admin_graphql_api_id": "gid://shopify/DeliveryCarrierService/1"
            }
        ]
    """
    return json.loads(carriers)


@pytest.fixture
def sales_channel_table(shopify_handler):
    return SalesChannelTable(shopify_handler)


@pytest.fixture
def sample_sales_channel():
    sales_channel = """
        [
            {
                "id": 107948343518,
                "created_at": "2023-10-13T15:52:57-04:00",
                "name": "Point of Sale"
            },
            {
                "id": 107948212446,
                "created_at": "2023-10-13T15:49:05-04:00",
                "name": "Online Store"
            }
        ]
    """
    return json.loads(sales_channel)


@pytest.fixture
def shipping_zone_table(shopify_handler):
    return ShippingZoneTable(shopify_handler)


@pytest.fixture
def sample_shipping_zones():
    shipping_zones = """
    [
        {
            "id": 376959271134,
            "name": "International",
            "profile_id": "gid://shopify/DeliveryProfile/93511188702",
            "location_group_id": "gid://shopify/DeliveryLocationGroup/94573756638",
            "admin_graphql_api_id": "gid://shopify/DeliveryZone/376959271134",
            "countries": [
                {
                    "id": 479226233054,
                    "name": "United Arab Emirates",
                    "tax": 0.0,
                    "code": "AE",
                    "tax_name": "VAT",
                    "shipping_zone_id": 376959271134,
                    "provinces": [
                        {
                            "id": 4904628420830,
                            "country_id": 479226233054,
                            "name": "Abu Dhabi",
                            "code": "AZ",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904628453598,
                            "country_id": 479226233054,
                            "name": "Ajman",
                            "code": "AJ",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904628486366,
                            "country_id": 479226233054,
                            "name": "Dubai",
                            "code": "DU",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904628519134,
                            "country_id": 479226233054,
                            "name": "Fujairah",
                            "code": "FU",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904628551902,
                            "country_id": 479226233054,
                            "name": "Ras al-Khaimah",
                            "code": "RK",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904628584670,
                            "country_id": 479226233054,
                            "name": "Sharjah",
                            "code": "SH",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904628617438,
                            "country_id": 479226233054,
                            "name": "Umm al-Quwain",
                            "code": "UQ",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        }
                    ]
                },
                {
                    "id": 479226265822,
                    "name": "Austria",
                    "tax": 0.0,
                    "code": "AT",
                    "tax_name": "AT VAT",
                    "shipping_zone_id": 376959271134,
                    "provinces": []
                },
                {
                    "id": 479226298590,
                    "name": "Australia",
                    "tax": 0.0,
                    "code": "AU",
                    "tax_name": "GST",
                    "shipping_zone_id": 376959271134,
                    "provinces": [
                        {
                            "id": 4904628650206,
                            "country_id": 479226298590,
                            "name": "Australian Capital Territory",
                            "code": "ACT",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904628682974,
                            "country_id": 479226298590,
                            "name": "New South Wales",
                            "code": "NSW",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904628715742,
                            "country_id": 479226298590,
                            "name": "Northern Territory",
                            "code": "NT",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904628748510,
                            "country_id": 479226298590,
                            "name": "Queensland",
                            "code": "QLD",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904628781278,
                            "country_id": 479226298590,
                            "name": "South Australia",
                            "code": "SA",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904628814046,
                            "country_id": 479226298590,
                            "name": "Tasmania",
                            "code": "TAS",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904628846814,
                            "country_id": 479226298590,
                            "name": "Victoria",
                            "code": "VIC",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904628879582,
                            "country_id": 479226298590,
                            "name": "Western Australia",
                            "code": "WA",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        }
                    ]
                },
                {
                    "id": 479226331358,
                    "name": "Belgium",
                    "tax": 0.0,
                    "code": "BE",
                    "tax_name": "BE TVA",
                    "shipping_zone_id": 376959271134,
                    "provinces": []
                },
                {
                    "id": 479226364126,
                    "name": "Canada",
                    "tax": 0.0,
                    "code": "CA",
                    "tax_name": "GST",
                    "shipping_zone_id": 376959271134,
                    "provinces": [
                        {
                            "id": 4904628912350,
                            "country_id": 479226364126,
                            "name": "Alberta",
                            "code": "AB",
                            "tax": 0.0,
                            "tax_name": "PST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904628945118,
                            "country_id": 479226364126,
                            "name": "British Columbia",
                            "code": "BC",
                            "tax": 0.0,
                            "tax_name": "PST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904628977886,
                            "country_id": 479226364126,
                            "name": "Manitoba",
                            "code": "MB",
                            "tax": 0.0,
                            "tax_name": "RST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629010654,
                            "country_id": 479226364126,
                            "name": "New Brunswick",
                            "code": "NB",
                            "tax": 0.0,
                            "tax_name": "HST",
                            "tax_type": "harmonized",
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629043422,
                            "country_id": 479226364126,
                            "name": "Newfoundland and Labrador",
                            "code": "NL",
                            "tax": 0.0,
                            "tax_name": "HST",
                            "tax_type": "harmonized",
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629076190,
                            "country_id": 479226364126,
                            "name": "Northwest Territories",
                            "code": "NT",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629108958,
                            "country_id": 479226364126,
                            "name": "Nova Scotia",
                            "code": "NS",
                            "tax": 0.0,
                            "tax_name": "HST",
                            "tax_type": "harmonized",
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629141726,
                            "country_id": 479226364126,
                            "name": "Nunavut",
                            "code": "NU",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629174494,
                            "country_id": 479226364126,
                            "name": "Ontario",
                            "code": "ON",
                            "tax": 0.0,
                            "tax_name": "HST",
                            "tax_type": "harmonized",
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629207262,
                            "country_id": 479226364126,
                            "name": "Prince Edward Island",
                            "code": "PE",
                            "tax": 0.0,
                            "tax_name": "HST",
                            "tax_type": "harmonized",
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629240030,
                            "country_id": 479226364126,
                            "name": "Quebec",
                            "code": "QC",
                            "tax": 0.0,
                            "tax_name": "QST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629272798,
                            "country_id": 479226364126,
                            "name": "Saskatchewan",
                            "code": "SK",
                            "tax": 0.0,
                            "tax_name": "PST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629305566,
                            "country_id": 479226364126,
                            "name": "Yukon",
                            "code": "YT",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        }
                    ]
                },
                {
                    "id": 479226396894,
                    "name": "Switzerland",
                    "tax": 0.0,
                    "code": "CH",
                    "tax_name": "MwSt",
                    "shipping_zone_id": 376959271134,
                    "provinces": []
                },
                {
                    "id": 479226429662,
                    "name": "Czech Republic",
                    "tax": 0.0,
                    "code": "CZ",
                    "tax_name": "CZ VAT",
                    "shipping_zone_id": 376959271134,
                    "provinces": []
                },
                {
                    "id": 479226462430,
                    "name": "Germany",
                    "tax": 0.0,
                    "code": "DE",
                    "tax_name": "DE MwSt",
                    "shipping_zone_id": 376959271134,
                    "provinces": []
                },
                {
                    "id": 479226495198,
                    "name": "Denmark",
                    "tax": 0.0,
                    "code": "DK",
                    "tax_name": "DK Moms",
                    "shipping_zone_id": 376959271134,
                    "provinces": []
                },
                {
                    "id": 479226527966,
                    "name": "Spain",
                    "tax": 0.0,
                    "code": "ES",
                    "tax_name": "ES IVA",
                    "shipping_zone_id": 376959271134,
                    "provinces": [
                        {
                            "id": 4904629338334,
                            "country_id": 479226527966,
                            "name": "A Coruña",
                            "code": "C",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629371102,
                            "country_id": 479226527966,
                            "name": "Álava",
                            "code": "VI",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629403870,
                            "country_id": 479226527966,
                            "name": "Albacete",
                            "code": "AB",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629436638,
                            "country_id": 479226527966,
                            "name": "Alicante",
                            "code": "A",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629469406,
                            "country_id": 479226527966,
                            "name": "Almería",
                            "code": "AL",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629502174,
                            "country_id": 479226527966,
                            "name": "Asturias",
                            "code": "O",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629534942,
                            "country_id": 479226527966,
                            "name": "Ávila",
                            "code": "AV",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629567710,
                            "country_id": 479226527966,
                            "name": "Badajoz",
                            "code": "BA",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629600478,
                            "country_id": 479226527966,
                            "name": "Balears",
                            "code": "PM",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629633246,
                            "country_id": 479226527966,
                            "name": "Barcelona",
                            "code": "B",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629666014,
                            "country_id": 479226527966,
                            "name": "Burgos",
                            "code": "BU",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629698782,
                            "country_id": 479226527966,
                            "name": "Cáceres",
                            "code": "CC",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629731550,
                            "country_id": 479226527966,
                            "name": "Cádiz",
                            "code": "CA",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629764318,
                            "country_id": 479226527966,
                            "name": "Cantabria",
                            "code": "S",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629797086,
                            "country_id": 479226527966,
                            "name": "Castellón",
                            "code": "CS",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629829854,
                            "country_id": 479226527966,
                            "name": "Ceuta",
                            "code": "CE",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629862622,
                            "country_id": 479226527966,
                            "name": "Ciudad Real",
                            "code": "CR",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629895390,
                            "country_id": 479226527966,
                            "name": "Córdoba",
                            "code": "CO",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629928158,
                            "country_id": 479226527966,
                            "name": "Cuenca",
                            "code": "CU",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629960926,
                            "country_id": 479226527966,
                            "name": "Girona",
                            "code": "GI",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904629993694,
                            "country_id": 479226527966,
                            "name": "Granada",
                            "code": "GR",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630026462,
                            "country_id": 479226527966,
                            "name": "Guadalajara",
                            "code": "GU",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630059230,
                            "country_id": 479226527966,
                            "name": "Guipúzcoa",
                            "code": "SS",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630091998,
                            "country_id": 479226527966,
                            "name": "Huelva",
                            "code": "H",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630124766,
                            "country_id": 479226527966,
                            "name": "Huesca",
                            "code": "HU",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630157534,
                            "country_id": 479226527966,
                            "name": "Jaén",
                            "code": "J",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630190302,
                            "country_id": 479226527966,
                            "name": "La Rioja",
                            "code": "LO",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630223070,
                            "country_id": 479226527966,
                            "name": "Las Palmas",
                            "code": "GC",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630255838,
                            "country_id": 479226527966,
                            "name": "León",
                            "code": "LE",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630288606,
                            "country_id": 479226527966,
                            "name": "Lleida",
                            "code": "L",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630321374,
                            "country_id": 479226527966,
                            "name": "Lugo",
                            "code": "LU",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630354142,
                            "country_id": 479226527966,
                            "name": "Madrid",
                            "code": "M",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630386910,
                            "country_id": 479226527966,
                            "name": "Málaga",
                            "code": "MA",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630419678,
                            "country_id": 479226527966,
                            "name": "Melilla",
                            "code": "ML",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630452446,
                            "country_id": 479226527966,
                            "name": "Murcia",
                            "code": "MU",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630485214,
                            "country_id": 479226527966,
                            "name": "Navarra",
                            "code": "NA",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630517982,
                            "country_id": 479226527966,
                            "name": "Ourense",
                            "code": "OR",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630550750,
                            "country_id": 479226527966,
                            "name": "Palencia",
                            "code": "P",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630583518,
                            "country_id": 479226527966,
                            "name": "Pontevedra",
                            "code": "PO",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630616286,
                            "country_id": 479226527966,
                            "name": "Salamanca",
                            "code": "SA",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630649054,
                            "country_id": 479226527966,
                            "name": "Santa Cruz de Tenerife",
                            "code": "TF",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630681822,
                            "country_id": 479226527966,
                            "name": "Segovia",
                            "code": "SG",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630714590,
                            "country_id": 479226527966,
                            "name": "Sevilla",
                            "code": "SE",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630747358,
                            "country_id": 479226527966,
                            "name": "Soria",
                            "code": "SO",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630780126,
                            "country_id": 479226527966,
                            "name": "Tarragona",
                            "code": "T",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630812894,
                            "country_id": 479226527966,
                            "name": "Teruel",
                            "code": "TE",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630845662,
                            "country_id": 479226527966,
                            "name": "Toledo",
                            "code": "TO",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630878430,
                            "country_id": 479226527966,
                            "name": "Valencia",
                            "code": "V",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630911198,
                            "country_id": 479226527966,
                            "name": "Valladolid",
                            "code": "VA",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630943966,
                            "country_id": 479226527966,
                            "name": "Vizcaya",
                            "code": "BI",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904630976734,
                            "country_id": 479226527966,
                            "name": "Zamora",
                            "code": "ZA",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631009502,
                            "country_id": 479226527966,
                            "name": "Zaragoza",
                            "code": "Z",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        }
                    ]
                },
                {
                    "id": 479226560734,
                    "name": "Finland",
                    "tax": 0.0,
                    "code": "FI",
                    "tax_name": "FI ALV",
                    "shipping_zone_id": 376959271134,
                    "provinces": []
                },
                {
                    "id": 479226593502,
                    "name": "France",
                    "tax": 0.0,
                    "code": "FR",
                    "tax_name": "FR TVA",
                    "shipping_zone_id": 376959271134,
                    "provinces": []
                },
                {
                    "id": 479226626270,
                    "name": "United Kingdom",
                    "tax": 0.0,
                    "code": "GB",
                    "tax_name": "GB VAT",
                    "shipping_zone_id": 376959271134,
                    "provinces": [
                        {
                            "id": 4904631042270,
                            "country_id": 479226626270,
                            "name": "British Forces",
                            "code": "BFP",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631075038,
                            "country_id": 479226626270,
                            "name": "England",
                            "code": "ENG",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631107806,
                            "country_id": 479226626270,
                            "name": "Northern Ireland",
                            "code": "NIR",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631140574,
                            "country_id": 479226626270,
                            "name": "Scotland",
                            "code": "SCT",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631173342,
                            "country_id": 479226626270,
                            "name": "Wales",
                            "code": "WLS",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        }
                    ]
                },
                {
                    "id": 479226659038,
                    "name": "Hong Kong",
                    "tax": 0.0,
                    "code": "HK",
                    "tax_name": "VAT",
                    "shipping_zone_id": 376959271134,
                    "provinces": [
                        {
                            "id": 4904631206110,
                            "country_id": 479226659038,
                            "name": "Hong Kong Island",
                            "code": "HK",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631238878,
                            "country_id": 479226659038,
                            "name": "Kowloon",
                            "code": "KL",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631271646,
                            "country_id": 479226659038,
                            "name": "New Territories",
                            "code": "NT",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        }
                    ]
                },
                {
                    "id": 479226691806,
                    "name": "Ireland",
                    "tax": 0.0,
                    "code": "IE",
                    "tax_name": "IE VAT",
                    "shipping_zone_id": 376959271134,
                    "provinces": [
                        {
                            "id": 4904631304414,
                            "country_id": 479226691806,
                            "name": "Carlow",
                            "code": "CW",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631337182,
                            "country_id": 479226691806,
                            "name": "Cavan",
                            "code": "CN",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631369950,
                            "country_id": 479226691806,
                            "name": "Clare",
                            "code": "CE",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631402718,
                            "country_id": 479226691806,
                            "name": "Cork",
                            "code": "CO",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631435486,
                            "country_id": 479226691806,
                            "name": "Donegal",
                            "code": "DL",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631468254,
                            "country_id": 479226691806,
                            "name": "Dublin",
                            "code": "D",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631501022,
                            "country_id": 479226691806,
                            "name": "Galway",
                            "code": "G",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631533790,
                            "country_id": 479226691806,
                            "name": "Kerry",
                            "code": "KY",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631566558,
                            "country_id": 479226691806,
                            "name": "Kildare",
                            "code": "KE",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631599326,
                            "country_id": 479226691806,
                            "name": "Kilkenny",
                            "code": "KK",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631632094,
                            "country_id": 479226691806,
                            "name": "Laois",
                            "code": "LS",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631664862,
                            "country_id": 479226691806,
                            "name": "Leitrim",
                            "code": "LM",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631697630,
                            "country_id": 479226691806,
                            "name": "Limerick",
                            "code": "LK",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631730398,
                            "country_id": 479226691806,
                            "name": "Longford",
                            "code": "LD",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631763166,
                            "country_id": 479226691806,
                            "name": "Louth",
                            "code": "LH",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631795934,
                            "country_id": 479226691806,
                            "name": "Mayo",
                            "code": "MO",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631828702,
                            "country_id": 479226691806,
                            "name": "Meath",
                            "code": "MH",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631861470,
                            "country_id": 479226691806,
                            "name": "Monaghan",
                            "code": "MN",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631894238,
                            "country_id": 479226691806,
                            "name": "Offaly",
                            "code": "OY",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631927006,
                            "country_id": 479226691806,
                            "name": "Roscommon",
                            "code": "RN",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631959774,
                            "country_id": 479226691806,
                            "name": "Sligo",
                            "code": "SO",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904631992542,
                            "country_id": 479226691806,
                            "name": "Tipperary",
                            "code": "TA",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632025310,
                            "country_id": 479226691806,
                            "name": "Waterford",
                            "code": "WD",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632058078,
                            "country_id": 479226691806,
                            "name": "Westmeath",
                            "code": "WH",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632090846,
                            "country_id": 479226691806,
                            "name": "Wexford",
                            "code": "WX",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632123614,
                            "country_id": 479226691806,
                            "name": "Wicklow",
                            "code": "WW",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        }
                    ]
                },
                {
                    "id": 479226724574,
                    "name": "Israel",
                    "tax": 0.0,
                    "code": "IL",
                    "tax_name": "VAT",
                    "shipping_zone_id": 376959271134,
                    "provinces": []
                },
                {
                    "id": 479226757342,
                    "name": "Italy",
                    "tax": 0.0,
                    "code": "IT",
                    "tax_name": "IT IVA",
                    "shipping_zone_id": 376959271134,
                    "provinces": [
                        {
                            "id": 4904632156382,
                            "country_id": 479226757342,
                            "name": "Agrigento",
                            "code": "AG",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632189150,
                            "country_id": 479226757342,
                            "name": "Alessandria",
                            "code": "AL",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632221918,
                            "country_id": 479226757342,
                            "name": "Ancona",
                            "code": "AN",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632254686,
                            "country_id": 479226757342,
                            "name": "Aosta",
                            "code": "AO",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632287454,
                            "country_id": 479226757342,
                            "name": "Arezzo",
                            "code": "AR",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632320222,
                            "country_id": 479226757342,
                            "name": "Ascoli Piceno",
                            "code": "AP",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632352990,
                            "country_id": 479226757342,
                            "name": "Asti",
                            "code": "AT",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632385758,
                            "country_id": 479226757342,
                            "name": "Avellino",
                            "code": "AV",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632418526,
                            "country_id": 479226757342,
                            "name": "Bari",
                            "code": "BA",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632451294,
                            "country_id": 479226757342,
                            "name": "Barletta-Andria-Trani",
                            "code": "BT",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632484062,
                            "country_id": 479226757342,
                            "name": "Belluno",
                            "code": "BL",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632516830,
                            "country_id": 479226757342,
                            "name": "Benevento",
                            "code": "BN",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632549598,
                            "country_id": 479226757342,
                            "name": "Bergamo",
                            "code": "BG",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632582366,
                            "country_id": 479226757342,
                            "name": "Biella",
                            "code": "BI",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632615134,
                            "country_id": 479226757342,
                            "name": "Bologna",
                            "code": "BO",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632647902,
                            "country_id": 479226757342,
                            "name": "Bolzano",
                            "code": "BZ",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632680670,
                            "country_id": 479226757342,
                            "name": "Brescia",
                            "code": "BS",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632713438,
                            "country_id": 479226757342,
                            "name": "Brindisi",
                            "code": "BR",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632746206,
                            "country_id": 479226757342,
                            "name": "Cagliari",
                            "code": "CA",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632778974,
                            "country_id": 479226757342,
                            "name": "Caltanissetta",
                            "code": "CL",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632811742,
                            "country_id": 479226757342,
                            "name": "Campobasso",
                            "code": "CB",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632844510,
                            "country_id": 479226757342,
                            "name": "Carbonia-Iglesias",
                            "code": "CI",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632877278,
                            "country_id": 479226757342,
                            "name": "Caserta",
                            "code": "CE",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632910046,
                            "country_id": 479226757342,
                            "name": "Catania",
                            "code": "CT",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632942814,
                            "country_id": 479226757342,
                            "name": "Catanzaro",
                            "code": "CZ",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904632975582,
                            "country_id": 479226757342,
                            "name": "Chieti",
                            "code": "CH",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633008350,
                            "country_id": 479226757342,
                            "name": "Como",
                            "code": "CO",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633041118,
                            "country_id": 479226757342,
                            "name": "Cosenza",
                            "code": "CS",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633073886,
                            "country_id": 479226757342,
                            "name": "Cremona",
                            "code": "CR",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633106654,
                            "country_id": 479226757342,
                            "name": "Crotone",
                            "code": "KR",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633139422,
                            "country_id": 479226757342,
                            "name": "Cuneo",
                            "code": "CN",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633172190,
                            "country_id": 479226757342,
                            "name": "Enna",
                            "code": "EN",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633204958,
                            "country_id": 479226757342,
                            "name": "Fermo",
                            "code": "FM",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633237726,
                            "country_id": 479226757342,
                            "name": "Ferrara",
                            "code": "FE",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633270494,
                            "country_id": 479226757342,
                            "name": "Firenze",
                            "code": "FI",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633303262,
                            "country_id": 479226757342,
                            "name": "Foggia",
                            "code": "FG",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633336030,
                            "country_id": 479226757342,
                            "name": "Forlì-Cesena",
                            "code": "FC",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633368798,
                            "country_id": 479226757342,
                            "name": "Frosinone",
                            "code": "FR",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633401566,
                            "country_id": 479226757342,
                            "name": "Genova",
                            "code": "GE",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633434334,
                            "country_id": 479226757342,
                            "name": "Gorizia",
                            "code": "GO",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633467102,
                            "country_id": 479226757342,
                            "name": "Grosseto",
                            "code": "GR",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633499870,
                            "country_id": 479226757342,
                            "name": "Imperia",
                            "code": "IM",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633532638,
                            "country_id": 479226757342,
                            "name": "Isernia",
                            "code": "IS",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633565406,
                            "country_id": 479226757342,
                            "name": "L'Aquila",
                            "code": "AQ",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633598174,
                            "country_id": 479226757342,
                            "name": "La Spezia",
                            "code": "SP",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633630942,
                            "country_id": 479226757342,
                            "name": "Latina",
                            "code": "LT",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633663710,
                            "country_id": 479226757342,
                            "name": "Lecce",
                            "code": "LE",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633696478,
                            "country_id": 479226757342,
                            "name": "Lecco",
                            "code": "LC",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633729246,
                            "country_id": 479226757342,
                            "name": "Livorno",
                            "code": "LI",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633762014,
                            "country_id": 479226757342,
                            "name": "Lodi",
                            "code": "LO",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633794782,
                            "country_id": 479226757342,
                            "name": "Lucca",
                            "code": "LU",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633827550,
                            "country_id": 479226757342,
                            "name": "Macerata",
                            "code": "MC",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633860318,
                            "country_id": 479226757342,
                            "name": "Mantova",
                            "code": "MN",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633893086,
                            "country_id": 479226757342,
                            "name": "Massa-Carrara",
                            "code": "MS",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633925854,
                            "country_id": 479226757342,
                            "name": "Matera",
                            "code": "MT",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633958622,
                            "country_id": 479226757342,
                            "name": "Medio Campidano",
                            "code": "VS",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904633991390,
                            "country_id": 479226757342,
                            "name": "Messina",
                            "code": "ME",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634024158,
                            "country_id": 479226757342,
                            "name": "Milano",
                            "code": "MI",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634056926,
                            "country_id": 479226757342,
                            "name": "Modena",
                            "code": "MO",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634089694,
                            "country_id": 479226757342,
                            "name": "Monza e Brianza",
                            "code": "MB",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634122462,
                            "country_id": 479226757342,
                            "name": "Napoli",
                            "code": "NA",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634155230,
                            "country_id": 479226757342,
                            "name": "Novara",
                            "code": "NO",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634187998,
                            "country_id": 479226757342,
                            "name": "Nuoro",
                            "code": "NU",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634220766,
                            "country_id": 479226757342,
                            "name": "Ogliastra",
                            "code": "OG",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634253534,
                            "country_id": 479226757342,
                            "name": "Olbia-Tempio",
                            "code": "OT",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634286302,
                            "country_id": 479226757342,
                            "name": "Oristano",
                            "code": "OR",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634319070,
                            "country_id": 479226757342,
                            "name": "Padova",
                            "code": "PD",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634351838,
                            "country_id": 479226757342,
                            "name": "Palermo",
                            "code": "PA",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634384606,
                            "country_id": 479226757342,
                            "name": "Parma",
                            "code": "PR",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634417374,
                            "country_id": 479226757342,
                            "name": "Pavia",
                            "code": "PV",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634450142,
                            "country_id": 479226757342,
                            "name": "Perugia",
                            "code": "PG",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634482910,
                            "country_id": 479226757342,
                            "name": "Pesaro e Urbino",
                            "code": "PU",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634515678,
                            "country_id": 479226757342,
                            "name": "Pescara",
                            "code": "PE",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634548446,
                            "country_id": 479226757342,
                            "name": "Piacenza",
                            "code": "PC",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634581214,
                            "country_id": 479226757342,
                            "name": "Pisa",
                            "code": "PI",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634613982,
                            "country_id": 479226757342,
                            "name": "Pistoia",
                            "code": "PT",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634646750,
                            "country_id": 479226757342,
                            "name": "Pordenone",
                            "code": "PN",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634679518,
                            "country_id": 479226757342,
                            "name": "Potenza",
                            "code": "PZ",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634712286,
                            "country_id": 479226757342,
                            "name": "Prato",
                            "code": "PO",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634745054,
                            "country_id": 479226757342,
                            "name": "Ragusa",
                            "code": "RG",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634777822,
                            "country_id": 479226757342,
                            "name": "Ravenna",
                            "code": "RA",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634810590,
                            "country_id": 479226757342,
                            "name": "Reggio Calabria",
                            "code": "RC",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634843358,
                            "country_id": 479226757342,
                            "name": "Reggio Emilia",
                            "code": "RE",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634876126,
                            "country_id": 479226757342,
                            "name": "Rieti",
                            "code": "RI",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634908894,
                            "country_id": 479226757342,
                            "name": "Rimini",
                            "code": "RN",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634941662,
                            "country_id": 479226757342,
                            "name": "Roma",
                            "code": "RM",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904634974430,
                            "country_id": 479226757342,
                            "name": "Rovigo",
                            "code": "RO",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635007198,
                            "country_id": 479226757342,
                            "name": "Salerno",
                            "code": "SA",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635039966,
                            "country_id": 479226757342,
                            "name": "Sassari",
                            "code": "SS",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635072734,
                            "country_id": 479226757342,
                            "name": "Savona",
                            "code": "SV",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635105502,
                            "country_id": 479226757342,
                            "name": "Siena",
                            "code": "SI",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635138270,
                            "country_id": 479226757342,
                            "name": "Siracusa",
                            "code": "SR",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635171038,
                            "country_id": 479226757342,
                            "name": "Sondrio",
                            "code": "SO",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635203806,
                            "country_id": 479226757342,
                            "name": "Taranto",
                            "code": "TA",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635236574,
                            "country_id": 479226757342,
                            "name": "Teramo",
                            "code": "TE",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635269342,
                            "country_id": 479226757342,
                            "name": "Terni",
                            "code": "TR",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635302110,
                            "country_id": 479226757342,
                            "name": "Torino",
                            "code": "TO",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635334878,
                            "country_id": 479226757342,
                            "name": "Trapani",
                            "code": "TP",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635367646,
                            "country_id": 479226757342,
                            "name": "Trento",
                            "code": "TN",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635400414,
                            "country_id": 479226757342,
                            "name": "Treviso",
                            "code": "TV",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635433182,
                            "country_id": 479226757342,
                            "name": "Trieste",
                            "code": "TS",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635465950,
                            "country_id": 479226757342,
                            "name": "Udine",
                            "code": "UD",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635498718,
                            "country_id": 479226757342,
                            "name": "Varese",
                            "code": "VA",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635531486,
                            "country_id": 479226757342,
                            "name": "Venezia",
                            "code": "VE",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635564254,
                            "country_id": 479226757342,
                            "name": "Verbano-Cusio-Ossola",
                            "code": "VB",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635597022,
                            "country_id": 479226757342,
                            "name": "Vercelli",
                            "code": "VC",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635629790,
                            "country_id": 479226757342,
                            "name": "Verona",
                            "code": "VR",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635662558,
                            "country_id": 479226757342,
                            "name": "Vibo Valentia",
                            "code": "VV",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635695326,
                            "country_id": 479226757342,
                            "name": "Vicenza",
                            "code": "VI",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635728094,
                            "country_id": 479226757342,
                            "name": "Viterbo",
                            "code": "VT",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        }
                    ]
                },
                {
                    "id": 479226790110,
                    "name": "Japan",
                    "tax": 0.0,
                    "code": "JP",
                    "tax_name": "CT",
                    "shipping_zone_id": 376959271134,
                    "provinces": [
                        {
                            "id": 4904635760862,
                            "country_id": 479226790110,
                            "name": "Hokkaidō",
                            "code": "JP-01",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635793630,
                            "country_id": 479226790110,
                            "name": "Aomori",
                            "code": "JP-02",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635826398,
                            "country_id": 479226790110,
                            "name": "Iwate",
                            "code": "JP-03",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635859166,
                            "country_id": 479226790110,
                            "name": "Miyagi",
                            "code": "JP-04",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635891934,
                            "country_id": 479226790110,
                            "name": "Akita",
                            "code": "JP-05",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635924702,
                            "country_id": 479226790110,
                            "name": "Yamagata",
                            "code": "JP-06",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635957470,
                            "country_id": 479226790110,
                            "name": "Fukushima",
                            "code": "JP-07",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904635990238,
                            "country_id": 479226790110,
                            "name": "Ibaraki",
                            "code": "JP-08",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636023006,
                            "country_id": 479226790110,
                            "name": "Tochigi",
                            "code": "JP-09",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636055774,
                            "country_id": 479226790110,
                            "name": "Gunma",
                            "code": "JP-10",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636088542,
                            "country_id": 479226790110,
                            "name": "Saitama",
                            "code": "JP-11",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636121310,
                            "country_id": 479226790110,
                            "name": "Chiba",
                            "code": "JP-12",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636154078,
                            "country_id": 479226790110,
                            "name": "Tōkyō",
                            "code": "JP-13",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636186846,
                            "country_id": 479226790110,
                            "name": "Kanagawa",
                            "code": "JP-14",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636219614,
                            "country_id": 479226790110,
                            "name": "Niigata",
                            "code": "JP-15",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636252382,
                            "country_id": 479226790110,
                            "name": "Toyama",
                            "code": "JP-16",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636285150,
                            "country_id": 479226790110,
                            "name": "Ishikawa",
                            "code": "JP-17",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636317918,
                            "country_id": 479226790110,
                            "name": "Fukui",
                            "code": "JP-18",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636350686,
                            "country_id": 479226790110,
                            "name": "Yamanashi",
                            "code": "JP-19",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636383454,
                            "country_id": 479226790110,
                            "name": "Nagano",
                            "code": "JP-20",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636416222,
                            "country_id": 479226790110,
                            "name": "Gifu",
                            "code": "JP-21",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636448990,
                            "country_id": 479226790110,
                            "name": "Shizuoka",
                            "code": "JP-22",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636481758,
                            "country_id": 479226790110,
                            "name": "Aichi",
                            "code": "JP-23",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636514526,
                            "country_id": 479226790110,
                            "name": "Mie",
                            "code": "JP-24",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636547294,
                            "country_id": 479226790110,
                            "name": "Shiga",
                            "code": "JP-25",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636580062,
                            "country_id": 479226790110,
                            "name": "Kyōto",
                            "code": "JP-26",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636612830,
                            "country_id": 479226790110,
                            "name": "Ōsaka",
                            "code": "JP-27",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636645598,
                            "country_id": 479226790110,
                            "name": "Hyōgo",
                            "code": "JP-28",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636678366,
                            "country_id": 479226790110,
                            "name": "Nara",
                            "code": "JP-29",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636711134,
                            "country_id": 479226790110,
                            "name": "Wakayama",
                            "code": "JP-30",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636743902,
                            "country_id": 479226790110,
                            "name": "Tottori",
                            "code": "JP-31",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636776670,
                            "country_id": 479226790110,
                            "name": "Shimane",
                            "code": "JP-32",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636809438,
                            "country_id": 479226790110,
                            "name": "Okayama",
                            "code": "JP-33",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636842206,
                            "country_id": 479226790110,
                            "name": "Hiroshima",
                            "code": "JP-34",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636874974,
                            "country_id": 479226790110,
                            "name": "Yamaguchi",
                            "code": "JP-35",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636907742,
                            "country_id": 479226790110,
                            "name": "Tokushima",
                            "code": "JP-36",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636940510,
                            "country_id": 479226790110,
                            "name": "Kagawa",
                            "code": "JP-37",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904636973278,
                            "country_id": 479226790110,
                            "name": "Ehime",
                            "code": "JP-38",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637006046,
                            "country_id": 479226790110,
                            "name": "Kōchi",
                            "code": "JP-39",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637038814,
                            "country_id": 479226790110,
                            "name": "Fukuoka",
                            "code": "JP-40",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637071582,
                            "country_id": 479226790110,
                            "name": "Saga",
                            "code": "JP-41",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637104350,
                            "country_id": 479226790110,
                            "name": "Nagasaki",
                            "code": "JP-42",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637137118,
                            "country_id": 479226790110,
                            "name": "Kumamoto",
                            "code": "JP-43",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637169886,
                            "country_id": 479226790110,
                            "name": "Ōita",
                            "code": "JP-44",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637202654,
                            "country_id": 479226790110,
                            "name": "Miyazaki",
                            "code": "JP-45",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637235422,
                            "country_id": 479226790110,
                            "name": "Kagoshima",
                            "code": "JP-46",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637268190,
                            "country_id": 479226790110,
                            "name": "Okinawa",
                            "code": "JP-47",
                            "tax": 0.0,
                            "tax_name": "Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        }
                    ]
                },
                {
                    "id": 479226822878,
                    "name": "South Korea",
                    "tax": 0.0,
                    "code": "KR",
                    "tax_name": "VAT",
                    "shipping_zone_id": 376959271134,
                    "provinces": [
                        {
                            "id": 4904637300958,
                            "country_id": 479226822878,
                            "name": "Busan",
                            "code": "KR-26",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637333726,
                            "country_id": 479226822878,
                            "name": "Chungbuk",
                            "code": "KR-43",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637366494,
                            "country_id": 479226822878,
                            "name": "Chungnam",
                            "code": "KR-44",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637399262,
                            "country_id": 479226822878,
                            "name": "Daegu",
                            "code": "KR-27",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637432030,
                            "country_id": 479226822878,
                            "name": "Daejeon",
                            "code": "KR-30",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637464798,
                            "country_id": 479226822878,
                            "name": "Gangwon",
                            "code": "KR-42",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637497566,
                            "country_id": 479226822878,
                            "name": "Gwangju",
                            "code": "KR-29",
                            "tax": 0.0,
                            "tax_name": "0.0",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637530334,
                            "country_id": 479226822878,
                            "name": "Gyeongbuk",
                            "code": "KR-47",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637563102,
                            "country_id": 479226822878,
                            "name": "Gyeonggi",
                            "code": "KR-41",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637595870,
                            "country_id": 479226822878,
                            "name": "Gyeongnam",
                            "code": "KR-48",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637628638,
                            "country_id": 479226822878,
                            "name": "Incheon",
                            "code": "KR-28",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637661406,
                            "country_id": 479226822878,
                            "name": "Jeju",
                            "code": "KR-49",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637694174,
                            "country_id": 479226822878,
                            "name": "Jeonbuk",
                            "code": "KR-45",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637726942,
                            "country_id": 479226822878,
                            "name": "Jeonnam",
                            "code": "KR-46",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637759710,
                            "country_id": 479226822878,
                            "name": "Sejong",
                            "code": "KR-50",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637792478,
                            "country_id": 479226822878,
                            "name": "Seoul",
                            "code": "KR-11",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637825246,
                            "country_id": 479226822878,
                            "name": "Ulsan",
                            "code": "KR-31",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        }
                    ]
                },
                {
                    "id": 479226855646,
                    "name": "Malaysia",
                    "tax": 0.0,
                    "code": "MY",
                    "tax_name": "SST",
                    "shipping_zone_id": 376959271134,
                    "provinces": [
                        {
                            "id": 4904637858014,
                            "country_id": 479226855646,
                            "name": "Johor",
                            "code": "JHR",
                            "tax": 0.0,
                            "tax_name": "SST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637890782,
                            "country_id": 479226855646,
                            "name": "Kedah",
                            "code": "KDH",
                            "tax": 0.0,
                            "tax_name": "SST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637923550,
                            "country_id": 479226855646,
                            "name": "Kelantan",
                            "code": "KTN",
                            "tax": 0.0,
                            "tax_name": "SST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637956318,
                            "country_id": 479226855646,
                            "name": "Kuala Lumpur",
                            "code": "KUL",
                            "tax": 0.0,
                            "tax_name": "SST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904637989086,
                            "country_id": 479226855646,
                            "name": "Labuan",
                            "code": "LBN",
                            "tax": 0.0,
                            "tax_name": "SST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638021854,
                            "country_id": 479226855646,
                            "name": "Melaka",
                            "code": "MLK",
                            "tax": 0.0,
                            "tax_name": "SST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638054622,
                            "country_id": 479226855646,
                            "name": "Negeri Sembilan",
                            "code": "NSN",
                            "tax": 0.0,
                            "tax_name": "SST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638087390,
                            "country_id": 479226855646,
                            "name": "Pahang",
                            "code": "PHG",
                            "tax": 0.0,
                            "tax_name": "SST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638120158,
                            "country_id": 479226855646,
                            "name": "Penang",
                            "code": "PNG",
                            "tax": 0.0,
                            "tax_name": "SST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638152926,
                            "country_id": 479226855646,
                            "name": "Perak",
                            "code": "PRK",
                            "tax": 0.0,
                            "tax_name": "SST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638185694,
                            "country_id": 479226855646,
                            "name": "Perlis",
                            "code": "PLS",
                            "tax": 0.0,
                            "tax_name": "SST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638218462,
                            "country_id": 479226855646,
                            "name": "Putrajaya",
                            "code": "PJY",
                            "tax": 0.0,
                            "tax_name": "SST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638251230,
                            "country_id": 479226855646,
                            "name": "Sabah",
                            "code": "SBH",
                            "tax": 0.0,
                            "tax_name": "SST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638283998,
                            "country_id": 479226855646,
                            "name": "Sarawak",
                            "code": "SWK",
                            "tax": 0.0,
                            "tax_name": "SST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638316766,
                            "country_id": 479226855646,
                            "name": "Selangor",
                            "code": "SGR",
                            "tax": 0.0,
                            "tax_name": "SST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638349534,
                            "country_id": 479226855646,
                            "name": "Terengganu",
                            "code": "TRG",
                            "tax": 0.0,
                            "tax_name": "SST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        }
                    ]
                },
                {
                    "id": 479226888414,
                    "name": "Netherlands",
                    "tax": 0.0,
                    "code": "NL",
                    "tax_name": "NL btw",
                    "shipping_zone_id": 376959271134,
                    "provinces": []
                },
                {
                    "id": 479226921182,
                    "name": "Norway",
                    "tax": 0.0,
                    "code": "NO",
                    "tax_name": "VAT",
                    "shipping_zone_id": 376959271134,
                    "provinces": []
                },
                {
                    "id": 479226953950,
                    "name": "New Zealand",
                    "tax": 0.0,
                    "code": "NZ",
                    "tax_name": "GST",
                    "shipping_zone_id": 376959271134,
                    "provinces": [
                        {
                            "id": 4904638382302,
                            "country_id": 479226953950,
                            "name": "Auckland",
                            "code": "AUK",
                            "tax": 0.0,
                            "tax_name": "GST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638415070,
                            "country_id": 479226953950,
                            "name": "Bay of Plenty",
                            "code": "BOP",
                            "tax": 0.0,
                            "tax_name": "GST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638447838,
                            "country_id": 479226953950,
                            "name": "Canterbury",
                            "code": "CAN",
                            "tax": 0.0,
                            "tax_name": "GST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638480606,
                            "country_id": 479226953950,
                            "name": "Chatham Islands",
                            "code": "CIT",
                            "tax": 0.0,
                            "tax_name": "GST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638513374,
                            "country_id": 479226953950,
                            "name": "Gisborne",
                            "code": "GIS",
                            "tax": 0.0,
                            "tax_name": "GST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638546142,
                            "country_id": 479226953950,
                            "name": "Hawke's Bay",
                            "code": "HKB",
                            "tax": 0.0,
                            "tax_name": "GST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638578910,
                            "country_id": 479226953950,
                            "name": "Manawatu-Wanganui",
                            "code": "MWT",
                            "tax": 0.0,
                            "tax_name": "GST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638611678,
                            "country_id": 479226953950,
                            "name": "Marlborough",
                            "code": "MBH",
                            "tax": 0.0,
                            "tax_name": "GST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638644446,
                            "country_id": 479226953950,
                            "name": "Nelson",
                            "code": "NSN",
                            "tax": 0.0,
                            "tax_name": "GST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638677214,
                            "country_id": 479226953950,
                            "name": "Northland",
                            "code": "NTL",
                            "tax": 0.0,
                            "tax_name": "GST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638709982,
                            "country_id": 479226953950,
                            "name": "Otago",
                            "code": "OTA",
                            "tax": 0.0,
                            "tax_name": "GST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638742750,
                            "country_id": 479226953950,
                            "name": "Southland",
                            "code": "STL",
                            "tax": 0.0,
                            "tax_name": "GST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638775518,
                            "country_id": 479226953950,
                            "name": "Taranaki",
                            "code": "TKI",
                            "tax": 0.0,
                            "tax_name": "GST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638808286,
                            "country_id": 479226953950,
                            "name": "Tasman",
                            "code": "TAS",
                            "tax": 0.0,
                            "tax_name": "GST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638841054,
                            "country_id": 479226953950,
                            "name": "Waikato",
                            "code": "WKO",
                            "tax": 0.0,
                            "tax_name": "GST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638873822,
                            "country_id": 479226953950,
                            "name": "Wellington",
                            "code": "WGN",
                            "tax": 0.0,
                            "tax_name": "GST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638906590,
                            "country_id": 479226953950,
                            "name": "West Coast",
                            "code": "WTC",
                            "tax": 0.0,
                            "tax_name": "GST",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        }
                    ]
                },
                {
                    "id": 479226986718,
                    "name": "Poland",
                    "tax": 0.0,
                    "code": "PL",
                    "tax_name": "PL VAT",
                    "shipping_zone_id": 376959271134,
                    "provinces": []
                },
                {
                    "id": 479227019486,
                    "name": "Portugal",
                    "tax": 0.0,
                    "code": "PT",
                    "tax_name": "PT VAT",
                    "shipping_zone_id": 376959271134,
                    "provinces": [
                        {
                            "id": 4904638939358,
                            "country_id": 479227019486,
                            "name": "Açores",
                            "code": "PT-20",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904638972126,
                            "country_id": 479227019486,
                            "name": "Aveiro",
                            "code": "PT-01",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639004894,
                            "country_id": 479227019486,
                            "name": "Beja",
                            "code": "PT-02",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639037662,
                            "country_id": 479227019486,
                            "name": "Braga",
                            "code": "PT-03",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639070430,
                            "country_id": 479227019486,
                            "name": "Bragança",
                            "code": "PT-04",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639103198,
                            "country_id": 479227019486,
                            "name": "Castelo Branco",
                            "code": "PT-05",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639135966,
                            "country_id": 479227019486,
                            "name": "Coimbra",
                            "code": "PT-06",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639168734,
                            "country_id": 479227019486,
                            "name": "Évora",
                            "code": "PT-07",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639201502,
                            "country_id": 479227019486,
                            "name": "Faro",
                            "code": "PT-08",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639234270,
                            "country_id": 479227019486,
                            "name": "Guarda",
                            "code": "PT-09",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639267038,
                            "country_id": 479227019486,
                            "name": "Leiria",
                            "code": "PT-10",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639299806,
                            "country_id": 479227019486,
                            "name": "Lisboa",
                            "code": "PT-11",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639332574,
                            "country_id": 479227019486,
                            "name": "Madeira",
                            "code": "PT-30",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639365342,
                            "country_id": 479227019486,
                            "name": "Portalegre",
                            "code": "PT-12",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639398110,
                            "country_id": 479227019486,
                            "name": "Porto",
                            "code": "PT-13",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639430878,
                            "country_id": 479227019486,
                            "name": "Santarém",
                            "code": "PT-14",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639463646,
                            "country_id": 479227019486,
                            "name": "Setúbal",
                            "code": "PT-15",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639496414,
                            "country_id": 479227019486,
                            "name": "Viana do Castelo",
                            "code": "PT-16",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639529182,
                            "country_id": 479227019486,
                            "name": "Vila Real",
                            "code": "PT-17",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639561950,
                            "country_id": 479227019486,
                            "name": "Viseu",
                            "code": "PT-18",
                            "tax": 0.0,
                            "tax_name": "VAT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        }
                    ]
                },
                {
                    "id": 479227052254,
                    "name": "Sweden",
                    "tax": 0.0,
                    "code": "SE",
                    "tax_name": "SE Moms",
                    "shipping_zone_id": 376959271134,
                    "provinces": []
                },
                {
                    "id": 479227085022,
                    "name": "Singapore",
                    "tax": 0.0,
                    "code": "SG",
                    "tax_name": "GST",
                    "shipping_zone_id": 376959271134,
                    "provinces": []
                },
                {
                    "id": 479227117790,
                    "name": "United States",
                    "tax": 0.0,
                    "code": "US",
                    "tax_name": "Federal Tax",
                    "shipping_zone_id": 376959271134,
                    "provinces": [
                        {
                            "id": 4904639594718,
                            "country_id": 479227117790,
                            "name": "Alabama",
                            "code": "AL",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639627486,
                            "country_id": 479227117790,
                            "name": "Alaska",
                            "code": "AK",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639660254,
                            "country_id": 479227117790,
                            "name": "American Samoa",
                            "code": "AS",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639693022,
                            "country_id": 479227117790,
                            "name": "Arizona",
                            "code": "AZ",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639725790,
                            "country_id": 479227117790,
                            "name": "Arkansas",
                            "code": "AR",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639758558,
                            "country_id": 479227117790,
                            "name": "California",
                            "code": "CA",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639791326,
                            "country_id": 479227117790,
                            "name": "Colorado",
                            "code": "CO",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639824094,
                            "country_id": 479227117790,
                            "name": "Connecticut",
                            "code": "CT",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639856862,
                            "country_id": 479227117790,
                            "name": "Delaware",
                            "code": "DE",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639889630,
                            "country_id": 479227117790,
                            "name": "District of Columbia",
                            "code": "DC",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639922398,
                            "country_id": 479227117790,
                            "name": "Federated States of Micronesia",
                            "code": "FM",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639955166,
                            "country_id": 479227117790,
                            "name": "Florida",
                            "code": "FL",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904639987934,
                            "country_id": 479227117790,
                            "name": "Georgia",
                            "code": "GA",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640020702,
                            "country_id": 479227117790,
                            "name": "Guam",
                            "code": "GU",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640053470,
                            "country_id": 479227117790,
                            "name": "Hawaii",
                            "code": "HI",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640086238,
                            "country_id": 479227117790,
                            "name": "Idaho",
                            "code": "ID",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640119006,
                            "country_id": 479227117790,
                            "name": "Illinois",
                            "code": "IL",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640151774,
                            "country_id": 479227117790,
                            "name": "Indiana",
                            "code": "IN",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640184542,
                            "country_id": 479227117790,
                            "name": "Iowa",
                            "code": "IA",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640217310,
                            "country_id": 479227117790,
                            "name": "Kansas",
                            "code": "KS",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640250078,
                            "country_id": 479227117790,
                            "name": "Kentucky",
                            "code": "KY",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640282846,
                            "country_id": 479227117790,
                            "name": "Louisiana",
                            "code": "LA",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640315614,
                            "country_id": 479227117790,
                            "name": "Maine",
                            "code": "ME",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640348382,
                            "country_id": 479227117790,
                            "name": "Marshall Islands",
                            "code": "MH",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640381150,
                            "country_id": 479227117790,
                            "name": "Maryland",
                            "code": "MD",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640413918,
                            "country_id": 479227117790,
                            "name": "Massachusetts",
                            "code": "MA",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640446686,
                            "country_id": 479227117790,
                            "name": "Michigan",
                            "code": "MI",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640479454,
                            "country_id": 479227117790,
                            "name": "Minnesota",
                            "code": "MN",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640512222,
                            "country_id": 479227117790,
                            "name": "Mississippi",
                            "code": "MS",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640544990,
                            "country_id": 479227117790,
                            "name": "Missouri",
                            "code": "MO",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640577758,
                            "country_id": 479227117790,
                            "name": "Montana",
                            "code": "MT",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640610526,
                            "country_id": 479227117790,
                            "name": "Nebraska",
                            "code": "NE",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640643294,
                            "country_id": 479227117790,
                            "name": "Nevada",
                            "code": "NV",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640676062,
                            "country_id": 479227117790,
                            "name": "New Hampshire",
                            "code": "NH",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640708830,
                            "country_id": 479227117790,
                            "name": "New Jersey",
                            "code": "NJ",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640741598,
                            "country_id": 479227117790,
                            "name": "New Mexico",
                            "code": "NM",
                            "tax": 0.0,
                            "tax_name": "GRT",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640774366,
                            "country_id": 479227117790,
                            "name": "New York",
                            "code": "NY",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640807134,
                            "country_id": 479227117790,
                            "name": "North Carolina",
                            "code": "NC",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640839902,
                            "country_id": 479227117790,
                            "name": "North Dakota",
                            "code": "ND",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640872670,
                            "country_id": 479227117790,
                            "name": "Northern Mariana Islands",
                            "code": "MP",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640905438,
                            "country_id": 479227117790,
                            "name": "Ohio",
                            "code": "OH",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640938206,
                            "country_id": 479227117790,
                            "name": "Oklahoma",
                            "code": "OK",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904640970974,
                            "country_id": 479227117790,
                            "name": "Oregon",
                            "code": "OR",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904641003742,
                            "country_id": 479227117790,
                            "name": "Palau",
                            "code": "PW",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904641036510,
                            "country_id": 479227117790,
                            "name": "Pennsylvania",
                            "code": "PA",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904641069278,
                            "country_id": 479227117790,
                            "name": "Puerto Rico",
                            "code": "PR",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904641102046,
                            "country_id": 479227117790,
                            "name": "Rhode Island",
                            "code": "RI",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904641134814,
                            "country_id": 479227117790,
                            "name": "South Carolina",
                            "code": "SC",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904641167582,
                            "country_id": 479227117790,
                            "name": "South Dakota",
                            "code": "SD",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904641200350,
                            "country_id": 479227117790,
                            "name": "Tennessee",
                            "code": "TN",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904641233118,
                            "country_id": 479227117790,
                            "name": "Texas",
                            "code": "TX",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904641265886,
                            "country_id": 479227117790,
                            "name": "Utah",
                            "code": "UT",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904641298654,
                            "country_id": 479227117790,
                            "name": "Vermont",
                            "code": "VT",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904641331422,
                            "country_id": 479227117790,
                            "name": "Virginia",
                            "code": "VA",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904641364190,
                            "country_id": 479227117790,
                            "name": "Washington",
                            "code": "WA",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904641396958,
                            "country_id": 479227117790,
                            "name": "West Virginia",
                            "code": "WV",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904641429726,
                            "country_id": 479227117790,
                            "name": "Wisconsin",
                            "code": "WI",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904641462494,
                            "country_id": 479227117790,
                            "name": "Wyoming",
                            "code": "WY",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904641495262,
                            "country_id": 479227117790,
                            "name": "Virgin Islands",
                            "code": "VI",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904641528030,
                            "country_id": 479227117790,
                            "name": "Armed Forces Americas",
                            "code": "AA",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904641560798,
                            "country_id": 479227117790,
                            "name": "Armed Forces Europe",
                            "code": "AE",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        },
                        {
                            "id": 4904641593566,
                            "country_id": 479227117790,
                            "name": "Armed Forces Pacific",
                            "code": "AP",
                            "tax": 0.0,
                            "tax_name": "State Tax",
                            "tax_type": null,
                            "tax_percentage": 0.0,
                            "shipping_zone_id": 376959271134
                        }
                    ]
                }
            ],
            "weight_based_shipping_rates": [],
            "price_based_shipping_rates": [
                {
                    "id": 557589070046,
                    "name": "Standard",
                    "price": "1700.00",
                    "shipping_zone_id": 376959271134,
                    "min_order_subtotal": null,
                    "max_order_subtotal": null
                }
            ],
            "carrier_shipping_rate_providers": []
        }
    ]
    """
    return json.loads(shipping_zones)
