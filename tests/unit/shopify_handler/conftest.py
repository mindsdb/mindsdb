import json

import pandas
import pytest
import requests
import shopify

from mindsdb.integrations.handlers.shopify_handler import Handler
from mindsdb.integrations.handlers.shopify_handler.shopify_tables import ProductsTable, CustomersTable, OrdersTable

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
