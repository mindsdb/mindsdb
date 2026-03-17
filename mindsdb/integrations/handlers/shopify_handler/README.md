# Shopify Handler

## Overview

The Shopify Handler for MindsDB provides an interface to connect to Shopify stores via GraphQL API and enables executing SQL queries against store data.

---

## Table of Contents

- [Shopify Handler Implementation](#shopify-handler-implementation)
- [Connection Initialization](#connection-initialization)
- [Available Tables](#available-tables)
- [Supported Operations](#supported-operations)
- [Usage Examples](#usage-examples)
- [Technical Details](#technical-details)

---


## Shopify Handler Implementation

This handler is implemented using:
- **ShopifyAPI** - the official Python library for Shopify
- **Shopify GraphQL Admin API** version **2025-10**

All requests to Shopify are executed through the GraphQL API, which ensures efficient data loading with the ability to select only the necessary fields.

Where possible, sorting and filtering are performed on the Shopify side. When this is not supported, sorting and filtering are handled on the MindsDB side.

---

## Connection Initialization

To connect to Shopify, the following parameters are required:

### Required Parameters

| Parameter | Type | Description |
|----------|-----|----------|
| `shop_url` | `str` | Your store URL (e.g., `shop-123456.myshopify.com`) |
| `client_id` | `str` | Client ID of your Shopify app |
| `client_secret` | `str` | Client Secret of your Shopify app |

### Creating a Connection

```sql
CREATE DATABASE shopify_store
WITH ENGINE = 'shopify',
PARAMETERS = {
  "shop_url": "your-shop-name.myshopify.com",
  "client_id": "your_client_id",
  "client_secret": "your_client_secret"
};
```

### Obtaining Credentials

1. Log in to the Shopify admin panel
2. Navigate to **https://dev.shopify.com/dashboard/**
3. Create a new app. During app creation, define the required scopes. Depending on which tables you need to access, you may want to add:
   - `read_products` - for access to products and variants
   - `read_customers` - for access to customer data
   - `read_orders` (or `read_marketplace_orders`, `read_quick_sale`) - for access to orders
   - `read_inventory` - for access to inventory data
   - `read_marketing_events` - for access to marketing events
   - `read_staff` - for access to staff member data
   - `read_gift_cards` - for access to gift cards
4. In the **Settings** section, locate:
   - **Client ID**
   - **Secret**
5. Install the app to your store.

---

## Available Tables

The handler provides access to the following tables:

### 1. `products` - Products

Contains information about store products.

**Key Fields:**
- `id` - Unique product identifier
- `title` - Product name
- `description` - Product description
- `vendor` - Manufacturer/supplier
- `productType` - Product type
- `status` - Product status (active, draft, archived)
- `tags` - Product tags
- `createdAt` - Creation date
- `updatedAt` - Last update date
- `totalInventory` - Total stock quantity
- `priceRangeV2` - Price range
- `variantsCount` - Number of variants

**Shopify Native Sorting:**
This table supports native sorting in Shopify API by the following fields: `createdAt`, `id`, `totalInventory`, `productType`, `publishedAt`, `title`, `updatedAt`, `vendor`.

**Shopify Native Filtering:**
This table supports native filtering in Shopify API by the following fields: `createdAt`, `id`, `isGiftCard`, `handle`, `totalInventory`, `productType`, `publishedAt`, `status`, `title`, `updatedAt`, `vendor`.

### 2. `product_variants` - Product Variants

Contains information about product variants (sizes, colors, etc.).

**Key Fields:**
- `id` - Unique variant identifier
- `productId` - Parent product ID
- `title` - Variant name
- `displayName` - Display name
- `sku` - Stock Keeping Unit
- `barcode` - Barcode
- `inventoryQuantity` - Stock quantity
- `position` - Position in list

**Shopify Native Sorting:**
This table supports native sorting in Shopify API by the following fields: `id`, `inventoryQuantity`, `displayName`, `position`, `sku`, `title`.

**Shopify Native Filtering:**
This table supports native filtering in Shopify API by the following fields: `barcode`, `id`, `inventoryQuantity`, `productId`, `sku`, `title`, `updatedAt`.

### 3. `customers` - Customers

Contains information about store customers.

**Key Fields:**
- `id` - Unique customer identifier
- `firstName` - First name
- `lastName` - Last name
- `emailAddress` - Email address
- `phoneNumber` - Phone number
- `displayName` - Display name
- `country` - Country
- `createdAt` - Creation date
- `updatedAt` - Update date
- `numberOfOrders` - Number of orders
- `amountSpent` - Total amount spent
- `tags` - Customer tags
- `state` - Account state
- `verifiedEmail` - Email verified

**Shopify Native Sorting:**
This table supports native sorting in Shopify API by the following fields: `createdAt`, `id`, `updatedAt`.

**Shopify Native Filtering:**
This table supports native filtering in Shopify API by the following fields: `country`, `createdAt`, `email`, `firstName`, `id`, `lastName`, `phoneNumber`, `updatedAt`.

### 4. `orders` - Orders

Contains information about orders.

**Key Fields:**
- `id` - Unique order identifier
- `name` - Order number (e.g., #1001)
- `number` - Numeric order number
- `customerId` - Customer ID
- `email` - Customer email
- `phone` - Customer phone
- `createdAt` - Creation date
- `processedAt` - Processing date
- `updatedAt` - Update date
- `cancelledAt` - Cancellation date
- `currentTotalPriceSet` - Current total price
- `currentSubtotalPriceSet` - Current subtotal price
- `totalWeight` - Total weight
- `test` - Test order
- `shippingAddress` - Shipping address
- `tags` - Order tags

**Shopify Native Sorting:**
This table supports native sorting in Shopify API by the following fields: `createdAt`, `id`, `number`, `poNumber`, `processedAt`, `updatedAt`.

**Shopify Native Filtering:**
This table supports native filtering in Shopify API by the following fields: `confirmationNumber`, `createdAt`, `customerId`, `discountCode`, `email`, `id`, `name`, `poNumber`, `processedAt`, `returnStatus`, `sourceIdentifier`, `sourceName`, `test`, `totalWeight`, `updatedAt`.

### 5. `marketing_events` - Marketing Events

Contains information about marketing events and campaigns.

**Key Fields:**
- `id` - Unique event identifier
- `startedAt` - Start date
- `description` - Description
- `type` - Event type

**Shopify Native Sorting:**
This table supports native sorting in Shopify API by the following fields: `id`, `startedAt`.

**Shopify Native Filtering:**
This table supports native filtering in Shopify API by the following fields: `id`, `startedAt`, `description`, `type`.

### 6. `inventory_items` - Inventory Items

Contains information about inventory items.

**Key Fields:**
- `id` - Unique identifier
- `sku` - Stock Keeping Unit
- `createdAt` - Creation date
- `updatedAt` - Update date

**Shopify Native Sorting:**
This table does not support native sorting in Shopify API. Sorting is handled on the MindsDB side.

**Shopify Native Filtering:**
This table supports native filtering in Shopify API by the following fields: `id`, `createdAt`, `sku`, `updatedAt`.

### 7. `staff_members` - Staff Members

Contains information about store staff members.

**Key Fields:**
- `id` - Unique identifier
- `firstName` - First name
- `lastName` - Last name
- `email` - Email
- `accountType` - Account type

**Shopify Native Sorting:**
This table supports native sorting in Shopify API by the following fields: `id`, `email`, `firstName`, `lastName`.

**Shopify Native Filtering:**
This table supports native filtering in Shopify API by the following fields: `accountType`, `email`, `firstName`, `lastName`, `id`.

### 8. `gift_cards` - Gift Cards

Contains information about gift cards.

**Key Fields:**
- `id` - Unique identifier
- `balance` - Current balance
- `initialValue` - Initial value
- `customerId` - Customer ID
- `orderId` - Order ID
- `createdAt` - Creation date
- `updatedAt` - Update date
- `expiresOn` - Expiration date
- `deactivatedAt` - Deactivation date

**Shopify Native Sorting:**
This table supports native sorting in Shopify API by the following fields: `balance`, `createdAt`, `deactivatedAt`, `expiresOn`, `id`, `initialValue`, `updatedAt`.

**Shopify Native Filtering:**
This table supports native filtering in Shopify API by the following fields: `createdAt`, `expiresOn`, `id`.

---

## Supported Operations

### SELECT - Reading Data

All tables support the SELECT operation with the following capabilities:

- **SELECT** - data selection
- **WHERE** - filtering by conditions (depending on the table)
- **ORDER BY** - sorting (depending on the table)
- **LIMIT** - limiting the number of records
- **Selecting specific columns** - query optimization

### WHERE Operators

Depending on the field and table, the following operators are supported:

- `=` (EQUAL) - equality
- `>` (GREATER_THAN) - greater than
- `>=` (GREATER_THAN_OR_EQUAL) - greater than or equal
- `<` (LESS_THAN) - less than
- `<=` (LESS_THAN_OR_EQUAL) - less than or equal
- `IN` - check for inclusion in a list (for some fields)
- `LIKE` - partial match (for some text fields)

---

## Usage Examples

### Basic SELECT Queries

#### Get All Products

```sql
SELECT id, title, vendor, status, totalInventory
FROM shopify_store.products;
```

#### Get Active Products with Sorting

```sql
SELECT id, title, vendor, totalInventory, createdAt
FROM shopify_store.products
WHERE status = 'active'
ORDER BY createdAt DESC
LIMIT 10;
```

#### Get Products from a Specific Vendor

```sql
SELECT id, title, productType, priceRangeV2
FROM shopify_store.products
WHERE vendor = 'Nike'
ORDER BY title;
```

#### Get Customers from a Specific Country

```sql
SELECT id, displayName, emailAddress, phoneNumber, numberOfOrders
FROM shopify_store.customers
WHERE country = 'Canada'
ORDER BY numberOfOrders DESC
LIMIT 20;
```

#### Get Customers Created After a Specific Date

```sql
SELECT id, firstName, lastName, emailAddress, createdAt
FROM shopify_store.customers
WHERE createdAt > '2024-01-01'
ORDER BY createdAt DESC;
```

#### Get Orders for a Specific Customer

```sql
SELECT id, name, email, currentTotalPriceSet, processedAt
FROM shopify_store.orders
WHERE customerId = 'gid://shopify/Customer/123456789'
ORDER BY processedAt DESC;
```
#### Get Orders with Customer Information

```sql
SELECT 
    o.id as order_id,
    o.name as order_name,
    o.createdAt as order_date,
    c.displayName as customer_name,
    c.emailAddress as customer_email,
    o.currentTotalPriceSet as total
FROM shopify_store.orders o
JOIN shopify_store.customers c ON o.customerId = c.id
WHERE o.createdAt > '2024-01-01'
ORDER BY o.createdAt DESC
LIMIT 50;
```

---

## Technical Details

### API Version

The connector uses **Shopify GraphQL Admin API version 2025-10**.

### Authentication

The connector uses OAuth 2.0 authentication with the `client_credentials` grant type:

1. Upon connection, a POST request is made to `https://{shop_url}/admin/oauth/access_token`
2. The `client_id` and `client_secret` are sent
3. An `access_token` is obtained for subsequent requests
4. A Shopify session is created with the obtained token

### GraphQL Queries

All data is loaded through the GraphQL API using nodes and connections. The connector automatically:

- Selects only requested fields for optimization
- Applies WHERE filters to GraphQL queries
- Performs sorting at the API level
- Limits the number of returned records

### Limitations

- The connector provides **read-only access** (SELECT)
- INSERT, UPDATE, DELETE operations are not supported in the current version
- Some fields may be null depending on store settings
- The current version does not handle API rate limiting
- For sorting, it is recommended to use fields that have native sorting support in the Shopify API

## Useful Links

- [Shopify GraphQL Admin API Documentation](https://shopify.dev/docs/api/admin-graphql)
- [Shopify API Permissions](https://shopify.dev/docs/api/usage/access-scopes)
- [Shopify App Development](https://shopify.dev/docs/apps)
- [ShopifyAPI Python Library](https://github.com/Shopify/shopify_python_api)

---
