# Welcome to the MindsDB Manual QA Testing for Shopify Handler

## Testing Shopify Handler

**1. Testing CREATE DATABASE**

```sql
CREATE DATABASE shopify_datasource
WITH ENGINE = 'shopify',
PARAMETERS = {
  "shop_url": "mindsdbtestt.myshopify.com",
  "access_token": "shpat_84bec93cb80fc0f"
};
```

The result is as follows:
[![shopify-create-database.png](https://i.postimg.cc/t70Vw72n/shopify-create-database.png)](https://postimg.cc/GTQ2s30b)


**2. Testing SELECT method**

```sql
SELECT * FROM shopify_datasource.products LIMIT 10;
```

The result is as follows:
[![shopify-select-method.png](https://i.postimg.cc/LXYW3074/shopify-select-method.png)](https://postimg.cc/k6CwnwqL)

**3. Testing Insert  method**


```sql
INSERT INTO shopify_datasource.customers(first_name, last_name, email)
VALUES 
('John', 'Doe', 'john.doe@example.com')
```

The result is the following:
[![insert-method.png](https://i.postimg.cc/dVhWpwhh/insert-method.png)](https://postimg.cc/3kHCDMCh)

### Results

Drop a remark based on your observation.
- [x] Works Great 💚 (This means that all the steps were executed successfully and the expected outputs were returned.)