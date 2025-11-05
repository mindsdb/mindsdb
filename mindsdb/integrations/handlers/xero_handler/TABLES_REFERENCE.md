# Xero Connector Tables Reference

This document provides comprehensive descriptions of all tables available in the MindsDB Xero connector. Each table description includes its purpose, the type of data it contains, and key columns that are most relevant for queries.

## Table of Contents

- [Data Tables (Transactional Data)](#data-tables-transactional-data)
  - [accounts](#accounts)
  - [bank_transactions](#bank_transactions)
  - [bank_transfers](#bank_transfers)
  - [budgets](#budgets)
  - [contacts](#contacts)
  - [contact_groups](#contact_groups)
  - [credit_notes](#credit_notes)
  - [invoices](#invoices)
  - [items](#items)
  - [journals](#journals)
  - [manual_journals](#manual_journals)
  - [organisations](#organisations)
  - [overpayments](#overpayments)
  - [payments](#payments)
  - [prepayments](#prepayments)
  - [purchase_orders](#purchase_orders)
  - [quotes](#quotes)
  - [repeating_invoices](#repeating_invoices)
- [Report Tables (Financial Reports)](#report-tables-financial-reports)
  - [balance_sheet_report](#balance_sheet_report)
  - [profit_loss_report](#profit_loss_report)
  - [trial_balance_report](#trial_balance_report)
  - [bank_summary_report](#bank_summary_report)
  - [budget_summary_report](#budget_summary_report)
  - [executive_summary_report](#executive_summary_report)
- [Usage Notes](#usage-notes)

---

## Data Tables (Transactional Data)

### accounts

**Description:** Chart of accounts containing all account records used for categorizing transactions. Includes account codes, names, types (revenue, expense, asset, liability, equity, bank), tax types, currency information, and banking details. Essential for understanding how transactions are categorized across the organization's financial structure.

**Key Columns:**
- `account_id` - Unique identifier for the account
- `code` - Account code used in the chart of accounts
- `name` - Display name of the account
- `type` - Account type (e.g., REVENUE, EXPENSE, BANK, CURRENT, FIXED)
- `account_class` - Classification (ASSET, LIABILITY, EQUITY, REVENUE, EXPENSE)
- `tax_type` - Default tax type for transactions on this account
- `status` - Account status (ACTIVE, ARCHIVED, DELETED)
- `currency_code` - Currency for the account
- `bank_account_number` - Bank account number if applicable
- `updated_date_utc` - Last modification timestamp

**Use Cases:** Financial categorization, chart of accounts management, transaction classification, tax reporting

---

### bank_transactions

**Description:** All transactions recorded against bank accounts including money in and money out. Contains spend money, receive money, and bank transfer transactions with full contact details, line items, tax information, and reconciliation status. Critical for cash flow analysis and bank reconciliation.

**Key Columns:**
- `bank_transaction_id` - Unique identifier for the transaction
- `type` - Transaction type (SPEND, RECEIVE, SPEND-OVERPAYMENT, RECEIVE-OVERPAYMENT, etc.)
- `contact_id` - Associated contact identifier
- `contact_name` - Name of the contact
- `date` - Transaction date
- `total` - Total transaction amount including tax
- `sub_total` - Transaction amount excluding tax
- `total_tax` - Total tax amount
- `status` - Transaction status (AUTHORISED, DELETED, VOIDED)
- `is_reconciled` - Whether the transaction has been reconciled
- `line_items` - JSON array of line item details
- `currency_code` - Transaction currency
- `reference` - Transaction reference number
- `bank_account_*` - Bank account details (code, name, type, etc.)

**Use Cases:** Cash flow analysis, bank reconciliation, expense tracking, revenue analysis, tax reporting

---

### bank_transfers

**Description:** Internal transfers between bank accounts within Xero. Tracks money movement between different bank accounts, including transfer dates, amounts, and reconciliation status. Useful for understanding internal cash movements.

**Key Columns:**
- `bank_transfer_id` - Unique identifier for the transfer
- `from_bank_account_*` - Source bank account details
- `to_bank_account_*` - Destination bank account details
- `amount` - Transfer amount
- `date` - Transfer date
- `has_attachments` - Whether supporting documents are attached
- `currency_code` - Transfer currency

**Use Cases:** Internal fund transfers, cash management, bank reconciliation, liquidity management

---

### budgets

**Description:** Budget records containing planned financial targets organized by account and time period. Includes budget lines with tracking category breakdowns. Essential for comparing actual performance against planned targets and variance analysis.

**Key Columns:**
- `budget_id` - Unique identifier for the budget
- `type` - Budget type (OVERALL or specific tracking option)
- `description` - Budget description
- `status` - Budget status (ACTIVE, DRAFT)
- `budget_lines` - JSON array of budget line items by account and period
- `tracking` - JSON array of tracking category assignments
- `updated_date_utc` - Last modification timestamp

**Use Cases:** Budget vs. actual analysis, financial planning, variance reporting, performance management

---

### contacts

**Description:** Customer and supplier contact records including individuals and organizations. Contains names, contact details, addresses, phone numbers, tax information, bank details, and customer/supplier status flags. Central to managing business relationships and transaction associations.

**Key Columns:**
- `contact_id` - Unique identifier for the contact
- `name` - Contact name (company or person)
- `first_name` - First name (for individuals)
- `last_name` - Last name (for individuals)
- `email_address` - Primary email address
- `is_customer` - Flag indicating if contact is a customer
- `is_supplier` - Flag indicating if contact is a supplier
- `contact_status` - Contact status (ACTIVE, ARCHIVED, GDPR_REQUEST)
- `addresses` - JSON array of postal and street addresses
- `phones` - JSON array of phone numbers
- `tax_number` - Tax identification number
- `default_currency` - Default currency for transactions
- `company_number` - Company registration number
- `bank_account_details` - Bank account information
- `updated_date_utc` - Last modification timestamp

**Use Cases:** Customer/supplier management, contact information, CRM integration, transaction filtering

---

### contact_groups

**Description:** Groupings of contacts for organizational and reporting purposes. Allows categorization of contacts (e.g., "VIP Customers", "Local Suppliers") for targeted communication and analysis.

**Key Columns:**
- `contact_group_id` - Unique identifier for the group
- `name` - Group name
- `status` - Group status (ACTIVE, DELETED)

**Use Cases:** Contact segmentation, targeted reporting, customer categorization, marketing lists

---

### credit_notes

**Description:** Credit notes issued to customers or received from suppliers, representing refunds or corrections to invoices. Includes allocation details showing how credits are applied against invoices, remaining credit amounts, and full line item breakdowns.

**Key Columns:**
- `credit_note_id` - Unique identifier for the credit note
- `credit_note_number` - Credit note number
- `type` - Type (ACCPAYCREDIT for bills, ACCRECCREDIT for sales)
- `contact_id` - Associated contact identifier
- `contact_name` - Contact name
- `date` - Credit note date
- `total` - Total credit amount including tax
- `sub_total` - Credit amount excluding tax
- `total_tax` - Total tax amount
- `remaining_credit` - Unallocated credit remaining
- `allocations` - JSON array showing how credit is allocated to invoices
- `line_items` - JSON array of line item details
- `status` - Credit note status (DRAFT, SUBMITTED, AUTHORISED, PAID, VOIDED, DELETED)
- `currency_code` - Transaction currency
- `updated_date_utc` - Last modification timestamp

**Use Cases:** Returns processing, invoice corrections, credit management, refund tracking

---

### invoices

**Description:** Sales invoices (accounts receivable) and bills (accounts payable) representing amounts owed to or by the organization. Contains complete transaction details including line items, payments applied, credit notes, amounts due/paid, tax calculations, and due dates. Core to revenue and expense tracking.

**Key Columns:**
- `invoice_id` - Unique identifier for the invoice
- `invoice_number` - Invoice number
- `type` - Invoice type (ACCPAY for bills, ACCREC for sales invoices)
- `contact_id` - Associated contact identifier
- `contact_name` - Contact name
- `date` - Invoice date
- `due_date` - Payment due date
- `total` - Total invoice amount including tax
- `sub_total` - Invoice amount excluding tax
- `total_tax` - Total tax amount
- `amount_due` - Outstanding amount still owed
- `amount_paid` - Amount already paid
- `amount_credited` - Amount credited via credit notes
- `status` - Invoice status (DRAFT, SUBMITTED, AUTHORISED, PAID, VOIDED, DELETED)
- `line_items` - JSON array of line item details
- `payments` - JSON array of payment records
- `credit_notes` - JSON array of applied credit notes
- `currency_code` - Transaction currency
- `currency_rate` - Exchange rate if multi-currency
- `reference` - Invoice reference
- `branding_theme_id` - Applied branding theme
- `has_attachments` - Whether supporting documents are attached
- `updated_date_utc` - Last modification timestamp

**Use Cases:** Accounts receivable/payable, revenue recognition, expense tracking, aging reports, payment collection

---

### items

**Description:** Product and service catalog items that can be used on invoices, bills, and quotes. Includes item codes, descriptions, pricing, account codes for revenue/expense, and inventory tracking flags. Streamlines line item entry and ensures consistent pricing.

**Key Columns:**
- `item_id` - Unique identifier for the item
- `code` - Item code/SKU
- `name` - Item name
- `description` - Item description for sales
- `purchase_description` - Description for purchases
- `sales_details_unit_price` - Default sales price
- `sales_details_account_code` - Revenue account code
- `sales_details_tax_type` - Sales tax type
- `purchase_details` - JSON object with purchase details
- `is_tracked_as_inventory` - Whether item quantity is tracked
- `is_sold` - Whether item can be sold
- `is_purchased` - Whether item can be purchased
- `updated_date_utc` - Last modification timestamp

**Use Cases:** Product catalog, pricing management, inventory tracking, consistent invoicing, cost analysis

---

### journals

**Description:** System-generated journal entries showing the double-entry bookkeeping records for all transactions. Each journal contains debit and credit lines that maintain the accounting equation. Essential for audit trails and understanding the underlying accounting entries.

**Key Columns:**
- `journal_id` - Unique identifier for the journal
- `journal_number` - Sequential journal number
- `journal_date` - Date of the journal entry
- `created_date_utc` - Creation timestamp
- `journal_lines` - JSON array of debit and credit line details including accounts, amounts, descriptions

**Use Cases:** Audit trails, accounting verification, double-entry bookkeeping analysis, transaction investigation

---

### manual_journals

**Description:** User-created journal entries for adjustments, corrections, and transactions not captured through standard workflows (like depreciation, accruals, or corrections). Includes narration for documentation and complete debit/credit line details.

**Key Columns:**
- `manual_journal_id` - Unique identifier for the manual journal
- `date` - Journal entry date
- `status` - Journal status (DRAFT, POSTED, DELETED, VOIDED)
- `narration` - Description/explanation of the journal entry
- `journal_lines` - JSON array of debit and credit lines
- `line_amount_types` - Whether amounts include tax (INCLUSIVE, EXCLUSIVE, NOTAX)
- `show_on_cash_basis_reports` - Whether to include in cash basis reporting
- `has_attachments` - Whether supporting documents are attached
- `updated_date_utc` - Last modification timestamp

**Use Cases:** Accounting adjustments, period-end entries, depreciation, accruals, error corrections

---

### organisations

**Description:** Organization profile information including legal details, tax settings, financial year configuration, base currency, and contact information. Typically contains a single record representing the connected Xero organization. Important for understanding business context.

**Key Columns:**
- `organisation_id` - Unique identifier (tenant ID)
- `name` - Organization trading name
- `legal_name` - Legal entity name
- `country_code` - Country of operation
- `base_currency` - Base currency code
- `tax_number` - Tax identification number
- `tax_number_name` - Tax number type (e.g., ABN, VAT, GST)
- `financial_year_end_month` - Month when financial year ends
- `financial_year_end_day` - Day when financial year ends
- `timezone` - Organization timezone
- `organisation_type` - Type of organization
- `edition` - Xero edition (BUSINESS, PARTNER, etc.)
- `addresses` - JSON array of addresses
- `phones` - JSON array of phone numbers
- `created_date_utc` - Organization creation date

**Use Cases:** Organization context, multi-tenant management, configuration reference, financial year calculations

---

### overpayments

**Description:** Payments that exceed the invoice amount, creating a credit balance. Tracks the overpayment amount, how it's allocated against other invoices, and remaining credit available. Important for managing customer prepayments and supplier overpayments.

**Key Columns:**
- `overpayment_id` - Unique identifier for the overpayment
- `contact_id` - Associated contact identifier
- `contact_name` - Contact name
- `date` - Overpayment date
- `total` - Total overpayment amount
- `sub_total` - Amount excluding tax
- `total_tax` - Tax amount
- `remaining_credit` - Unallocated credit remaining
- `allocations` - JSON array showing how overpayment is allocated
- `status` - Overpayment status (AUTHORISED, PAID, VOIDED)
- `currency_code` - Transaction currency
- `currency_rate` - Exchange rate if multi-currency
- `type` - Overpayment type (RECEIVE-OVERPAYMENT, SPEND-OVERPAYMENT)
- `updated_date_utc` - Last modification timestamp

**Use Cases:** Credit management, overpayment tracking, allocation to invoices, customer account balances

---

### payments

**Description:** All payment records linking to invoices, credit notes, overpayments, or prepayments. Shows payment dates, amounts, bank accounts, payment methods, and reconciliation status. Critical for cash management and accounts receivable/payable tracking.

**Key Columns:**
- `payment_id` - Unique identifier for the payment
- `date` - Payment date
- `amount` - Payment amount in invoice currency
- `bank_amount` - Payment amount in bank account currency
- `account_id` - Bank account identifier
- `account_code` - Bank account code
- `invoice_id` - Associated invoice identifier (if applicable)
- `invoice_number` - Invoice number
- `invoice_type` - Type of invoice (ACCPAY, ACCREC)
- `payment_type` - Payment method (e.g., ACCRECPAYMENT, ACCPAYPAYMENT)
- `status` - Payment status (AUTHORISED, DELETED)
- `is_reconciled` - Whether payment has been reconciled
- `reference` - Payment reference
- `currency_rate` - Exchange rate if multi-currency
- `batch_payment_id` - Batch payment identifier if part of a batch
- `updated_date_utc` - Last modification timestamp

**Use Cases:** Payment tracking, cash receipts, payment reconciliation, accounts receivable/payable management

---

### prepayments

**Description:** Payments made before receiving goods/services or received before delivering goods/services. Tracks how prepayments are allocated over time as invoices are issued. Important for managing advance payments and deferred revenue.

**Key Columns:**
- `prepayment_id` - Unique identifier for the prepayment
- `contact_id` - Associated contact identifier
- `contact_name` - Contact name
- `date` - Prepayment date
- `total` - Total prepayment amount
- `sub_total` - Amount excluding tax
- `total_tax` - Tax amount
- `remaining_credit` - Unallocated prepayment remaining
- `allocations` - JSON array showing how prepayment is allocated to invoices
- `status` - Prepayment status (AUTHORISED, PAID, VOIDED)
- `currency_code` - Transaction currency
- `currency_rate` - Exchange rate if multi-currency
- `type` - Prepayment type (RECEIVE-PREPAYMENT, SPEND-PREPAYMENT)
- `line_amount_types` - Tax treatment
- `updated_date_utc` - Last modification timestamp

**Use Cases:** Advance payment tracking, deferred revenue management, prepayment allocation, customer deposits

---

### purchase_orders

**Description:** Purchase orders sent to suppliers representing commitments to purchase goods or services. Contains order details, delivery information, expected arrival dates, line items, and approval status. Essential for procurement tracking and three-way matching.

**Key Columns:**
- `purchase_order_id` - Unique identifier for the purchase order
- `purchase_order_number` - PO number
- `contact_id` - Supplier contact identifier
- `contact_name` - Supplier name
- `date` - Purchase order date
- `delivery_date` - Expected delivery date
- `expected_arrival_date` - Expected arrival date
- `status` - PO status (DRAFT, SUBMITTED, AUTHORISED, BILLED, DELETED)
- `type` - PO type (PURCHASEORDER)
- `total` - Total PO amount including tax
- `sub_total` - Amount excluding tax
- `total_tax` - Tax amount
- `line_items` - JSON array of line item details
- `delivery_address` - Delivery address
- `delivery_instructions` - Special delivery instructions
- `attention_to` - Person to address PO to
- `reference` - PO reference
- `currency_code` - Transaction currency
- `has_attachments` - Whether supporting documents are attached
- `updated_date_utc` - Last modification timestamp

**Use Cases:** Procurement management, purchase tracking, three-way matching, supplier orders, commitment reporting

---

### quotes

**Description:** Sales quotes/estimates sent to customers showing pricing before converting to invoices. Includes expiry dates, terms, line items, and quote status. Useful for sales pipeline tracking and conversion analysis.

**Key Columns:**
- `quote_id` - Unique identifier for the quote
- `quote_number` - Quote number
- `contact` - JSON object with contact details
- `date` - Quote date
- `expiry_date` - Quote expiration date
- `status` - Quote status (DRAFT, SENT, ACCEPTED, DECLINED, INVOICED, DELETED)
- `total` - Total quoted amount including tax
- `sub_total` - Amount excluding tax
- `total_tax` - Tax amount
- `total_discount` - Total discount amount
- `line_items` - JSON array of line item details
- `title` - Quote title
- `summary` - Quote summary/description
- `terms` - Terms and conditions
- `reference` - Quote reference
- `currency_code` - Transaction currency
- `currency_rate` - Exchange rate if multi-currency
- `branding_theme_id` - Applied branding theme
- `updated_date_utc` - Last modification timestamp

**Use Cases:** Sales pipeline, quote tracking, conversion analysis, proposal management, win/loss reporting

---

### repeating_invoices

**Description:** Templates for recurring invoices that automatically generate on a schedule. Contains schedule configuration (frequency, start/end dates), line items, and approval settings. Important for managing subscription revenue and recurring billing.

**Key Columns:**
- `repeating_invoice_id` - Unique identifier for the template
- `id` - Alternative identifier
- `type` - Invoice type (ACCPAY, ACCREC)
- `status` - Template status (DRAFT, AUTHORISED, DELETED)
- `contact_id` - Associated contact identifier
- `contact_name` - Contact name
- `schedule_unit` - Schedule frequency unit (WEEKLY, MONTHLY, etc.)
- `schedule_period` - Number of units between invoices
- `schedule_due_date` - Days after invoice date payment is due
- `schedule_due_date_type` - Due date calculation method
- `schedule_start_date` - Date to start generating invoices
- `schedule_next_scheduled_date` - Next invoice generation date
- `schedule_end_date` - Date to stop generating invoices
- `line_items` - JSON array of line item details
- `currency_code` - Transaction currency
- `total` - Total invoice amount including tax
- `sub_total` - Amount excluding tax
- `total_tax` - Tax amount
- `reference` - Invoice reference
- `branding_theme_id` - Applied branding theme
- `approved_for_sending` - Whether template is approved

**Use Cases:** Subscription billing, recurring revenue, automated invoicing, SaaS revenue management

---

## Report Tables (Financial Reports)

All report tables share a common structure with metadata columns and dynamic period columns (`period_1`, `period_2`, etc.) that contain financial values for different time periods. The `row_type` field distinguishes between section headers, subsection headers, and data rows.

### Common Report Columns

- `report_id` - Unique identifier for the report
- `report_name` - Name of the report
- `report_title` - Display title of the report
- `report_type` - Report type identifier
- `report_date` - Date the report was generated
- `updated_date_utc` - Last update timestamp
- `section` - Major section (e.g., Assets, Liabilities, Revenue, Expenses)
- `subsection` - Subsection within a section
- `depth` - Hierarchical depth level for nested accounts
- `row_type` - Type of row (Header, Section, SummaryRow, Row)
- `row_title` - Display title for the row
- `account_id` - Account identifier (for detail rows)
- `period_1`, `period_2`, ... `period_N` - Financial values for each period

---

### balance_sheet_report

**Description:** Balance sheet (statement of financial position) showing assets, liabilities, and equity at specific points in time. Organized into sections (Assets, Liabilities, Equity) with hierarchical account groupings. Essential for understanding financial position and net worth. Supports multi-period comparison.

**Report Sections:**
- **Assets** - Current assets (cash, receivables, inventory) and fixed assets (property, equipment)
- **Liabilities** - Current liabilities (payables, short-term debt) and long-term liabilities
- **Equity** - Owner's equity, retained earnings, current year earnings

**Key Use Cases:**
- Financial position analysis
- Net worth calculation
- Liquidity assessment
- Asset vs. liability comparison
- Period-over-period balance changes
- Working capital analysis

**Query Example:**
```sql
SELECT * FROM xero.balance_sheet_report
WHERE report_date = '2024-12-31'
```

---

### profit_loss_report

**Description:** Profit and loss statement (income statement) showing revenue, expenses, and net profit over time periods. Organized into Income and Expense sections with hierarchical groupings. Core report for understanding profitability, margins, and operational performance. Supports multi-period comparison for trend analysis.

**Report Sections:**
- **Revenue/Income** - Sales revenue, service income, other income
- **Cost of Sales** - Direct costs of producing goods/services
- **Expenses** - Operating expenses (wages, rent, utilities, marketing, etc.)
- **Net Profit** - Bottom line profitability

**Key Use Cases:**
- Profitability analysis
- Revenue trend analysis
- Expense management
- Margin calculation
- Period-over-period performance comparison
- Budget variance analysis

**Query Example:**
```sql
SELECT * FROM xero.profit_loss_report
WHERE report_date BETWEEN '2024-01-01' AND '2024-12-31'
```

---

### trial_balance_report

**Description:** Trial balance listing all accounts with their debit and credit balances at a point in time. Shows both balance sheet and profit & loss accounts in a single view, verifying that total debits equal total credits. Essential for month-end close processes and account reconciliation.

**Report Sections:**
- Lists all active accounts with their current balances
- Separate debit and credit columns
- Typically includes both balance sheet and P&L accounts

**Key Use Cases:**
- Month-end close verification
- Account reconciliation
- Identifying out-of-balance conditions
- Audit preparation
- General ledger verification
- Accounting period validation

**Query Example:**
```sql
SELECT * FROM xero.trial_balance_report
WHERE report_date = '2024-12-31'
```

---

### bank_summary_report

**Description:** Summary of all bank account balances and transactions over time. Shows opening balances, money in/out movements, and closing balances for each bank account. Critical for cash management, cash flow analysis, and bank reconciliation oversight.

**Report Sections:**
- One section per bank account
- Opening balance for the period
- Total money in (receipts)
- Total money out (payments)
- Closing balance

**Key Use Cases:**
- Cash flow monitoring
- Bank account reconciliation
- Liquidity management
- Cash position analysis
- Multi-account cash overview
- Cash movement tracking

**Query Example:**
```sql
SELECT * FROM xero.bank_summary_report
WHERE report_date BETWEEN '2024-01-01' AND '2024-12-31'
```

---

### budget_summary_report

**Description:** Comparison of actual financial results against budgeted amounts by account and time period. Shows budget vs. actual variances to identify areas over/under budget. Essential for performance management and financial control.

**Report Sections:**
- Organized by account or account group
- Budget amount (planned)
- Actual amount (realized)
- Variance amount (actual - budget)
- Variance percentage

**Key Use Cases:**
- Budget vs. actual analysis
- Variance investigation
- Performance management
- Financial control
- Identifying budget overruns
- Forecast accuracy assessment

**Query Example:**
```sql
SELECT * FROM xero.budget_summary_report
WHERE report_date = '2024-12-31'
```

---

### executive_summary_report

**Description:** High-level financial overview combining key metrics from balance sheet and profit & loss. Includes cash summary, revenue/expense totals, accounts receivable/payable aging, and profitability ratios. Designed for executive-level visibility into overall financial health.

**Report Sections:**
- **Cash** - Cash position and movement
- **Revenue** - Total revenue for the period
- **Expenses** - Total expenses for the period
- **Accounts Receivable** - Outstanding invoices aging
- **Accounts Payable** - Outstanding bills aging
- **Net Profit** - Bottom line profitability

**Key Use Cases:**
- Executive dashboard
- Financial health overview
- KPI monitoring
- Board reporting
- Quick financial snapshot
- Multi-metric analysis

**Query Example:**
```sql
SELECT * FROM xero.executive_summary_report
WHERE report_date = '2024-12-31'
```

---

## Usage Notes

### Data Tables vs. Report Tables

- **Data Tables** are best for:
  - Transaction-level analysis and detailed queries
  - Building custom reports and dashboards
  - Filtering by specific criteria (dates, contacts, statuses)
  - Accessing granular data like line items and allocations
  - Joining multiple tables for comprehensive analysis

- **Report Tables** are best for:
  - Standard financial reporting
  - Period-over-period comparisons
  - Executive dashboards and summaries
  - Pre-aggregated financial data
  - Quick insights without complex joins

### Important Considerations

1. **Nested Data**: Fields like `line_items`, `addresses`, `phones`, and `allocations` are returned as JSON strings and may require parsing for detailed analysis.

2. **Currency**: All monetary amounts respect the organization's base currency unless a different `currency_code` is specified. Multi-currency transactions include `currency_rate` for conversion.

3. **Filtering**: Most tables support filtering by:
   - Date ranges (`date`, `updated_date_utc`)
   - Contact IDs (`contact_id`)
   - Statuses (`status`)
   - Document types (`type`)
   - Other table-specific criteria

4. **Pagination**: The connector automatically handles pagination for large datasets up to 1000 records per page.

5. **Report Periods**: Report tables generate dynamic `period_N` columns based on the date range requested. The number of periods varies by report type and date range.

6. **Read-Only Access**: All tables are read-only. Insert, update, and delete operations are not supported.

7. **Timestamps**: All timestamps are in UTC format. Use appropriate timezone conversion for local time analysis.

8. **Reconciliation**: Fields like `is_reconciled` indicate whether transactions have been matched with bank statements.

---

## Example Queries

### Get all unpaid invoices
```sql
SELECT invoice_id, invoice_number, contact_name, date, due_date, amount_due
FROM xero.invoices
WHERE status = 'AUTHORISED' AND amount_due > 0
ORDER BY due_date
```

### Analyze monthly revenue
```sql
SELECT contact_name, SUM(total) as revenue, COUNT(*) as invoice_count
FROM xero.invoices
WHERE type = 'ACCREC'
  AND status IN ('AUTHORISED', 'PAID')
  AND date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY contact_name
ORDER BY revenue DESC
```

### Get current balance sheet
```sql
SELECT section, row_title, period_1 as current_balance
FROM xero.balance_sheet_report
WHERE report_date = '2024-12-31'
  AND row_type = 'Row'
ORDER BY section, row_title
```

### Track payment reconciliation status
```sql
SELECT p.payment_id, p.date, p.amount, p.is_reconciled,
       i.invoice_number, c.name as customer_name
FROM xero.payments p
JOIN xero.invoices i ON p.invoice_id = i.invoice_id
JOIN xero.contacts c ON i.contact_id = c.contact_id
WHERE p.is_reconciled = false
ORDER BY p.date DESC
```

---

## Additional Resources

- [Xero Developer Documentation](https://developer.xero.com/documentation/api/accounting/overview)
- [Xero API Accounting Endpoints](https://developer.xero.com/documentation/api/accounting/endpoints)
- [MindsDB Xero Handler](https://github.com/mindsdb/mindsdb/tree/main/mindsdb/integrations/handlers/xero_handler)
