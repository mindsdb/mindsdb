# Sharepoint Handler

Sharepoint handler for MindsDB provides interfaces to connect to Sharepoint via graph APIs and pull data into MindsDB.

---

## Table of Contents

- [Sharepoint Handler](#Sharepoint-handler)
  - [Table of Contents](#table-of-contents)
  - [About Sharepoint](#about-sharepoint)
  - [Sharepoint Handler Implementation](#sharepoint-handler-implementation)
  - [Sharepoint Handler Initialization](#sharepoint-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [Some useful definitions](#some-useful-definitions)
  - [TODO](#todo)
  - [Example Usage](#example-usage)

---

## About Sharepoint

SharePoint in Microsoft 365 empowers teamwork with dynamic and productive team sites for every project team, department, and division. Share files, data, news, and resources. Customize your site to streamline your team’s work.

## Sharepoint Handler Implementation

This handler was implemented using the [Microsoft Graph API](https://learn.microsoft.com/en-us/graph/use-the-api) endpoint. 

Graph API is a REST API endpoint that provides a simple and easy-to-use interface to access many microsoft tools including Sharepoint.

## Sharepoint Handler Initialization

The Sharepoint handler is initialized with the following parameters:

- `clientId`: (required) Microsoft App client ID
- `clientSecret`: (required) client secret associated with the App
- `tenantId`: (required) GUID of the tenant in which the App has been created

## How to get your credentials.

1. Visit Microsoft Entra admin center and register a new App
2. Go to API permissions and grant all the permissions related to sharepoint sites and resources
3. Go to Certificates & secrets tab of the app and create a new client secret
4. Now go to the App overview page where you will find client-ID and tenant-ID of your App.

## Implemented Features

- Fetch sites associated with the account and ability to update the metadata associated with a site (deletion and creation of sites has not been implemented)
- Fetch lists associated with the account and ability to create more lists, update fields associated with lists and deletion of lists
- Fetch site columns associated with the account and ability to create more site columns, update fields associated with site columns and deletion of site columns
- Fetch list items associated with the account and ability to create more list items, update fields associated with list items and deletion of list items


## Some useful definitions

### Sites:
SharePoint-sites are essentially containers for information. The way you store and organize things in SharePoint is by Sites.

### Lists:
A list is a collection of data that you can share with your team members and people who you've provided access to. You'll find a number of ready-to-use list templates to provide a good starting point for organizing list items.

https://support.microsoft.com/en-us/office/introduction-to-lists-0a1c3ace-def0-44af-b225-cfa8d92c52d7

### Site columns
A Site Column is a template of a configured column. By creating a Site Column, you can reuse it anywhere else in the site and not have to manually rebuild its configuration at each reuse.

When creating a new column in a list or library, you have a choice to either "Create column" or "Add from existing site columns". Selecting the latter will add a replica of the Site Column to the location you are working.

https://learn.microsoft.com/en-us/microsoft-365/community/what-is-site-column

### List items
A SharePoint list can be considered as a collection of items. The list items can be a variety of things, such as contacts, calendars, announcements, and issues-tracking.

https://support.microsoft.com/en-us/office/introduction-to-lists-0a1c3ace-def0-44af-b225-cfa8d92c52d7

### Difference between site column and list columns
The main difference between site column and list columns is the scope of use.
That is a list column will only be available to that particular list/library, and not outside that boundary. 
If you wish to use that column outside that list/library, you will have to recreate it at the new location.

Site columns on the other hand, are created at the site level, and available to reuse from the site they're created in (as the starting point).

https://learn.microsoft.com/en-us/microsoft-365/community/list-column-or-site-column-which-one-to-choose

## TODO

- Update and delete a site which is functionality that is not yet supported by Graph API
- Add other tables like list columns and other components that are part of a sharepoint site
- Replace the REST calls with an SDK. Currently, the Microsoft Graph SDK is under development/preview mode. In the future, we should replace the REST calls with library's methods. 

## Example Usage
```
CREATE DATABASE sharepoint_test
With 
    ENGINE = 'sharepoint',
    PARAMETERS = {
     "clientId":"YOUR_CLIENT_ID",
     "clientSecret":"YOUR_CLIENT_SECRET",
     "tenantId":"YOUR_TENANT_ID"
    };
```

After setting up the Sharepoint Handler, you can use SQL queries to fetch data from Sharepoint
and perform CRUD operations on it:

Example shows how to fetch all the lists associated with the account:
```sql
SELECT *
FROM sharepoint_test.lists
```
