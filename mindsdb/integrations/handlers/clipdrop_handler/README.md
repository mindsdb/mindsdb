# Clipdrop Handler

Clipdrop handler for MindsDB provides interfaces to connect to Clipdrop via APIs.

---

## Table of Contents

- [Clipdrop Handler](#clipdrop-handler)
  - [Table of Contents](#table-of-contents)
  - [About Clipdrop](#about-clipdrop)
  - [Clipdrop Handler Implementation](#clipdrop-handler-implementation)
  - [Clipdrop Handler Initialization](#clipdrop-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [ToDO](#todo)
  - [Example Usage](#example-usage)

---

## About Clipdrop

The Clipdrop API allows you to integrate best-in-class AI to your apps in minutes.

## Clipdrop Handler Implementation

This handler was implemented using the `requests` library that makes http calls to https://clipdrop.co/apis/docs/image-upscaling

## Clipdrop Handler Initialization

The Clipdrop handler is initialized with the following parameters:

- `api_key`: API key to connect to clipdrop
- `dir_to_save`: Local Directory to save the output images from the API.

Read about creating an account [here](https://clipdrop.co/).

## Implemented Features

- [x] Remove Text


## ToDO

- [x] Image inpainting

## Example Usage

The first step is to create a database with the new `clipdrop` engine. 

~~~~sql
CREATE DATABASE mindsdb_clipdrop
WITH ENGINE = 'clipdrop',
PARAMETERS = {
  "api_key": "api_key",
  "dir_to_save": "/Users/Sam/Documents/test/"
};
~~~~

Use the established connection to query your database:

~~~~sql
SELECT * FROM mindsdb_clipdrop.remove_text WHERE img_url="https://static.vecteezy.com/system/resources/thumbnails/022/721/714/small/youtube-logo-for-popular-online-media-content-creation-website-and-application-free-png.png";
~~~~

~~~~sql
SELECT * FROM mindsdb_clipdrop.remove_background where img_url="https://upload.wikimedia.org/wikipedia/commons/a/a5/Red_Kitten_01.jpg";
~~~~

~~~~sql
SELECT * FROM mindsdb_clipdrop.sketch_to_image WHERE img_url="https://png.pngtree.com/png-vector/20230531/ourmid/pngtree-simple-kitten-drawing-with-cat-sitting-on-the-white-background-vector-png-image_6785704.png" AND text="A pink fairy tale cat";
~~~~

~~~~sql
SELECT * FROM mindsdb_clipdrop.replace_background WHERE img_url="https://upload.wikimedia.org/wikipedia/commons/thumb/d/d8/Indian_Cricket_Player.jpg/1024px-Indian_Cricket_Player.jpg" and text="A yellow background";
~~~~

~~~~sql
SELECT * FROM mindsdb_clipdrop.reimagine WHERE img_url="https://upload.wikimedia.org/wikipedia/commons/thumb/d/d8/Indian_Cricket_Player.jpg/1024px-Indian_Cricket_Player.jpg";
~~~~

~~~~sql
SELECT * FROM mindsdb_clipdrop.text_to_image WHERE text="A pink fairy tale cat";
~~~~
