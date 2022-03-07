# blaze-retail-api
A light python wrapper for interacting with BLAZE retail API -- https://apidocs.blaze.me/blaze-developer-api/referencea. A WIP starting with GET requests.
NOT published to Pypi or any other repo as this is still a WIP. Happy to have any help or recommendations for this project. 

This wrapper is aimed to provide cleaned up versions of response data from BLAZE retail API formatted as pandas dataframes. I may add functionality to simply return the JSON objects retrieved, but my goal here is to clean up the nested JSON objects for easier use. Feel free to submit pull requests etc if you want to help with this project. It may be better too in the future to add generator functionality for ease of use, but that's probably a ways down the road. Just thought I'd share this in case others are looking for something more user friendly for pulling BLAZE API data. 

The blaze_retail_api object initializes with default values for partner_key and Authorization keys. See: https://apidocs.blaze.me/blaze-developer-api/guides/Authentication Those default values are your environment vars. Set env vars as: blz_partner_key = partner_key and blz_api_key=your BLAZE public API key generated from their platform.

# How to use:
```
import blaze_retail_api

b = blaze_retail_api(partner_key=<BLAZE RETAIL partner_key>, Authorization=<BLAZE RETAIL user key>)
b.get_products()

```
This returns all products under the current context of your credentials.
