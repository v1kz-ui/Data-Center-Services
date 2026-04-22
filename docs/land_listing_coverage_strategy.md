# Texas Land Listing Coverage Strategy

Goal: maintain an auditable view of active Texas land-for-sale listings within
30 miles of major population hubs.

## What We Can Prove Today

The local database can prove coverage for listings that are already loaded into
`market_listing`, active, geocoded, and classified as sale land by either
`asset_type` or title terms.

Run:

```powershell
.venv\Scripts\python.exe scripts\report_land_listing_hub_coverage.py
```

Default outputs:

- `temp/land_listing_hub_coverage.json`
- `temp/land_listing_hub_coverage.csv`
- `temp/land_listing_hub_matches.csv`

The report defines major population hubs as Texas Census places with 2024 ACS
5-year population of at least 100,000 and uses Census geoinfo internal points for
latitude/longitude:

- Population: https://api.census.gov/data/2024/acs/acs5?get=NAME,B01003_001E&for=place:*&in=state:48
- Geography: https://api.census.gov/data/2024/geoinfo?get=NAME,INTPTLAT,INTPTLON&for=place:*&in=state:48

## What "Every Listing" Requires

No single public marketplace can guarantee every active land listing in Texas.
The defensible standard is source-complete coverage across all authorized feeds
we are allowed to use, plus a gap log for feeds we have not authorized yet.

Current complete public feed path:

- MyEListing sitemap-backed Texas pull, stored under `listing_source_id='myelisting'`.
- AcreValue Texas public land-listing pull, stored under `listing_source_id='acrevalue'`.

Public web endpoints checked but not pulled without authorization:

- Land.com/LandWatch sitemap and listing pages returned HTTP 403 to automated requests.
- LoopNet sitemap pages returned HTTP 403 to automated requests.
- Crexi public pages returned HTTP 403 to automated requests.

Priority authorized additions:

- MLS or broker feeds through RESO Web API access: https://www.reso.org/reso-web-api/
- ListHub publisher or licensed content API access: https://www.listhub.com/api-documentation/
- Crexi via an authorized partnership/API route: https://learn.crexi.com/listing-partnerships-crexi-help-center
- Land.com, broker-owned feeds, land brokerage CSVs, and county surplus/auction feeds where permission or public terms allow reuse.

Compliance note: do not bypass authentication, rate limits, robots controls, or
terms that prohibit automated extraction. Crexi's public terms restrict
unauthorized scraping and automated access: https://www.crexi.com/tos

## Audit Rules

1. Load each authorized source into `market_listing` with source attribution.
2. Normalize sale status and land-like asset classification.
3. Geocode every listing or mark it as a coverage defect.
4. Dedupe across sources by parcel id when present; otherwise use normalized
   title/address/city/postal code, rounded lat/lon, acreage, and price.
5. For each Census population hub, count all listings within 30 miles. Keep
   overlapping hub catchments rather than assigning a listing to only one city.
6. Track hub rows with zero listings, ungeocoded listings, stale sources, and
   missing authorized source families as explicit caveats.

## Client View Readiness

The client-facing board should surface:

- Active sale land listings within 30 miles of major hubs.
- Hub coverage counts and zero-coverage alerts.
- Source coverage counts by provider.
- Last successful refresh and known caveats.
- A plain statement that the board is complete for loaded authorized feeds, not
  a claim that it contains every privately marketed or unauthorized listing.
