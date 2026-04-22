# Additional Public Source Research Audit

Last updated: 2026-04-19

## Purpose

This audit identifies high-value public data sources that are not already core to the current 51-source plan and that materially improve data-center siting diligence in Texas.

The goal is not to collect data for its own sake. The goal is to close decision gaps that still exist after parcel, utility, flood, broadband, and environmental baseline ingestion.

## Recommended Additions

| Priority | Source | Owner | Official URL | Why it matters for data-center siting | Gap closed beyond current stack | Integration mode |
|---|---|---|---|---|---|---|
| `now` | National Wetlands Inventory (NWI) Wetlands Data Layer | U.S. Fish & Wildlife Service | [Wetlands Data](https://www.fws.gov/program/national-wetlands-inventory/wetlands-data) | Screens wetlands and likely permitting friction before legal diligence starts. | Current stack has flood and habitat signals, but not a dedicated wetlands exclusion layer. | Download, WMS, ArcGIS/web services |
| `now` | SSURGO / gSSURGO soils | USDA NRCS | [SSURGO](https://www.nrcs.usda.gov/resources/data-and-reports/soil-survey-geographic-database-ssurgo) and [gSSURGO](https://www.nrcs.usda.gov/resources/data-and-reports/gridded-soil-survey-geographic-gssurgo-database) | Adds soil bearing, hydric soil, flooding frequency, shrink-swell, and engineering limitations. | Current parcel scoring does not yet capture geotechnical suitability. | Download, Soil Data Access, raster package |
| `now` | 3D Elevation Program (3DEP) | USGS | [3DEP](https://www.usgs.gov/3d-elevation-program) | Improves grading, drainage, cut/fill, access-road feasibility, and micro-topography review. | Current stack uses broad flood and utility proximity, not high-resolution terrain. | DEM download, tiles, The National Map services |
| `now` | NOAA Atlas 14 / PFDS | NOAA NWS HDSC | [PFDS](https://hdsc.nws.noaa.gov/hdsc/pfds/pfds_map_cont.html) | Gives design rainfall depth and intensity needed for stormwater and site-hardening assumptions. | FEMA flood zones do not replace rainfall-intensity design inputs. | Point lookup, GIS grids, scripted pulls |
| `now` | Storm Events Database | NOAA NCEI | [Storm Events Database](https://www.ncei.noaa.gov/stormevents/) | Provides county and local recurrence history for hail, tornado, wind, flood, freeze, and severe storm events. | Current hazard view is mostly static risk; this adds historical operational stress. | Search, bulk CSV download |
| `now` | National Levee Database | USACE | [National Levee Database](https://levees.sec.usace.army.mil/) | Important for Houston, Corpus, Brownsville, and other protected-but-still-exposed corridors. | FEMA flood zones alone miss residual levee risk and behind-levee exposure context. | Web app, data services, manual export |
| `now` | TxDOT traffic counts / STARS II | Texas Department of Transportation | [Traffic count maps](https://www.txdot.gov/data-maps/traffic-count-maps.html) | Improves heavy-haul logistics, interchange screening, and road congestion analysis. | OSM highways show corridor presence, not actual traffic performance. | Web map, GIS download, public site |
| `next` | 3D Hydrography Program (3DHP) | USGS | [3DHP](https://www.usgs.gov/3dhp) | Adds better stream networks, watersheds, and drainage context for hydraulic review. | Current water inputs focus on availability, not full hydrologic structure. | Data service, download |
| `next` | EJScreen | U.S. EPA | [EJScreen mapper](https://ejscreen.epa.gov/mapper/) | Useful for client governance, community-sensitivity review, and outreach planning. | Current stack has environmental constraints but not a combined EJ vulnerability screen. | Web map, downloadable layers |
| `next` | AirToxScreen | U.S. EPA | [AirToxScreen Mapping Tool](https://www.epa.gov/AirToxScreen/airtoxscreen-mapping-tool) | Adds local air-toxics burden and industrial emissions exposure context around candidate sites. | EPA Superfund alone is too narrow for industrial adjacency screening. | Web map, downloadable data |
| `next` | ECHO (Enforcement and Compliance History Online) | U.S. EPA | [ECHO](https://echo.epa.gov/) | Surfaces nearby regulated facilities, wastewater dischargers, enforcement history, and permit context. | Current stack lacks a broad nearby-facility compliance layer. | API, bulk export, web search |
| `next` | eGRID | U.S. EPA | [eGRID](https://www.epa.gov/energy/emissions-generation-resource-integrated-database-egrid) | Adds grid carbon intensity and subregion emissions for ESG-sensitive clients. | Current power scoring looks at access and generation, not delivered-grid emissions profile. | Download, GIS, summary tables |
| `next` | Digital Obstacle File (DOF) | FAA | [DOF](https://www.faa.gov/air_traffic/flight_info/aeronav/digital_products/dof/) | Flags aviation and height-related conflicts near airports and approach surfaces. | Current siting stack does not yet screen vertical obstruction risk. | Scheduled DAT download |
| `next` | Higher Education R&D Survey (HERD) | NSF NCSES | [HERD Survey](https://ncses.nsf.gov/explore-data/microdata/higher-education-research-development) | Gives a stronger talent-quality proxy than simple university distance alone. | Current talent scoring is mostly proximity-based. | CSV/microdata download |
| `later` | Sea Level Rise Viewer | NOAA Digital Coast | [Sea Level Rise Viewer](https://coast.noaa.gov/digitalcoast/tools/slr.html) | Adds future coastal inundation scenarios for Gulf-facing opportunities. | Useful for Houston and Corpus diligence, but not statewide priority one. | Viewer, raster/vector downloads |

## Best Immediate Additions

If the team has limited time, the most practical next additions are:

1. `NWI`
2. `SSURGO / gSSURGO`
3. `3DEP`
4. `NOAA Atlas 14 PFDS`
5. `Storm Events Database`
6. `TxDOT traffic counts`
7. `National Levee Database`

These seven sources improve exclusion screening, civil feasibility, and operating-risk review without changing the current business logic model too aggressively.

## Why These Matter More Than More Generic Data

- They reduce false positives in the current top-50 shortlist.
- They improve diligence quality before broker and legal time is spent.
- They help explain *why* a site is strong or weak in language that high-end clients will understand.
- They are mostly public, stable, and aligned with repeatable engineering review.

## Suggested Integration Order

1. Add `NWI` and `SSURGO` to the parcel-level exclusion and site-feasibility pass.
2. Add `3DEP` and `Atlas 14` to the engineering-risk layer.
3. Add `Storm Events` and `National Levee Database` to resilience scoring.
4. Add `TxDOT`, `DOF`, and `HERD` to logistics and talent refinement.
5. Add `EJScreen`, `AirToxScreen`, `ECHO`, and `eGRID` as client-facing governance overlays.

## Recommendation

The smartest move is not to ingest every possible public source at once. It is to ingest the sources that most improve parcel validation for the already-ranked live candidates.

That means the next best build tranche is:

- parcel linkage for the top cohort
- `NWI`
- `SSURGO`
- `3DEP`
- `Atlas 14`
- `Storm Events`
- `TxDOT traffic`

That package will make the current top-50 board meaningfully more defensible with minimal wasted effort.
