# Live Candidate Grading Method

Last updated: 2026-04-19

## Purpose

This grading pass is designed to rank live Texas market listings for data-center siting before full parcel-level factor evidence is available statewide.

It is intentionally candidate-first:

- Start from actively marketed Texas listings with coordinates.
- Keep the shortlist focused on major Texas population and university markets.
- Use public infrastructure and risk signals already loaded into the platform.
- Preserve the seeded dashboard catalogue only as a fallback for empty or cold-start environments.

The current acreage policy is now right-sized for compact deployments:

- Sites as small as `1` acre are fully viable.
- `1` to `3` acres is treated as the preferred footprint range.
- Sites above `10` acres do not receive additional scale credit and begin to taper down materially in the fit score.
- Very large tracts can still score well on infrastructure, but they no longer float to the top just because they are large.

## Scope Gate

The live shortlist includes only listings that satisfy all of the following:

- `market_listing.is_active = true`
- Texas listings with latitude and longitude
- Asset types suitable for data-center screening, led by:
  - `commercial land`
  - `industrial properties`
  - `flex-office-warehouse`
  - selected `specialty real estate`
- Within 30 miles of a major metro population anchor or university anchor

The current featured shortlist is intentionally limited to these anchor metros:

- Dallas-Fort Worth
- Houston
- Austin
- San Antonio
- Brazos Valley
- El Paso
- Rio Grande Valley

Smaller support markets remain searchable in raw inventory but are not allowed to dominate the top-50 featured board.

## Data Inputs Used Now

The current live scoring run uses:

- `market_listing`
- `IF-001` HIFLD substations
- `IF-007` FEMA National Risk Index
- `IF-009` PeeringDB facilities
- `IF-021` USGS water monitoring sites
- `IF-023` EPA Superfund sites
- `IF-025` EIA power plants
- `IF-026` OSM highway corridors

## Factor Model

The current model keeps the agreed 10-factor structure and 5 bonuses.

### Base Factors

1. `F01 Power access` — 20 points  
   Nearest substation proximity, substation voltage, and nearby generation capacity.

2. `F02 Fiber/connectivity proxy` — 12 points  
   Nearest PeeringDB facility distance plus carrier/network density.

3. `F03 Flood and hazard risk` — 12 points  
   FEMA NRI county-level risk proxy using anchor county expected annual loss and resilience.

4. `F04 Site-size fit / deployment fit` — 10 points  
   Right-sizes acreage and, when available, building square footage to favor practical first-wave data-center footprints, with the strongest acreage fit in the `1` to `3` acre range.

5. `F05 Land-use fit` — 10 points  
   Asset type suitability and title-keyword screening for industrial/data-center compatibility.

6. `F06 Water availability proxy` — 8 points  
   Distance to USGS water monitoring sites plus municipal-access proxy.

7. `F07 Environmental constraint proxy` — 8 points  
   Penalty if listing city overlaps current EPA Superfund city inventory.

8. `F08 Metro population access` — 8 points  
   Distance to the nearest major population or university anchor inside the target metro, with closer sites scoring higher.

9. `F09 Highway / logistics access` — 6 points  
   Distance to major highway corridor points from OSM.

10. `F10 Market economics` — 6 points  
   Price-per-acre versus metro median, blended with market-size weight.

### Bonuses

- `B01` Actively marketed site bonus
- `B02` Sweet-spot footprint / building bonus
- `B03` Power plus fiber adjacency bonus
- `B04` City plus university proximity bonus
- `B05` Pricing transparency / economics bonus

## Ranking Controls

To keep the client-facing board usable, the shortlist applies:

- A major-market allowlist
- A 30-mile metro-access rule using the nearest city or university anchor
- A maximum of 10 featured opportunities per metro

This prevents a single high-volume market from flooding the board even if it has many similar low-cost listings.

It also prevents acreage-heavy outliers from dominating the board when a smaller, better-located, infrastructure-ready site is the more realistic near-term deployment candidate.

## Confidence Model

Current live opportunities carry a `confidence_score` of `82`.

That score reflects the present evidence mix:

- Measured inputs: listing fields, substations, peering facilities, water sites, highways, power plants
- Proxy inputs: anchor-county hazard, market size, university adjacency
- Heuristic inputs: asset-type and title-language fit

This is strong enough for shortlist generation, but it is not the same as parcel-perfect diligence.

## Current Output

The latest live run writes:

- [live_candidate_shortlist.json](C:/Dev/Anti-Gravity/Dense%20Data%20Center%20Locator/temp/live_candidate_shortlist.json)

The dashboard now uses live candidate scoring automatically when data is available and falls back to the seeded catalogue otherwise.

## Known Limitations

- Factor evidence is not yet parcel-linked statewide.
- MyEListing is configured as separate sale and lease feeds, but the shortlist still depends on the latest successful refresh of each feed.
- Zoning is not yet driving this live shortlist directly unless a listing is later parcel-linked.
- Environmental screening is still a coarse proxy rather than exact polygon intersection.
- Parcel linkage for the top cohort is still the next diligence step.

## Next Build Step

The next correct step is not more blind ranking. It is top-cohort parcel linkage and exclusion review:

- Link the top 100 to canonical parcels where possible.
- Run exact zoning, acreage, flood, and environmental checks.
- Promote only validated candidates into the final diligence list.
