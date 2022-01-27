# 2022_01_comext

[![Build Status](https://github.com/floforsthuber/2022_01_comext.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/floforsthuber/2022_01_comext.jl/actions/workflows/CI.yml?query=branch%3Amain)
[![Coverage](https://codecov.io/gh/floforsthuber/2022_01_comext.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/floforsthuber/2022_01_comext.jl)

# Overview

General instructions for the Eurostat bulkdownload facility can be found [here](https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=comext%2FInstructions+on+how+to+use+the+bulkdownload+facility.pdf)

Correspondance tables for product/country classifications can be found [here](https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&dir=comext%2FCOMEXT_METADATA%2FCLASSIFICATIONS_AND_RELATIONS%2FENGLISH)

Product code updates of CN8 classification can be found [here](https://ec.europa.eu/eurostat/ramon/nomenclatures/index.cfm?TargetUrl=LST_CLS_DLD&StrNom=CN_2021&StrLanguageCode=EN&StrLayoutCode=HIERARCHIC)

## Original columns:

- **DECLARANT:** reporting country, number code
- **DECLARANT_ISO:** reporting country, ISO2 codes
- **PARTNER:** partner country, number code
- **PARTNER_ISO:** reporting country, ISO2 codes

- **TRADE_TYPE:** indicator for intra-EU (I) and extra-EU (E) TRADE_TYPE

- **PRODUCT_XXX:** different product classifications, CN, SITC, CPA_2002, CPA_2008, CPA_2.1, BEC, SECTION are available

- **FLOW:** indicator for import (1) or export (2)
- **STAT_REGIME:** indicates the use of the import/export which requires different customts treatment, therefore, for a given product and country-pair there might exist multiple entries (more pre-2009):
    - *(1)* normal imports/exports
    - *(2)* inward processing: allows to import a good temporarily for processing and re-xporting whilst benefiting from duty exemptions
    - *(3)* outward processing: allows to export a good temporarily for processing and re-importing whislt benefiting from duty exemptions
    - *(9)* not recorded from customs declaration: imports/exports for which customs procedure is not the data source

- **SUPP_UNIT/ SUP_QUANTITY:** for certain goods a supplementary quantity is provided in addition to net mass to provide more useful information
    - *SUPP_UNIT:* indicates supplementary unit (1-6, A-Z), the exact correspondence can be found [here](https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=comext%2FCOMEXT_METADATA%2FCLASSIFICATIONS_AND_RELATIONS%2FENGLISH%2FSU.txt)
    - *SUP_QUANTITY:* indicates quantity in the specified supplementary unit

- **PERIOD:** time indicator in format YYYYMM

- **VALUE_IN_EUROS:** trade value expressed Euros. FOB valuation for exports and CIF valuation for imports.
- **QUANTITY_IN_KG:** weight of goods in kilograms without packaging.

