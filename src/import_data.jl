# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script with functions to import and transform raw data
# -------------------------------------------------------------------------------------------------------------------------------------------------------------


using DataFrames, CSV, XLSX, LinearAlgebra, Statistics


dir_raw = "C:/Users/u0148308/data/raw/" # location of raw data

url = "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=comext%2FCOMEXT_DATA%2FPRODUCTS%2F"

years = string.(2020:2020)
months = lpad.(1:12, 2, '0')

for i in years
    for j in months
        id = "full" * i * j * ".7z"
        download(url * id, dir_raw * "comext/" * id)
    end
end

# trying to automate un-zipping, unfortunately failed

# run(`cmd /c set PATH=%PATH% ';' "C:\\Program Files\\7-Zip\\" echo %PATH% 7z`)
# run(`cmd /c cd "C:\\Users\\u0148308\\data\\raw\\"`)
# run(`cmd /c 7z e a.7z`)

path = dir_raw * "comext/" * "full202001.dat"
df = CSV.read(path, DataFrame)

describe(df)

glimpse = df[rand(1:size(df,1),100),:]
transform!(glimpse, names(glimpse) .=> ByRow(string), renamecols=false)

XLSX.writetable(dir_raw * "comext/" * "glimpse" * ".xlsx", glimpse, overwrite=true)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# General instructions for the Eurostat bulkdownload facility can be found here:
# https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=comext%2FInstructions+on+how+to+use+the+bulkdownload+facility.pdf

# Correspondance tables for product/country classifications can be found here:
# https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&dir=comext%2FCOMEXT_METADATA%2FCLASSIFICATIONS_AND_RELATIONS%2FENGLISH

# Original columns:

# DECLARANT: reporting country, number code
# DECLARANT_ISO: reporting country, ISO2 codes
# PARTNER: partner country, number code
# PARTNER_ISO: reporting country, ISO2 codes

# TRADE_TYPE: indicator for intra-EU (I) and extra-EU (E) TRADE_TYPE

# PRODUCT_***: different product classifications, CN, SITC, CPA_2002, CPA_2008, CPA_2.1, BEC, SECTION are available

# FLOW: indicator for import (1) or export (2)
# STAT_REGIME: indicates the use of the import/export which requires different customts treatment, therefore, for a given product and country-pair 
#              there might exist multiple entries (some more pre 2009):
#              (1) normal imports/exports
#              (2) inward processing: allows to import a good temporarily for processing and re-xporting whilst benefiting from duty exemptions
#              (3) outward processing: allows to export a good temporarily for processing and re-importing whislt benefiting from duty exemptions
#              (9) not recorded from customs declaration: imports/exports for which customs procedure is not the data source

# SUPP_UNIT/ SUP_QUANTITY: for certain goods a supplementary quantity is provided in addition to net mass to provide more useful information
#                          SUPP_UNIT indicates supplementary unit (1-6, A-Z), the exact correspondence can be found here: https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=comext%2FCOMEXT_METADATA%2FCLASSIFICATIONS_AND_RELATIONS%2FENGLISH%2FSU.txt
#                          SUP_QUANTITY indicates quantity in the specified supplementary unit

# PERIOD: time indicator in format YYYYMM

# VALUE_IN_EUROS: trade value expressed Euros. FOB valuation for exports and CIF valuation for imports.
# QUANTITY_IN_KG: weight of goods in kilograms without packaging.

# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# some thoughts:

# which timespan? 
#   2010-now would make reduce STAT_REGIME to only 1,2,3,9
#   how to treat Croatia and UK? simply follow classification in TRADE_TYPE

# which frequency?
#   monthly/annually (with annual data less precise how to treat HR and UK)

# how to deal with STAT_REGIME?
#   issue of double counting? can we extract value added?

# what exactly do we want to compute?