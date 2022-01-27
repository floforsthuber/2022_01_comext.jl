# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script with functions to import and transform raw data
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics

dir_io = "C:/Users/u0148308/data/comext/" # location of input/output (io)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Download raw data from Comext Bulk Download Facility
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

url = "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=comext%2FCOMEXT_DATA%2FPRODUCTS%2F"

years = string.(2015:2021)
months = lpad.(1:12, 2, '0')

for i in years
    for j in months
        
        id = "full" * i * j * ".7z"

        if !isfile(dir_io * "raw/zipped/" * id)
            download(url * id, dir_io * "raw/zipped/" * id)
        else
            println(" ✓ The zipped file for $j/$i has already been downloaded.")
        end

    end
end


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Unzip raw data
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# trying to automate un-zipping, unfortunately failed => NEED TO MANUALLY UNZIPP to folder dir_io!
# run(`cmd /c set PATH=%PATH% ';' "C:\\Program Files\\7-Zip\\" echo %PATH% 7z`)
# run(`cmd /c cd "C:\\Users\\u0148308\\data\\raw\\"`)
# run(`cmd /c 7z e a.7z`)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Import data into Julia and do initial cleaning
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# initialize DataFrame
# Notes:
#   - column names and types need to correspond to the final output of the "inital_cleaning" function
df = DataFrame(DECLARANT_ISO=String[], PARTNER_ISO=String[], TRADE_TYPE=String[], PRODUCT_NC=String[], FLOW=String[], PERIOD=Int64[],
                VALUE_IN_EUROS=Union{Missing, Float64}[], QUANTITY_IN_KG=Union{Missing, Float64}[])

# initial cleaning of the data               
function initial_cleaning(path::String)
    
    df = CSV.read(path, DataFrame)

    # correct column types
    # Notes:
    #   - treat PERIOD as numeric to allow sorting
    transform!(df, [:DECLARANT, :PARTNER, :PERIOD] .=> ByRow(Int64), renamecols=false)
    transform!(df, [:DECLARANT_ISO, :PARTNER_ISO, :TRADE_TYPE, :PRODUCT_NC, :PRODUCT_SITC, :PRODUCT_CPA2002, :PRODUCT_CPA2008, :PRODUCT_CPA2_1,
                    :PRODUCT_BEC, :PRODUCT_BEC5, :PRODUCT_SECTION, :FLOW, :STAT_REGIME] .=> ByRow(string), renamecols=false)
    transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG, :SUP_QUANTITY] .=> ByRow(Float64), renamecols=false)

    # rename some indicators
    df[:, :TRADE_TYPE] .= ifelse.(df[:, :TRADE_TYPE] .== "I", "intra", "extra")
    df[:, :FLOW] .= ifelse.(df[:, :FLOW] .== "1", "imports", "exports")
    df[:, :SUPP_UNIT] .= ifelse.(ismissing.(df[:, :SUPP_UNIT]), missing, string.(df[:, :SUPP_UNIT])) # to make sure type is of Union{Missing, String}

    # missing data
    # Notes:
    #   - if value or quanity is reported as 0 use missing instead
    transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG, :SUP_QUANTITY] .=> ByRow(x->ifelse(x == 0.0, missing, x)), renamecols=false)

    # aggregate over STAT_REGIME, SUPP_UNIT
    # Notes:
    #   - only lose few rows ~1%
    #   - disregard SUPP_UNIT and SUP_QUANTITY for the time being
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "TRADE_TYPE", "PRODUCT_NC", "FLOW", "PERIOD"]
    gdf = groupby(df, cols_grouping)
    df = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> sum, renamecols=false)

    return df
end

# # -----------

for i in years
    for j in months
        path = dir_raw * "comext/" * "full" * i * j * ".dat"
        append!(df, initial_cleaning(path))
        println(" ✓ The data for $j/$i has been successfully added.")
    end
end

# -----------

# for now just use 3 months otherwise its rather slow since ~30GB of data
path = dir_io * "raw/" * "full" * "2020" * "01" * ".dat"
append!(df, initial_cleaning(path))
path = dir_io * "raw/" * "full" * "2020" * "02" * ".dat"
append!(df, initial_cleaning(path))
path = dir_io * "raw/" * "full" * "2020" * "03" * ".dat"
append!(df, initial_cleaning(path))

sort!(df) # not necessary for computation

# -----------

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Unit prices
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# compute UNIT_PRICE
transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)

# compute UNIT_PRICE_CHANGE (in percentages?)
function pct_change(input::AbstractVector)
    [i == 1 ? missing : (input[i]-input[i-1])/input[i-1]*100 for i in eachindex(input)]
end

# Notes:
#   - not every product imported/exported every period (depends on grouping)
#       + with this function it means that we calulate price changes between periods when products are bought, i.e.
#         price change between Feb2019 and April2019, but could also be Feb2019 and Dec2020!
#       + possible solutions:
#           - higher level of aggregation, for example sum over all sources (or intra/extra EU)
#           - only compute price change for successive months (would lose a lot of observations)
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "PRODUCT_NC", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, :UNIT_PRICE => pct_change => :UNIT_PRICE_CHANGE)

# export data
df_export = ifelse.(ismissing.(df), NaN, df)[1:30_000,:] # excel cannot deal with so many rows
XLSX.writetable(dir_io * "clean/" * "df_prices.xlsx", df_export, overwrite=true)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Share of Belgium
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# Notes:
#   - how to treat missing values? skipmissing for now
#   - what constitutes as total?
#       + for DECLARANT_ISO the total are all EU ctrys
#           -  change in composition, most importantly GB exiting will increase shares
#       + for PARTNER_ISO the total is the entire world
#       + the total will thus be the BE share of EU imports/exports to the entire world (intra + extra)

# compute total (EU imports/exports to the entre world)
cols_grouping = ["PRODUCT_NC", "FLOW", "PERIOD"]
gdf = groupby(df, cols_grouping)
df_sum_total = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> (x -> sum(skipmissing(x))), renamecols=false)

# compute Belgium (BE imports/exports to the entire world)
df_BE = subset(df, :DECLARANT_ISO => ByRow(x -> x == "BE"))
gdf = groupby(df_BE, cols_grouping)
df_sum_BE = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> (x -> sum(skipmissing(x))), renamecols=false)

# join, rename and compute shares in percentages (?)
df_join = leftjoin(df_sum_total, df_sum_BE, on=[:PRODUCT_NC, :FLOW, :PERIOD], makeunique=true)
rename!(df_join, ["PRODUCT_NC", "FLOW", "PERIOD", "VALUE_TOTAL", "QUANITY_TOTAL", "VALUE_BE", "QUANITY_BE"])
df_join.VALUE_SHARE = df_join.VALUE_BE ./ df_join.VALUE_TOTAL .* 100
df_join.QUANTITY_SHARE = df_join.QUANITY_BE ./ df_join.QUANITY_TOTAL .* 100

# compute change in shares
cols_grouping = ["PRODUCT_NC", "FLOW"]
gdf = groupby(df_join, cols_grouping)
df_share_BE = transform(gdf, [:VALUE_SHARE, :QUANTITY_SHARE] .=> pct_change .=> [:VALUE_SHARE_CHANGE, :QUANTITY_SHARE_CHANGE])

# export data
sort!(df_share_BE, [:PRODUCT_NC, :FLOW, :PERIOD])
df_export = ifelse.(ismissing.(df_share_BE), NaN, df_share_BE) # excel cannot deal with missing (but still an issue opening the file)
XLSX.writetable(dir_io * "clean/" * "df_share_BE.xlsx", df_export, overwrite=true)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
