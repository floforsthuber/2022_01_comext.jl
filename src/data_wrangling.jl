# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script with functions to import and transform raw data
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics

dir_io = "C:/Users/u0148308/data/comext/" # location of input/output (io)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Download raw data from Comext Bulk Download Facility
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

url = "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&file=comext%2FCOMEXT_DATA%2FPRODUCTS%2F"

years = string.(2015:2020)
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
function initial_cleaning(year::String, month::String)
    
    path = dir_io * "raw/" * "full" * year * month * ".dat"
    df = CSV.read(path, DataFrame)

    # correct column types
    # Notes:
    #   - treat PERIOD as numeric to allow sorting
    transform!(df, [:DECLARANT, :PARTNER, :PERIOD] .=> ByRow(Int64), renamecols=false)

    # different additional PROD_BEC column from 2017 onwards
    col_strings = [:DECLARANT_ISO, :PARTNER_ISO, :TRADE_TYPE, :PRODUCT_NC, :PRODUCT_SITC, :PRODUCT_CPA2002, :PRODUCT_CPA2008, :PRODUCT_CPA2_1, 
                    :PRODUCT_BEC, :PRODUCT_SECTION, :FLOW, :STAT_REGIME]
    col_strings = ifelse(year in ["2015", "2016"], col_strings, [col_strings; :PRODUCT_BEC5])
    transform!(df,  col_strings .=> ByRow(string), renamecols=false)

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

# -----------

for i in years
    for j in months
        append!(df, initial_cleaning(i, j))
        println(" ✓ The data for $j/$i has been successfully added.")
    end
end

# months = lpad.(1:11, 2, '0')
# for i in ["2021"]
#     for j in months
#         append!(df, initial_cleaning(i, j))
#         println(" ✓ The data for $j/$i has been successfully added.")
#     end
# end

# -----------

# for now just use 3 months otherwise its rather slow since ~30GB of data
# path = dir_io * "raw/" * "full" * "2017" * "01" * ".dat"
# df = CSV.read(path, DataFrame)
# append!(df, initial_cleaning("2015", "01"))
# path = dir_io * "raw/" * "full" * "2020" * "02" * ".dat"
# append!(df, initial_cleaning(path))
# path = dir_io * "raw/" * "full" * "2020" * "03" * ".dat"
# append!(df, initial_cleaning(path))

# possibly need to sort before grouping, seems that grouping sorts incorrectly sometimes?
sort!(df) # do sorting just in case (unexplainable problem below)

# -----------


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Unit prices
# -------------------------------------------------------------------------------------------------------------------------------------------------------------


# compute UNIT_PRICE
transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)


# compute UNIT_PRICE_CHANGE
# Notes:
#   - percentages?
#   - not every product imported/exported every period
#       + calculate price changes between periods when products are bought, i.e. price change between Feb2019 and March2019, 
#         but could also be Feb2019 and Dec2020!
#   - potential solutions
#       + compute change for successive periods (months) only
function pct_change(input::AbstractVector)
    [i == 1 ? missing : (input[i]-input[i-1])/input[i-1]*100 for i in eachindex(input)]
end

cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "PRODUCT_NC", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, :UNIT_PRICE => pct_change => :UNIT_PRICE_CHANGE)


# compute MOM_PRICE_CHANGE
# Notes:
#   - functions subsets UNIT_PRICE_CHANGE to only successive periods (months)
#       + notice the double ifelse statemen in shorthand, find a way to use ifelse instead to make it easier to read
function mom_change(period::AbstractVector, input::AbstractVector)
    [i == 1 ? missing : period[i]-period[i-1] != 1 ? missing : input[i] for i in eachindex(input)]
end

gdf = groupby(df, cols_grouping)
df = transform(gdf, [:PERIOD, :UNIT_PRICE_CHANGE] => mom_change => :MOM)


# export data
df_export = ifelse.(ismissing.(df), NaN, df)[1:30_000,:] # excel cannot deal with so many rows
XLSX.writetable(dir_io * "clean/" * "df_prices.xlsx", df_export, overwrite=true)

CSV.write(dir_io * "clean/" * "df_prices.csv", df)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Share of Belgium
# -------------------------------------------------------------------------------------------------------------------------------------------------------------


# Notes:
#   - how to treat missing values? skipmissing for now
#   - what constitutes as total?
#       + for DECLARANT_ISO the total are all EU ctrys, termed EU
#           -  change in composition, most importantly GB exiting will increase shares
#       + for PARTNER_ISO the total is the entire world, termed WORLD
#       + the total will thus be the BE share of EU imports/exports to the entire world (intra + extra)

# compute EU_WORLD (EU imports/exports to the entire world)
cols_grouping = ["PRODUCT_NC", "FLOW", "PERIOD"]
gdf = groupby(df, cols_grouping)
df_sum_EU_WORLD = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> (x -> sum(skipmissing(x))), renamecols=false)
df_sum_EU_WORLD.TRADE_TYPE .= "total"

# compute EU_EXTRA and EU_INTRA (EU imports/exports to extra/intra EU countries)
cols_grouping = ["TRADE_TYPE", "PRODUCT_NC", "FLOW", "PERIOD"]
gdf = groupby(df, cols_grouping)
df_sum_EU_EXTRA_INTRA = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> (x -> sum(skipmissing(x))), renamecols=false)

# compute BE_WORLD (BE imports/exports to the entire world)
df_BE = subset(df, :DECLARANT_ISO => ByRow(x -> x == "BE"))
cols_grouping = ["PRODUCT_NC", "FLOW", "PERIOD"]
gdf = groupby(df_BE, cols_grouping)
df_sum_BE_WORLD = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> (x -> sum(skipmissing(x))), renamecols=false)
df_sum_BE_WORLD.TRADE_TYPE .= "total"

# compute BE_EXTRA and BE_INTRA (BE imports/exports to extra/intra EU countries)
cols_grouping = ["TRADE_TYPE", "PRODUCT_NC", "FLOW", "PERIOD"]
gdf = groupby(df_BE, cols_grouping)
df_sum_BE_EXTRA_INTRA = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> (x -> sum(skipmissing(x))), renamecols=false)

# merge tables
df_sum_EU = vcat(df_sum_EU_EXTRA_INTRA, df_sum_EU_WORLD)
df_sum_BE = vcat(df_sum_BE_EXTRA_INTRA, df_sum_BE_WORLD)

# join, rename and compute shares in percentages (?)
df_join = leftjoin(df_sum_EU, df_sum_BE, on=[:TRADE_TYPE, :PRODUCT_NC, :FLOW, :PERIOD], makeunique=true)
rename!(df_join, ["TRADE_TYPE", "PRODUCT_NC", "FLOW", "PERIOD", "VALUE_EU", "QUANITY_EU", "VALUE_BE", "QUANITY_BE"])
df_join.VALUE_SHARE = df_join.VALUE_BE ./ df_join.VALUE_EU .* 100
df_join.QUANTITY_SHARE = df_join.QUANITY_BE ./ df_join.QUANITY_EU .* 100

# possibly need to sort before grouping, seems that grouping sorts incorrectly sometimes?
sort!(df_join, [:PRODUCT_NC, :FLOW, :TRADE_TYPE, :PERIOD]) # dont understand why I need the sorting here? otherwise some MOM are not taken?

# compute SHARE_CHANGE
cols_grouping = ["TRADE_TYPE", "PRODUCT_NC", "FLOW"]
gdf = groupby(df_join, cols_grouping)
df_share_BE = transform(gdf, [:VALUE_SHARE, :QUANTITY_SHARE] .=> pct_change .=> [:VALUE_SHARE_CHANGE, :QUANTITY_SHARE_CHANGE])

# compute MOM_CHANGE
gdf = groupby(df_share_BE, cols_grouping)
df_share_BE = transform(gdf, [:PERIOD, :VALUE_SHARE_CHANGE] => mom_change => :MOM_VALUE,
                             [:PERIOD, :QUANTITY_SHARE_CHANGE] => mom_change => :MOM_QUANTITY)


# export data
df_export = ifelse.(ismissing.(df_share_BE), NaN, df_share_BE)[1:15_000,:] # excel cannot deal with missing (but still an issue opening the file)
XLSX.writetable(dir_io * "clean/" * "df_share_BE.xlsx", df_export, overwrite=true)

CSV.write(dir_io * "clean/" * "df_share_BE.csv", df_share_BE)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
