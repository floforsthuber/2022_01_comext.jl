# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script creating monthly data files
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics

dir_io = "C:/Users/u0148308/data/comext/" # location of input/output (io)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# timespan
years = string.(2015:2021)
months = lpad.(1:12, 2, '0')

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

function pct_change(input::AbstractVector)
    [i == 1 ? missing : (input[i]-input[i-1])/input[i-1]*100 for i in eachindex(input)]
end

function mom_change(period::AbstractVector, input::AbstractVector)
    [i == 1 ? missing : period[i]-period[i-1] != 1 ? missing : (input[i]-input[i-1])/input[i-1]*100 for i in eachindex(input)]
end

g(x) = x[end-1:end]

periods = repeat(years, inner=12) .* repeat(months, outer=length(years))
periods = parse.(Int64, periods) # convert to Int64 (actually needed? think we can do all with String)


# create monthly files
# Notes:
#   - import data for 3 months, compute prices and shares
#       + could use 2 months only but then we can only compute MOM. In this setting we obtain also UNIT_PRICE_CHANGE between Jan and Mar,
#         this might be useful later if we realize MOM is too restrictive
#   - subset to middle month (i.e. Feb if Jan, Feb, Mar) and export
#       + means we lose first and last month (i.e. cannot compute changes since no previous/following data)

for (i, time) in enumerate(periods[1:end-2])

    year = string.(time)[1:4]
    month = g.(string.(periods[[i, i+1, i+2]]))
    sub_month = month[2]

    df = DataFrame(DECLARANT_ISO=String[], PARTNER_ISO=String[], TRADE_TYPE=String[], PRODUCT_NC=String[], FLOW=String[], PERIOD=Int64[],
                VALUE_IN_EUROS=Union{Missing, Float64}[], QUANTITY_IN_KG=Union{Missing, Float64}[])

    append!(df, initial_cleaning(year, month[1]))
    append!(df, initial_cleaning(year, sub_month))
    append!(df, initial_cleaning(year, month[3]))

    sort!(df)

    # -------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Unit prices
    # -------------------------------------------------------------------------------------------------------------------------------------------------------------

    # compute UNIT_PRICE
    transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)

    # compute UNIT_PRICE_CHANGE
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "PRODUCT_NC", "FLOW"]
    gdf = groupby(df, cols_grouping)
    df = transform(gdf, :UNIT_PRICE => pct_change => :UNIT_PRICE_CHANGE)

    # compute MOM_PRICE_CHANGE
    gdf = groupby(df, cols_grouping)
    df = transform(gdf, [:PERIOD, :UNIT_PRICE] => mom_change => :MOM)

    # export data
    df_export = subset(df, :PERIOD => ByRow(x-> string(x) == year * sub_month))
    CSV.write(dir_io * "clean/" * "df_prices_" * year * sub_month * ".csv", df_export)

    println(" ✓ Price data for $sub_month/$year were successfully created.")


    # -------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Share of Belgium
    # -------------------------------------------------------------------------------------------------------------------------------------------------------------

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
    sort!(df_join, [:PRODUCT_NC, :FLOW, :TRADE_TYPE, :PERIOD])

    # compute SHARE_CHANGE
    cols_grouping = ["TRADE_TYPE", "PRODUCT_NC", "FLOW"]
    gdf = groupby(df_join, cols_grouping)
    df_share_BE = transform(gdf, [:VALUE_SHARE, :QUANTITY_SHARE] .=> pct_change .=> [:VALUE_SHARE_CHANGE, :QUANTITY_SHARE_CHANGE])

    # compute MOM_CHANGE
    gdf = groupby(df_share_BE, cols_grouping)
    df_share_BE = transform(gdf, [:PERIOD, :VALUE_SHARE] => mom_change => :MOM_VALUE,
                                 [:PERIOD, :QUANTITY_SHARE] => mom_change => :MOM_QUANTITY)


    # export data
    df_export = subset(df_share_BE, :PERIOD => ByRow(x-> string(x) == year * sub_month))
    CSV.write(dir_io * "clean/" * "df_share_BE_" * year * sub_month * ".csv", df_export)

    println(" ✓ Belgian shares data for $sub_month/$year were successfully created. \n")


end

