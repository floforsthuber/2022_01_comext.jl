# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script creating monthly data files
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics

# other scripts
dir_home = "x:/VIVES/1-Personal/Florian/git/2022_01_comext/src/"
include(dir_home * "functions.jl")

# location of data input/output (io)
dir_io = "C:/Users/u0148308/data/comext/" 
dir_dropbox = "C:/Users/u0148308/Dropbox/BREXIT/"

# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# timespan
years = string.(2001:2021)
months = lpad.(1:12, 2, '0')

# subsets string
g(x) = x[end-1:end]

periods = repeat(years, inner=12) .* repeat(months, outer=length(years))
periods = parse.(Int64, periods) # convert to Int64 (actually needed? think we can do all with String)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------


# create monthly files
# Notes:
#   - import data for 3 months, compute prices and shares
#       + could use 2 months only but then we can only compute MOM. In this setting we obtain also UNIT_PRICE_CHANGE between Jan and Mar,
#         this might be useful later if we realize MOM is too restrictive
#   - subset to middle month data export (i.e. Feb if Jan, Feb, Mar)
#       + use Jan & Feb to calculate MOM change
#       + use Feb & Mar to calculate PRICE_CHANGE (actually when subset and exported we lose this again => use 2 months only and drop PRICE_CHANGE)
#   - lose first and last month (i.e. cannot compute changes since no previous/following data)

for (i, time) in enumerate(periods[1:end-3])

    # specify data to import
    year = string.(time)[1:4]
    month = g.(string.(periods[[i, i+1, i+2]]))
    sub_month = month[2]

    # initialize dataframe
    df = DataFrame(DECLARANT_ISO=String[], PARTNER_ISO=String[], TRADE_TYPE=String[], PRODUCT_NC=String[], FLOW=String[], PERIOD=Int64[],
                    VALUE_IN_EUROS=Union{Missing, Float64}[], QUANTITY_IN_KG=Union{Missing, Float64}[], 
                    PRODUCT_SITC=String[], PRODUCT_CPA2008=String[], PRODUCT_CPA2002=String[], PRODUCT_CPA2_1=String[],
                    PRODUCT_BEC=String[], PRODUCT_BEC5=String[], PRODUCT_SECTION=String[])

    # add 3 months to dataframe and do initial cleaning
    append!(df, initial_cleaning(year, month[1]))
    append!(df, initial_cleaning(year, sub_month))
    append!(df, initial_cleaning(year, month[3]))

    #sort!(df)
    sort!(df, [:DECLARANT_ISO, :PARTNER_ISO, :PRODUCT_NC, :FLOW, :TRADE_TYPE, :PERIOD]) # do sorting just in case (unexplainable problem below)

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
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "PRODUCT_NC", "FLOW"]
    gdf = groupby(df, cols_grouping)
    df = transform(gdf, [:PERIOD, :UNIT_PRICE] => mom_change => :MOM)

    # export data
    df_export = subset(df, :PERIOD => ByRow( x-> string(x) == year*sub_month))
    CSV.write(dir_io * "clean/prices/" * "df_prices_" * year * sub_month * ".csv", df_export)

    println(" ??? Price data for $sub_month/$year was successfully created.")


    # -------------------------------------------------------------------------------------------------------------------------------------------------------------
    # Share of Belgium
    # -------------------------------------------------------------------------------------------------------------------------------------------------------------

    # compute EU_WORLD (EU imports/exports to the entire world)
    cols_grouping = ["PRODUCT_NC", "FLOW", "PERIOD"]
    prod_class_other = ["PRODUCT_SITC", "PRODUCT_CPA2002", "PRODUCT_CPA2008", "PRODUCT_CPA2_1", "PRODUCT_BEC", "PRODUCT_BEC5"]
    cols_grouping = [cols_grouping; prod_class_other]

    gdf = groupby(df, cols_grouping)
    df_sum_EU_WORLD = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> (x -> sum(skipmissing(x))), renamecols=false)
    df_sum_EU_WORLD.TRADE_TYPE .= "total"

    # compute EU_EXTRA and EU_INTRA (EU imports/exports to extra/intra EU countries)
    cols_grouping = ["TRADE_TYPE", "PRODUCT_NC", "FLOW", "PERIOD"]
    cols_grouping = [cols_grouping; prod_class_other]
    gdf = groupby(df, cols_grouping)
    df_sum_EU_EXTRA_INTRA = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> (x -> sum(skipmissing(x))), renamecols=false)

    # compute BE_WORLD (BE imports/exports to the entire world)
    df_BE = subset(df, :DECLARANT_ISO => ByRow(x -> x == "BE"))
    cols_grouping = ["PRODUCT_NC", "FLOW", "PERIOD"]
    cols_grouping = [cols_grouping; prod_class_other]

    gdf = groupby(df_BE, cols_grouping)
    df_sum_BE_WORLD = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> (x -> sum(skipmissing(x))), renamecols=false)
    df_sum_BE_WORLD.TRADE_TYPE .= "total"

    # compute BE_EXTRA and BE_INTRA (BE imports/exports to extra/intra EU countries)
    cols_grouping = ["TRADE_TYPE", "PRODUCT_NC", "FLOW", "PERIOD"]
    cols_grouping = [cols_grouping; prod_class_other]
    gdf = groupby(df_BE, cols_grouping)
    df_sum_BE_EXTRA_INTRA = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> (x -> sum(skipmissing(x))), renamecols=false)

    # merge tables
    df_sum_EU = vcat(df_sum_EU_EXTRA_INTRA, df_sum_EU_WORLD)
    df_sum_BE = vcat(df_sum_BE_EXTRA_INTRA, df_sum_BE_WORLD)

    # join, rename and compute shares in percentages (?)
    cols_join = ["TRADE_TYPE", "PRODUCT_NC", "FLOW", "PERIOD"]
    cols_join = [cols_join; prod_class_other]
    df_join = leftjoin(df_sum_EU, df_sum_BE, on=cols_join, makeunique=true)

    rename!(df_join, ["VALUE_IN_EUROS", "QUANTITY_IN_KG", "VALUE_IN_EUROS_1", "QUANTITY_IN_KG_1"] .=> ["VALUE_EU", "QUANITY_EU", "VALUE_BE", "QUANITY_BE"])

    df_join.VALUE_SHARE = df_join.VALUE_BE ./ df_join.VALUE_EU .* 100
    df_join.QUANTITY_SHARE = df_join.QUANITY_BE ./ df_join.QUANITY_EU .* 100

    # possibly need to sort before grouping, seems that grouping sorts incorrectly sometimes?
    sort!(df_join, [:PRODUCT_NC, :FLOW, :TRADE_TYPE, :PERIOD]) # dont understand why I need the sorting here? otherwise some MOM are not taken?

    # compute SHARE_CHANGE
    cols_grouping = ["TRADE_TYPE", "PRODUCT_NC", "FLOW"]
    gdf = groupby(df_join, cols_grouping)
    df_share_BE = transform(gdf, [:VALUE_SHARE, :QUANTITY_SHARE] .=> pct_change .=> [:VALUE_SHARE_CHANGE, :QUANTITY_SHARE_CHANGE])

    # compute MOM_CHANGE
    cols_grouping = ["TRADE_TYPE", "PRODUCT_NC", "FLOW"]
    gdf = groupby(df_share_BE, cols_grouping)
    df_share_BE = transform(gdf, [:PERIOD, :VALUE_SHARE] => mom_change => :MOM_VALUE,
                                 [:PERIOD, :QUANTITY_SHARE] => mom_change => :MOM_QUANTITY)

    # export data
    df_export = subset(df_share_BE, :PERIOD => ByRow( x-> string(x) == year*sub_month))
    CSV.write(dir_io * "clean/shares/" * "df_share_BE_" * year * sub_month * ".csv", df_export)

    println(" ??? Belgian shares data for $sub_month/$year were successfully created. \n")


end

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
