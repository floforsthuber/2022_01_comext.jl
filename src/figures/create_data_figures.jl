# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script to compile data for some descriptive statistics
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics, StatsBase

dir_io = "C:/Users/u0148308/data/comext/" # location of input/output (io)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# Notes:
#   - import raw data, initial cleaning
#   - subset and compute data for figure
#   - loop over months and add to DataFrame
#   - export data
#   - import data and plot figures (different script)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------


# initial cleaning of the data               
function initial_cleaning(year::String, month::String)
    
    path = dir_io * "raw/" * "full" * year * month * ".dat"
    df = CSV.read(path, DataFrame)

    # column types
    # Notes:
    #   - treat PERIOD as numeric to allow sorting
    transform!(df, [:DECLARANT, :PARTNER, :PERIOD] .=> ByRow(Int64), renamecols=false)

    # additional PRODUCT_BEC5 column from 2017 onwards
    if year in string.(2001:2016) # why does shorthand not work?
        df.PRODUCT_BEC5 .= missing
    else
    end

    # column names different: PRODUCT_cpa2002, PRODUCT_cpa2008
    if year in string.(2001:2001)
        rename!(df, :PRODUCT_cpa2002 => :PRODUCT_CPA2002, :PRODUCT_cpa2008 => :PRODUCT_CPA2008)
    elseif year in string.(2002:2007)
        rename!(df, :PRODUCT_cpa2008 => :PRODUCT_CPA2008)
    end

    cols_string = ["DECLARANT_ISO", "PARTNER_ISO", "TRADE_TYPE", "PRODUCT_NC", "PRODUCT_SITC", "PRODUCT_CPA2002", "PRODUCT_CPA2008", "PRODUCT_CPA2_1",
                   "PRODUCT_BEC", "PRODUCT_BEC5", "PRODUCT_SECTION", "FLOW", "STAT_REGIME"]
    transform!(df,  cols_string .=> ByRow(string), renamecols=false)

    transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG, :SUP_QUANTITY] .=> ByRow(Float64), renamecols=false)

    # formatting
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
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "TRADE_TYPE", "PRODUCT_NC", "PRODUCT_SITC", "PRODUCT_CPA2002", "PRODUCT_CPA2008", "PRODUCT_CPA2_1",
                     "PRODUCT_BEC", "PRODUCT_BEC5", "PRODUCT_SECTION", "FLOW", "PERIOD"]
    gdf = groupby(df, cols_grouping)
    df = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> sum, renamecols=false)

    return df
end


# function to create EU as a seperate partner
# Notes:
#   - with the parameter "EU" one can specify the exact countries
function append_EU(df::DataFrame, EU::Vector{String})

    # subset
    df_EU = subset(df, :TRADE_TYPE => ByRow(x-> x == "intra"), :PARTNER_ISO => ByRow(x -> x in EU))
    cols_grouping = ["DECLARANT_ISO", "PRODUCT_NC", "PRODUCT_SITC", "PRODUCT_CPA2002", "PRODUCT_CPA2008", "PRODUCT_CPA2_1",
                    "PRODUCT_BEC", "PRODUCT_BEC5", "PRODUCT_SECTION", "FLOW", "PERIOD"]
    gdf = groupby(df_EU, cols_grouping)
    df_EU = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> (x -> sum(skipmissing(x))), renamecols=false)
    df_EU.TRADE_TYPE .= "intra"
    df_EU.PARTNER_ISO .= "EU"

    df = vcat(df, df_EU)

    return df
end


# function to create data for figure 1
function data_fig1(df::DataFrame, declarants::Vector{String}, partners::Vector{String})

    # subset dataframe
    df = subset(df, :DECLARANT_ISO => ByRow(x -> x in declarants), :PARTNER_ISO => ByRow(x -> x in partners))
    subset!(df, :PRODUCT_NC => ByRow(x -> x != "TOTAL")) # take out total
    subset!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> !ismissing(x))) # take out missing values
    subset!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> x != 0)) # take out 0 (there are some introduced by adding EU)

    # compute UNIT_PRICE
    transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)

    # compute weights for price index
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD"]
    gdf = groupby(df, cols_grouping)
    df_total = combine(gdf, :VALUE_IN_EUROS => sum => :VALUE_TOTAL)
    df_join = leftjoin(df, df_total, on=cols_grouping)
    transform!(df_join, [:VALUE_IN_EUROS, :VALUE_TOTAL] => ByRow((v,s) -> v/s) => :VALUE_WEIGHTS)
    transform!(df_join, [:UNIT_PRICE, :VALUE_WEIGHTS] => ByRow((p, w) -> p * w) => :PRICE_INDEX)

    # aggregate over products
    gdf = groupby(df_join, cols_grouping)
    df = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG, :PRICE_INDEX] .=> sum, renamecols=false)


    return df
end


# function to create data for figure 2
# Notes:
#   - essentially the same function as for figure 1 but with additional grouping at CN digit level
function data_fig2(df::DataFrame, declarants::Vector{String}, partners::Vector{String}, digits::Int64)
        
    # subset dataframe
    df = subset(df, :DECLARANT_ISO => ByRow(x -> x in declarants), :PARTNER_ISO => ByRow(x -> x in partners))
    subset!(df, :PRODUCT_NC => ByRow(x -> x != "TOTAL")) # take out total
    subset!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> !ismissing(x))) # take out missing values

    # compute UNIT_PRICE
    transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)

    # add 2 digit CN classification
    # Notes:
    #   - is simply the first digits
    g(x) = x[1:digits]
    df.PRODUCT_NC_digits = g.(df.PRODUCT_NC)

    # compute weights for price index
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD", "PRODUCT_NC_digits"]
    gdf = groupby(df, cols_grouping)
    df_total = combine(gdf, :VALUE_IN_EUROS => sum => :VALUE_TOTAL)
    df_join = leftjoin(df, df_total, on=cols_grouping)
    transform!(df_join, [:VALUE_IN_EUROS, :VALUE_TOTAL] => ByRow((v,s) -> v/s) => :VALUE_WEIGHTS)
    transform!(df_join, [:UNIT_PRICE, :VALUE_WEIGHTS] => ByRow((p, w) -> p * w) => :PRICE_INDEX)
    
    # aggregate over products
    gdf = groupby(df_join, cols_grouping)
    df = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG, :PRICE_INDEX] .=> sum, renamecols=false)

    return df
end


# function to create data for figure 3
function data_fig3(df::DataFrame, declarants::Vector{String}, partners::Vector{String})
    
    # clean df
    subset!(df, :PRODUCT_NC => ByRow(x -> x != "TOTAL")) # take out TOTAL
    subset!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> !ismissing(x))) # take out missing values

    # compute total exports/imports
    df_total = subset(df, :DECLARANT_ISO => ByRow(x -> x in ["BE"]))
    cols_grouping = ["DECLARANT_ISO", "FLOW", "PERIOD"]
    gdf = groupby(df_total, cols_grouping)
    df_total = combine(gdf, :VALUE_IN_EUROS => sum => :VALUE_TOTAL, :QUANTITY_IN_KG => sum => :QUANTITY_TOTAL)

    # compute total exports/imports for each partner
    df_total_partner = subset(df, :DECLARANT_ISO => ByRow(x -> x in declarants), :PARTNER_ISO => ByRow(x -> x in partners))
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD"]
    gdf = groupby(df_total_partner, cols_grouping)
    df_total_partner = combine(gdf, :VALUE_IN_EUROS => sum => :VALUE_TOTAL_PARTNER, :QUANTITY_IN_KG => sum => :QUANTITY_TOTAL_PARTNER)

    # compute export/import shares
    # Notes:
    #   - shares are in percentages
    df_join = leftjoin(df_total_partner, df_total, on=["DECLARANT_ISO", "FLOW", "PERIOD"])
    transform!(df_join, [:VALUE_TOTAL_PARTNER, :VALUE_TOTAL] => ByRow((v,s) -> v/s*100) => :VALUE_SHARE)
    transform!(df_join, [:QUANTITY_TOTAL_PARTNER, :QUANTITY_TOTAL] => ByRow((v,s) -> v/s*100) => :QUANTITY_SHARE)

    df = df_join[:,Not([:VALUE_TOTAL_PARTNER, :VALUE_TOTAL, :QUANTITY_TOTAL_PARTNER, :QUANTITY_TOTAL])]

    return df
end


# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# timespan
years = string.(2001:2021)
months = lpad.(1:12, 2, '0')

# EU27
ctrys_EU27 = ["AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", "FR", "GR", "HR", "HU",
             "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK"]

# initialize dataframes
df_fig1 = DataFrame(DECLARANT_ISO=String[], PARTNER_ISO=String[], FLOW=String[], PERIOD=Int64[],
                    VALUE_IN_EUROS=Float64[], QUANTITY_IN_KG=Float64[], PRICE_INDEX=Float64[])

df_fig2 = DataFrame(DECLARANT_ISO=String[], PARTNER_ISO=String[], FLOW=String[], PERIOD=Int64[], PRODUCT_NC_digits=String[],
                    VALUE_IN_EUROS=Float64[], QUANTITY_IN_KG=Float64[], PRICE_INDEX=Float64[])

df_fig3 = DataFrame(DECLARANT_ISO=String[], PARTNER_ISO=String[], FLOW=String[], PERIOD=Int64[], 
                    VALUE_SHARE=Float64[], QUANTITY_SHARE=Float64[])


for i in years
    for j in months

        #import and clean data
        df = initial_cleaning(i, j)

        # create EU as partner
        df = append_EU(df, ctrys_EU27)

        # append data for figures
        append!(df_fig1, data_fig1(df, ["BE"], ["GB", "EU", "DE", "FR", "NL"]))
        append!(df_fig2, data_fig2(df, ["BE"], ["GB"], 2))
        append!(df_fig3, data_fig3(df, ["BE"], ["GB", "EU", "DE", "FR", "NL"]))

        println(" ✓ Data for figure 1, 2 and 3 has been successfully added for $j/$i. \n")
    end
end

# export 
CSV.write(dir_io * "clean/" * "df_fig1" * ".csv", df_fig1)
CSV.write(dir_io * "clean/" * "df_fig2" * ".csv", df_fig2)
CSV.write(dir_io * "clean/" * "df_fig3" * ".csv", df_fig3)



# -------------------------------------------------------------------------------------------------------------------------------------------------------------

function tab1(df::DataFrame, declarants::Vector{String}, partners::Vector{String}, digits::Int64)

    # clean df
    subset!(df, :PRODUCT_NC => ByRow(x -> x != "TOTAL")) # take out TOTAL
    subset!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> !ismissing(x))) # take out missing values

    # add 2 digit CN classification
    # Notes:
    #   - is simply the first digits
    g(x) = x[1:digits]
    df.PRODUCT_NC_digits = g.(df.PRODUCT_NC)

    # add year identifier
    h(x) = x[1:4]
    df.YEAR = h.(string.(df.PERIOD))

    # compute total exports/imports
    cols_grouping = ["DECLARANT_ISO", "FLOW", "YEAR"]
    gdf = groupby(df, cols_grouping)
    df_total = combine(gdf, :VALUE_IN_EUROS => sum => :VALUE_TOTAL, :QUANTITY_IN_KG => sum => :QUANTITY_TOTAL)

    # compute total exports/imports per partner
    df_partner = subset(df, :DECLARANT_ISO => ByRow(x -> x in declarants), :PARTNER_ISO => ByRow(x -> x in partners))

    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "YEAR"]
    gdf = groupby(df_partner, cols_grouping)
    df_total_partner = combine(gdf, :VALUE_IN_EUROS => sum => :VALUE_TOTAL_PARTNER, :QUANTITY_IN_KG => sum => :QUANTITY_TOTAL_PARTNER)
    
    # compute total exports/imports for each partner and product
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "YEAR", "PRODUCT_NC_digits"]
    gdf = groupby(df_partner, cols_grouping)
    df_partner_product = combine(gdf, :VALUE_IN_EUROS => sum, :QUANTITY_IN_KG => sum, renamecols=false)
    
    # compute export/import shares per partner
    # Notes:
    #   - shares are in percentages
    df_join = leftjoin(df_partner_product, df_total_partner, on=["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "YEAR"])
    transform!(df_join, [:VALUE_IN_EUROS, :VALUE_TOTAL_PARTNER] => ByRow((v,s) -> v/s*100) => :VALUE_SHARE_PARTNER)
    transform!(df_join, [:QUANTITY_IN_KG, :QUANTITY_TOTAL_PARTNER] => ByRow((v,s) -> v/s*100) => :QUANTITY_SHARE_PARTNER)

    # compute export/import shares of total
    df = leftjoin(df_join, df_total, on=["DECLARANT_ISO", "FLOW", "YEAR"])
    transform!(df, [:VALUE_IN_EUROS, :VALUE_TOTAL] => ByRow((v,s) -> v/s*100) => :VALUE_SHARE_TOTAL)
    transform!(df, [:QUANTITY_IN_KG, :QUANTITY_TOTAL] => ByRow((v,s) -> v/s*100) => :QUANTITY_SHARE_TOTAL)
    
    # remove columns with values, keep only shares
    cols_remove = ["VALUE_IN_EUROS", "VALUE_TOTAL", "VALUE_TOTAL_PARTNER", "QUANTITY_IN_KG", "QUANTITY_TOTAL", "QUANTITY_TOTAL_PARTNER"]
    df = df[:, Not(cols_remove)]

    return df
end

a = tab1(df, ["BE"], ["GB", "FR"], 2)