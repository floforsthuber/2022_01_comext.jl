# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script for some descriptive statistics
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics, StatsBase

dir_io = "C:/Users/u0148308/data/comext/" # location of input/output (io)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Import data into Julia
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# Notes:
#   - import raw data, initial cleaning
#   - subset and compute data for figure
#   - loop over months and add to DataFrame
#   - plot figure

# initial cleaning of the data               
function initial_cleaning(year::String, month::String)
    
    path = dir_io * "raw/" * "full" * year * month * ".dat"
    df = CSV.read(path, DataFrame)

    # column types
    # Notes:
    #   - treat PERIOD as numeric to allow sorting
    transform!(df, [:DECLARANT, :PARTNER, :PERIOD] .=> ByRow(Int64), renamecols=false)

    # additional PRODUCT_BEC5 column from 2017 onwards
    if year in ["2015", "2016"] # why does shorthand not work?
        df.PRODUCT_BEC5 .= missing
    else
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


df = initial_cleaning("2020", "01")

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# figure1
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# Notes:
#   - figure 1: evolution of total imports/exports from BE to UK vis-a-vis FR, NL, DE and EU
#   - figure 2: evolution of imports/exports from BE to UK at 2digit level
#   - figure 3: evolution of share of imports/exports from BE to UK of total imports/exports from BE
#   - all figures in values and unit values (unit prices):
#       + compute a price index as using a weighted sum
#   - include vertical lines for different Brexit events

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

# EU27
ctrys_EU = ["AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", "FR", "GR", "HR", "HU",
             "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK"]

df = append_EU(df, ctrys_EU)

# function to create data for figure 1
function data_fig1(df::DataFrame, declarants::Vector{String}, partners::Vector{String})

    # subset dataframe
    df = subset(df, :DECLARANT_ISO => ByRow(x -> x in declarants), :PARTNER_ISO => ByRow(x -> x in partners))
    subset!(df, :PRODUCT_NC => ByRow(x -> x != "TOTAL")) # take out total
    subset!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> !ismissing(x))) # take out missing values
    subset!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> x != 0))

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

df_fig1 = DataFrame(DECLARANT_ISO=String[], PARTNER_ISO=String[], FLOW=String[], PERIOD=Int64[],
                    VALUE_IN_EUROS=Float64[], QUANTITY_IN_KG=Float64[], PRICE_INDEX=Float64[])


a = data_fig1(df, ["BE"], ["GB", "DE", "FR", "NL", "EU"])


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

df_fig2 = DataFrame(DECLARANT_ISO=String[], PARTNER_ISO=String[], FLOW=String[], PERIOD=Int64[], PRODUCT_NC_digits=String[],
                    VALUE_IN_EUROS=Float64[], QUANTITY_IN_KG=Float64[], PRICE_INDEX=Float64[])

a = data_fig2(df, ["BE"], ["GB"], 2)


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
    df_total_partner = subset(df, :DECLARANT_ISO => ByRow(x -> x in ["BE"]), :PARTNER_ISO => ByRow(x -> x in ["GB"]))
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

df_fig3 = DataFrame(DECLARANT_ISO=String[], PARTNER_ISO=String[], FLOW=String[], PERIOD=Int64[], 
                    VALUE_SHARE=Float64[], QUANTITY_SHARE=Float64[])


a = data_fig3(df, ["BE"], ["GB"])


