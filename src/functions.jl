# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script collecting all functions used in the repository
# -------------------------------------------------------------------------------------------------------------------------------------------------------------




# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Functions for making raw data operational
# -------------------------------------------------------------------------------------------------------------------------------------------------------------


# initial cleaning of the data               
function initial_cleaning(year::String, month::String)
    
    path = dir_dropbox * "rawdata/comext/" * "full" * year * month * ".dat"
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

# function to create WORLD as a seperate partner
# Notes:
#   - need to create WORLD before EU, otherwise EU countries counted twice
function append_WORLD(df::DataFrame)
    
    # sum over PARTNER
    df_WORLD = subset(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> !ismissing(x)))
    subset!(df_WORLD, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> x != 0))
    cols_grouping = ["DECLARANT_ISO", "PRODUCT_NC", "PRODUCT_SITC", "PRODUCT_CPA2002", "PRODUCT_CPA2008", "PRODUCT_CPA2_1",
                    "PRODUCT_BEC", "PRODUCT_BEC5", "PRODUCT_SECTION", "FLOW", "PERIOD"]
    gdf = groupby(df_WORLD, cols_grouping)
    df_WORLD = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> sum, renamecols=false)

    df_WORLD.TRADE_TYPE .= "total"
    df_WORLD.PARTNER_ISO .= "WORLD"

    df = vcat(df, df_WORLD)

    return df
end

# function to create EU as a seperate partner
# Notes:
#   - with the vector "EU" one can specify the exact countries
function append_EU(df::DataFrame, EU::Vector{String})

    # subset
    df_EU = subset(df, :TRADE_TYPE => ByRow(x-> x == "intra"), :PARTNER_ISO => ByRow(x -> x in EU))
    subset!(df_EU, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> !ismissing(x)))
    subset!(df_EU, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> x != 0))
    cols_grouping = ["DECLARANT_ISO", "PRODUCT_NC", "PRODUCT_SITC", "PRODUCT_CPA2002", "PRODUCT_CPA2008", "PRODUCT_CPA2_1",
                    "PRODUCT_BEC", "PRODUCT_BEC5", "PRODUCT_SECTION", "FLOW", "PERIOD"]
    gdf = groupby(df_EU, cols_grouping)
    df_EU = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> sum, renamecols=false)
    df_EU.TRADE_TYPE .= "intra"
    df_EU.PARTNER_ISO .= "EU"

    df = vcat(df, df_EU)

    return df
end

# function to import data for single country
function import_ctry(year::String, month::String, ctry::String)

    path = dir_dropbox * "rawdata/comext/" * ctry * "/" * "full" * year * month * "_" * ctry * ".csv"
    df = CSV.read(path, DataFrame)

    cols_string = ["DECLARANT_ISO", "PARTNER_ISO", "TRADE_TYPE", "PRODUCT_NC", "PRODUCT_SITC", "PRODUCT_CPA2002", "PRODUCT_CPA2008", "PRODUCT_CPA2_1",
                "PRODUCT_BEC", "PRODUCT_BEC5", "PRODUCT_SECTION", "FLOW"]
    transform!(df,  cols_string .=> ByRow(string), renamecols=false)
    
    # take out rows with missing values and quantities (already taken out when computing EU/WORLD)
    subset!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> !ismissing(x)))
    subset!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> x != 0))
    transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(Float64), renamecols=false)

    return df
end


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Functions for computing statistics
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# computes pct_change between successive periods
# Notes:
#   - percentages?
#   - not every product imported/exported every period
#       + calculate price changes between periods when products are bought, i.e. price change between Feb2019 and March2019, 
#         but could also be Feb2019 and Dec2020!
function pct_change(input::AbstractVector)
    [i == 1 ? missing : (input[i]-input[i-1])/input[i-1]*100 for i in eachindex(input)]
end


# computes pct_change between successive months
# Notes:
#   - functions computes UNIT_PRICE_CHANGE only for successive periods (months)
#       + notice the double ifelse statement in shorthand, find a way to use ifelse instead to make it easier to read!
function mom_change(period::AbstractVector, input::AbstractVector)
    [i == 1 ? missing : period[i]-period[i-1] != 1 ? missing : (input[i]-input[i-1])/input[i-1]*100 for i in eachindex(input)]
end


# moving average
# Notes:
#   - specify range with 'n'
movingaverage(input::AbstractArray, n::Int64) = [i < n ? missing : mean(input[i-n+1:i]) for i in eachindex(input)]


# computes standard deviation for rolling window
# requires PERIOD to be sorted when grouped
function rolling_std(input::AbstractArray, n::Int64)
    @assert 1 <= n <= length(input)
    output = missings(Float64, length(input)) # initialize vector (keep first n as missing)
    for i in eachindex(output)[n:end]
        output[i] = std(input[i-n+1:i])
    end
    return output
end

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# HP filter (Hodrick–Prescott)
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# Wikipedia: https://en.wikipedia.org/wiki/Hodrick%E2%80%93Prescott_filter
#   The first term of the equation is the sum of the squared deviations, which penalizes the cyclical component. 
#   The second term is a multiple λ of the sum of the squares of the trend component's second differences. 
#   This second term penalizes variations in the growth rate of the trend component. The larger the value of λ, the higher is the penalty. 
#   Hodrick and Prescott suggest 1600 as a value for λ for quarterly data. 
#   Ravn and Uhlig (2002) state that λ should vary by the fourth power of the frequency observation ratio. 
#   Thus, λ should equal 6.25 (1600/4^4) for annual data and 129,600 (1600*3^4) for monthly data. 
#   In practice, λ = 100 for yearly data and λ = 14400 for monthly data are commonly used.

function HP(x::AbstractArray, λ::Int64)
    n = length(x)
    m = 2
    @assert n > m
    I = Diagonal(ones(n))

    # use diagm instead of spdiagm otherwise error with grouped dataframe
    D = diagm(0 => fill(1, n-m),
        -1 => fill(-2, n-m),
        -2 => fill(1, n-m) )
    @inbounds D = D[1:n,1:n-m]

    return (I + λ * D * D') \ x
end


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Functions for creating data for figures
# -------------------------------------------------------------------------------------------------------------------------------------------------------------


# function to create data for figure 1
function data_fig1(df::DataFrame, declarants::Vector{String}, partners::Vector{String})

    # subset dataframe
    df = subset(df, :DECLARANT_ISO => ByRow(x -> x in declarants), :PARTNER_ISO => ByRow(x -> x in partners))
    subset!(df, :PRODUCT_NC => ByRow(x -> x != "TOTAL")) # take out total

    # compute UNIT_PRICE
    transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)

    # compute weights for price index
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD"]
    gdf = groupby(df, cols_grouping)
    df_total = combine(gdf, :VALUE_IN_EUROS => sum => :VALUE_TOTAL)
    df_join = leftjoin(df, df_total, on=cols_grouping)
    transform!(df_join, [:VALUE_IN_EUROS, :VALUE_TOTAL] => ByRow((v,s) -> v/s) => :WEIGHTS_VALUE)
    transform!(df_join, [:UNIT_PRICE, :WEIGHTS_VALUE] => ByRow((p, w) -> p * w) => :PRICE_INDEX)

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
    transform!(df_join, [:VALUE_IN_EUROS, :VALUE_TOTAL] => ByRow((v,s) -> v/s) => :WEIGHTS_VALUE)
    transform!(df_join, [:UNIT_PRICE, :WEIGHTS_VALUE] => ByRow((p, w) -> p * w) => :PRICE_INDEX)
    
    # aggregate over products
    gdf = groupby(df_join, cols_grouping)
    df = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG, :PRICE_INDEX] .=> sum, renamecols=false)

    return df
end


# function to create data for figure 3
function data_fig3(df::DataFrame, declarants::Vector{String}, partners::Vector{String})
    
    # clean df
    subset!(df, :PRODUCT_NC => ByRow(x -> x != "TOTAL")) # take out TOTAL

    # compute total exports/imports (i.e. to WORLD)
    df_total = subset(df, :DECLARANT_ISO => ByRow(x -> x in declarants), :PARTNER_ISO => ByRow(x -> x == "WORLD"))
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


# function to create data for table 1
function tab1(df::DataFrame, declarants::Vector{String}, partners::Vector{String}, digits::Int64)

    # clean df
    subset!(df, :DECLARANT_ISO => ByRow(x -> x in declarants))
    subset!(df, :PRODUCT_NC => ByRow(x -> x != "TOTAL")) # take out TOTAL

    # add 2 digit CN classification
    # Notes:
    #   - is simply the first digits
    g(x) = x[1:digits]
    df.PRODUCT_NC_digits = g.(df.PRODUCT_NC)

    # aggregate over new CN classification and PARNTERS => WORLD (to compute shares)
    subset!(df, :PARTNER_ISO => ByRow(x -> x in [partners; "WORLD"]))
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PRODUCT_NC_digits", "PERIOD"]
    gdf = groupby(df, cols_grouping)
    df = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> sum, renamecols=false)

    return df
end

