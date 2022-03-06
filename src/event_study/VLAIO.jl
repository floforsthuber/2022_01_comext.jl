# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script to compile data for some descriptive statistics
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics, StatsBase, StatsPlots, Dates

# other scripts
dir_home = "x:/VIVES/1-Personal/Florian/git/2022_01_comext/src/"
include(dir_home * "functions.jl")

# location of data input/output (io)
dir_io = "C:/Users/u0148308/data/comext/" 
dir_dropbox = "C:/Users/u0148308/Dropbox/BREXIT/"


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Raw data
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

df_VLAIO = CSV.read(dir_dropbox * "rawdata/" * "df_VLAIO" * ".csv", DataFrame)
transform!(df_VLAIO, ["PRODUCT_NC", "PRODUCT_NC_LAB1", "PRODUCT_NC_LAB2", "DECLARANT_ISO", "TRADE_TYPE", "PARTNER_ISO", "FLOW"] .=> ByRow(string), renamecols=false)
transform!(df_VLAIO, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> convert(Union{Missing, Float64}, x)), renamecols=false)

# remove missings
df_VLAIO.PRODUCT_NC = lpad.(string.(df_VLAIO.PRODUCT_NC), 8, '0')
subset!(df_VLAIO, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> !ismissing(x))) # lose 1-16027346/21667553 ~26% observations

# MAD adjustment
outlier_cutoff = 3
transform!(df_VLAIO, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)
gdf = groupby(df_VLAIO, "PRODUCT_NC")
df_VLAIO = transform(gdf, :UNIT_PRICE => MAD_method => :MAD)
subset!(df_VLAIO, :MAD => ByRow(x -> x < outlier_cutoff)) # lose further 1-13110297/16027346 ~18% observations (~40% of total)
df_VLAIO = df_VLAIO[:, Not([:UNIT_PRICE, :MAD])]

# add EU/WORLD again
EU27 = ["Roemenië", "Griekenland", "Oostenrijk", "Polen", "Duitsland", "Spanje", "Hongarije", "Slovakije", "Italië", "Nederland",
       "Frankrijk", "Letland", "Kroatië", "Cyprus", "Malta", "Litouwen", "Slovenië", "Estland", "Portugal", "Finland", "Tsjechië", 
       "Luxemburg", "Zweden", "Denemarken", "Bulgarije", "Ierland", "België"]

# slightly modified functions
function append_WORLD(df::DataFrame)

    # sum over PARTNER
    cols_grouping = ["DECLARANT_ISO", "PRODUCT_NC", "PRODUCT_NC_LAB1", "PRODUCT_NC_LAB2", "TRADE_TYPE", "FLOW", "PERIOD"]
    gdf = groupby(df, cols_grouping)
    df_WORLD = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> (x -> sum(skipmissing(x))), renamecols=false)

    df_WORLD.TRADE_TYPE .= "total"
    df_WORLD.PARTNER_ISO .= "WORLD"

    df = vcat(df, df_WORLD)

    return df
end

function append_EU(df::DataFrame, EU::Vector{String})

    # subset and sum over EU ctrys
    df_EU = subset(df, :PARTNER_ISO => ByRow(x -> x in EU))
    cols_grouping = ["DECLARANT_ISO", "PRODUCT_NC", "PRODUCT_NC_LAB1", "PRODUCT_NC_LAB2", "TRADE_TYPE", "FLOW", "PERIOD"]
    gdf = groupby(df_EU, cols_grouping)
    df_EU = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> (x -> sum(skipmissing(x))), renamecols=false)

    df_EU.TRADE_TYPE .= "intra"
    df_EU.PARTNER_ISO .= "EU"

    df = vcat(df, df_EU)

    return df
end

df_VLAIO = append_WORLD(df_VLAIO)
df_VLAIO = append_EU(df_VLAIO, EU27)

# double check if no zeros/missing introduced by WORLD/EU
transform!(df_VLAIO, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> ifelse(iszero(x) | ismissing(x), missing, x)), renamecols=false)
subset!(df_VLAIO, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> !ismissing(x))) # lose 1-15228494/15228494 ~0% observations

# -------------------------------------------------------------------------------------------------------------------------------------------------------------

cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD"]
gdf = groupby(df_VLAIO[:, [cols_grouping; "PRODUCT_NC"]], cols_grouping)

years = string.(2014:2021)
months = lpad.(1:12, 2, '0')
period = repeat(years, inner=12) .* repeat(months, outer=length(years))
period = parse.(Int64, period) # convert to Int64

intersection_exports = unique(df_VLAIO.PRODUCT_NC) # simply start with all products
intersection_imports = unique(df_VLAIO.PRODUCT_NC) # simply start with all products

for (i, time) in enumerate(period[1:end-2])

    # exports
    prod_GB_t1 = gdf[(DECLARANT_ISO = "Vlaanderen", PARTNER_ISO = "Verenigd Koninkrijk", FLOW = "exports", PERIOD = period[i])].PRODUCT_NC
    prod_GB_t2 = gdf[(DECLARANT_ISO = "Vlaanderen", PARTNER_ISO = "Verenigd Koninkrijk", FLOW = "exports", PERIOD = period[i+1])].PRODUCT_NC

    prod_NL_t1 = gdf[(DECLARANT_ISO = "Vlaanderen", PARTNER_ISO = "Nederland", FLOW = "exports", PERIOD = period[i])].PRODUCT_NC
    prod_NL_t2 = gdf[(DECLARANT_ISO = "Vlaanderen", PARTNER_ISO = "Nederland", FLOW = "exports", PERIOD = period[i+1])].PRODUCT_NC

    intersection_exports = intersect(intersection_exports, prod_GB_t1, prod_GB_t2, prod_NL_t1, prod_NL_t2)
    
    # imports
    prod_GB_t1 = gdf[(DECLARANT_ISO = "Vlaanderen", PARTNER_ISO = "Verenigd Koninkrijk", FLOW = "imports", PERIOD = period[i])].PRODUCT_NC
    prod_GB_t2 = gdf[(DECLARANT_ISO = "Vlaanderen", PARTNER_ISO = "Verenigd Koninkrijk", FLOW = "imports", PERIOD = period[i+1])].PRODUCT_NC

    prod_NL_t1 = gdf[(DECLARANT_ISO = "Vlaanderen", PARTNER_ISO = "Nederland", FLOW = "imports", PERIOD = period[i])].PRODUCT_NC
    prod_NL_t2 = gdf[(DECLARANT_ISO = "Vlaanderen", PARTNER_ISO = "Nederland", FLOW = "imports", PERIOD = period[i+1])].PRODUCT_NC

    intersection_imports = intersect(intersection_exports, prod_GB_t1, prod_GB_t2, prod_NL_t1, prod_NL_t2)

end