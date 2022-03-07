# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Benchmark results
#   - Fernandes and Winters (2021, p. 7)
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics, StatsBase, StatsPlots, Dates, TimeSeries

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
df_VLAIO = df_VLAIO[:, Not(:MAD)]

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

# df_VLAIO = append_WORLD(df_VLAIO)
# df_VLAIO = append_EU(df_VLAIO, EU27)

# double check if no zeros/missing introduced by WORLD/EU
transform!(df_VLAIO, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> ifelse(iszero(x) | ismissing(x), missing, x)), renamecols=false)
subset!(df_VLAIO, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> !ismissing(x))) # lose 1-15228494/15228494 ~0% observations

cols_subset = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD", "PRODUCT_NC", "VALUE_IN_EUROS", "QUANTITY_IN_KG", "UNIT_PRICE"]
df_VLAIO = df_VLAIO[:, cols_subset]
sort!(df_VLAIO, :PERIOD)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# YoY percentage change
#   - computes percentage change if difference between PERIOD == 100 (201512 - 201412 = 100)
function yoy_change(period::AbstractVector, input::AbstractVector)
    M = [period[i]-period[j] == 100 ? log(input[i]/input[j]) : missing for i in eachindex(input), j in eachindex(input)] # matrix
    V = [all(ismissing.(M[i,:])) ? missing : M[i, findfirst(typeof.(M[i,:]) .== Float64)] for i in 1:size(M, 1)] # reduce to vector
    return V
end

# compute 
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "PRODUCT_NC", "FLOW"]
gdf = groupby(df_VLAIO, cols_grouping)
df = transform(gdf, [:PERIOD, :VALUE_IN_EUROS] => yoy_change => :YOY_VALUE, [:PERIOD, :QUANTITY_IN_KG] => yoy_change => :YOY_QUANTITY,
            [:PERIOD, :UNIT_PRICE] => yoy_change => :YOY_PRICE)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# EXAMPLE with GB, NL, DE

transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)

# treatment period
t_pre = Date(2015,7,1):Month(1):Date(2016,6,1)
t_post = Date(2016,7,1):Month(1):Date(2017,7,1)

# subsetting
subset!(df, :YOY_PRICE => ByRow(x -> !ismissing(x)))

# example with GB and NL and exports
subset!(df, :PARTNER_ISO => ByRow(x -> x in ["Nederland", "Verenigd Koninkrijk", "Duitsland"]), :DATE => ByRow(x -> x in [t_pre; t_post]))

# common products
#   - products which are exported to both GB and NL in pre-treatment period
prod_GB = unique(subset(df, :PARTNER_ISO => ByRow(x -> x == "Verenigd Koninkrijk"), :FLOW => ByRow(x -> x == "exports"), :DATE => ByRow(x -> x in t_pre)).PRODUCT_NC)
prod_NL = unique(subset(df, :PARTNER_ISO => ByRow(x -> x == "Nederland"), :FLOW => ByRow(x -> x == "exports"), :DATE => ByRow(x -> x in t_pre)).PRODUCT_NC)
prod_DE = unique(subset(df, :PARTNER_ISO => ByRow(x -> x == "Duitsland"), :FLOW => ByRow(x -> x == "exports"), :DATE => ByRow(x -> x in t_pre)).PRODUCT_NC)
prod = intersect(prod_GB, prod_NL, prod_DE)
#   - products which are exported to both GB and NL in post-treatment period
prod_GB = unique(subset(df, :PARTNER_ISO => ByRow(x -> x == "Verenigd Koninkrijk"), :FLOW => ByRow(x -> x == "exports"), :DATE => ByRow(x -> x in t_post)).PRODUCT_NC)
prod_NL = unique(subset(df, :PARTNER_ISO => ByRow(x -> x == "Nederland"), :FLOW => ByRow(x -> x == "exports"), :DATE => ByRow(x -> x in t_post)).PRODUCT_NC)
prod_DE = unique(subset(df, :PARTNER_ISO => ByRow(x -> x == "Duitsland"), :FLOW => ByRow(x -> x == "exports"), :DATE => ByRow(x -> x in t_post)).PRODUCT_NC)
# products which are both exported to GB and NL and in both pre- and post-treatment period
prod = intersect(prod, prod_GB, prod_NL, prod_DE)

subset!(df, :PRODUCT_NC => ByRow(x -> x in prod))

# dummies
df.d_GB = ifelse.(df.PARTNER_ISO .== "Verenigd Koninkrijk", 1, 0)
transform!(df, :DATE => ByRow(x -> ifelse(x in t_post, 1, 0)) => :d_POST)
df.dummy = df.d_GB .* df.d_POST





using FixedEffectModels, RegressionTables

reg_VALUE = FixedEffectModels.reg(df, @formula(YOY_VALUE ~ dummy + fe(PARTNER_ISO) + fe(PRODUCT_NC)&fe(PERIOD)), Vcov.cluster(:PARTNER_ISO), save=true)
reg_QUANTITY = FixedEffectModels.reg(df, @formula(YOY_QUANTITY ~ dummy + fe(PARTNER_ISO) + fe(PRODUCT_NC)&fe(PERIOD)), Vcov.cluster(:PARTNER_ISO), save=true)
reg_PRICE = FixedEffectModels.reg(df, @formula(YOY_PRICE ~ dummy + fe(PARTNER_ISO) + fe(PRODUCT_NC)&fe(PERIOD)), Vcov.cluster(:PARTNER_ISO), save=true)


RegressionTables.regtable(reg_VALUE, reg_QUANTITY, reg_PRICE ; renderSettings = asciiOutput(), 
    regression_statistics=[:nobs, :r2], print_fe_section=true, estimformat="%0.4f")