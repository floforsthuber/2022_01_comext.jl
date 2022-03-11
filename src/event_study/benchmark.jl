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
df_VLAIO.PRODUCT_NC = lpad.(string.(df_VLAIO.PRODUCT_NC), 8, '0') # needs to be done again
subset!(df_VLAIO, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> !ismissing(x))) # lose 1-16027346/21667553 ~26% observations

# MAD adjustment
outlier_cutoff = 3
transform!(df_VLAIO, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)
gdf = groupby(df_VLAIO, "PRODUCT_NC")
df_VLAIO = transform(gdf, :UNIT_PRICE => MAD_method => :MAD)
subset!(df_VLAIO, :MAD => ByRow(x -> x < outlier_cutoff)) # lose further 1-13110297/16027346 ~18% observations (~40% of total)
df_VLAIO = df_VLAIO[:, Not([:MAD, :UNIT_PRICE])]

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

transform!(df_VLAIO, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)
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

# compute YOY monthly log difference
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "PRODUCT_NC", "FLOW"]
gdf = groupby(df_VLAIO, cols_grouping)
df = transform(gdf, [:PERIOD, :VALUE_IN_EUROS] => yoy_change => :YOY_VALUE, [:PERIOD, :QUANTITY_IN_KG] => yoy_change => :YOY_QUANTITY,
            [:PERIOD, :UNIT_PRICE] => yoy_change => :YOY_PRICE)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# EXAMPLE with GB, NL, DE, FR

# DATE format for subsetting
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)

# drop missing 
subset!(df, :YOY_PRICE => ByRow(x -> !ismissing(x)))


# function to find common products 
function common_products(df::DataFrame, treated::String, control::Vector{String}, brexit_dates::Vector{Date}, flow::String, interval::Int64)

    prod = unique(df.PRODUCT_NC) # initialize

    for brexit in brexit_dates

        for ctry in [treated; control]

            # pre- and post-treatment time interval
            pre_treatment = brexit-Month(1)-Month(interval):Month(1):brexit-Month(1)
            post_treatment = brexit:Month(1):brexit+Month(interval)

            # products pre- and post-treatment
            prod_pre = unique(subset(df, :PARTNER_ISO => ByRow(x -> x == ctry), :FLOW => ByRow(x -> x == flow),
                :DATE => ByRow(x -> x in pre_treatment)).PRODUCT_NC)
            prod_post = unique(subset(df, :PARTNER_ISO => ByRow(x -> x == ctry), :FLOW => ByRow(x -> x == flow),
                :DATE => ByRow(x -> x in post_treatment)).PRODUCT_NC)

            # find and update intersection of product codes
            prod = intersect(prod, prod_pre, prod_post)

        end
    end

    n_prod = length(prod)
    println("\n ✓ $n_prod common products are traded between $treated and $control")

    return sort(prod)
end

# function to produce regression data
function data_reg(df::DataFrame, treated::String, control::Vector{String}, brexit::Date, flow::String, interval::Int64, products::Vector{String})

    # pre- and post-treatment timeframe
    pre_treatment = brexit-Month(1)-Month(interval):Month(1):brexit-Month(1)
    post_treatment = brexit:Month(1):brexit+Month(interval)

    # data for log change regression
    df_reg = subset(df, :PARTNER_ISO => ByRow(x -> x in [treated; control]), :FLOW => ByRow(x -> x == flow), 
                        :DATE => ByRow(x -> x in [pre_treatment; post_treatment]), :PRODUCT_NC => ByRow(x -> x in products))
    
    transform!(df_reg, :PARTNER_ISO => ByRow(x -> ifelse(x == treated, 1, 0)) => :d_TREATMENT) # dummy, treated country = 1
    transform!(df_reg, :DATE => ByRow(x -> ifelse(x in post_treatment, 1, 0)) => :d_POST) # dummy, post-treatment period = 1

    # data for standard deviation regression
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PRODUCT_NC", "d_POST"] # lose PERIOD
    gdf = groupby(df_reg, cols_grouping)
    df_reg_std = combine(gdf, [:YOY_VALUE, :YOY_QUANTITY, :YOY_PRICE] .=> std .=> [:STD_VALUE, :STD_QUANTITY, :STD_PRICE])
    subset!(df_reg_std, [:STD_VALUE, :STD_QUANTITY, :STD_PRICE] .=> ByRow(x -> !isnan(x))) # take out NaN (from std calculation)
    transform!(df_reg_std, :PARTNER_ISO => ByRow(x -> ifelse(x == treated, 1, 0)) => :d_TREATMENT) # dummy, treated country = 1


    return df_reg, df_reg_std

end


# ------------
# play with some partners

# eurozone = ["Griekenland", "Oostenrijk", "Duitsland", "Spanje", "Slovakije", "Italië", "Nederland", "Frankrijk", "Letland", "Cyprus", "Malta", "Litouwen",
#             "Slovenië", "Estland", "Portugal", "Finland", "Luxemburg", "Ierland", "België"]
# not_eurozone = ["Roemenië", "Polen", "Hongarije", "Kroatië", "Tsjechië", "Zweden", "Denemarken", "Bulgarije"]
# not_EU = ["Verenigde Staten", "Canada", "China", "Zwitserland", "Noorwegen", "Rusland"]

# partners = ["Nederland", "Duitsland", "Frankrijk"]
partners = ["Nederland", "Duitsland", "Frankrijk", "Italië"]
# partners = ["Nederland", "Duitsland", "Frankrijk", "Italië", "Verenigde Staten"]
# partners = copy(EU27)
# partners = copy(eurozone)
# partners = ["Nederland", "Duitsland", "Frankrijk", "Denemarken", "Verenigde Staten"]

# ------------
# Setup

# Brexit dates
referendum = Date(2016, 07, 01) # referendum to leave the EU, Date(2016, 06, 23)
exit = Date(2020, 02, 01) # formal exit of GB from the EU, Date(2020,01,31)
trade = Date(2021, 05, 01) # Trade and Cooperation Agreement becomes effective, Date(2021,04,27)
brexit_dates = [referendum; exit; trade]

treated = "Verenigd Koninkrijk" # treated group
control = copy(partners) # control group
flow = "exports"
interval = 12 # 12 months before and after brexit scenarios

# # placebo
# treated = "Italië" # treated group
# control = ["Duitsland", "Nederland", "Frankrijk"] # control group
# flow = "exports"
# interval = 12 # 12 months before and after brexit scenarios


prod_common = common_products(df, treated, control, brexit_dates, flow, interval)

df_reg, df_reg_std = data_reg(df, treated, control, referendum, flow, interval, prod_common)


# regression figures
include(dir_home * "event_study/" * "functions_figures.jl")
df_fig, folder = figures_reg(df_VLAIO, treated, control, brexit_dates, interval, prod_common) # use df_VLAIO to extend time period
figure_1(df_fig, folder, treated)
figure_3(df_fig, folder, treated)
figure_5(df_fig, folder)
figure_7(df_fig, folder)
table_3(df_fig, folder, brexit_dates)



# ------------
# Regression

using FixedEffectModels, RegressionTables

# YOY monthly log difference
reg_VALUE = FixedEffectModels.reg(df_reg, @formula(YOY_VALUE ~ d_TREATMENT&d_POST + fe(PARTNER_ISO) + fe(PRODUCT_NC)&fe(PERIOD)), Vcov.cluster(:PARTNER_ISO), save=true)
reg_QUANTITY = FixedEffectModels.reg(df_reg, @formula(YOY_QUANTITY ~ d_TREATMENT&d_POST + fe(PARTNER_ISO) + fe(PRODUCT_NC)&fe(PERIOD)), Vcov.cluster(:PARTNER_ISO), save=true)
reg_PRICE = FixedEffectModels.reg(df_reg, @formula(YOY_PRICE ~ d_TREATMENT&d_POST + fe(PARTNER_ISO) + fe(PRODUCT_NC)&fe(PERIOD)), Vcov.cluster(:PARTNER_ISO), save=true)

RegressionTables.regtable(reg_VALUE, reg_QUANTITY, reg_PRICE ; renderSettings = asciiOutput(), 
    regression_statistics=[:nobs, :r2], print_fe_section=true, estimformat="%0.4f")


# STD of YOY monthly log difference
#   - lose PERIOD dimension (just control for PRODUCT FE instead)
reg_STD_VALUE = FixedEffectModels.reg(df_reg_std, @formula(STD_VALUE ~ d_TREATMENT&d_POST + fe(PARTNER_ISO) + fe(PRODUCT_NC)), Vcov.cluster(:PARTNER_ISO), save=true)
reg_STD_QUANTITY = FixedEffectModels.reg(df_reg_std, @formula(STD_QUANTITY ~ d_TREATMENT&d_POST + fe(PARTNER_ISO) + fe(PRODUCT_NC)), Vcov.cluster(:PARTNER_ISO), save=true)
reg_STD_PRICE = FixedEffectModels.reg(df_reg_std, @formula(STD_PRICE ~ d_TREATMENT&d_POST + fe(PARTNER_ISO) + fe(PRODUCT_NC)), Vcov.cluster(:PARTNER_ISO), save=true)

RegressionTables.regtable(reg_STD_VALUE, reg_STD_QUANTITY, reg_STD_PRICE ; renderSettings = asciiOutput(), 
    regression_statistics=[:nobs, :r2], print_fe_section=true, estimformat="%0.4f")


