# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script to compile data for some descriptive statistics
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics, StatsBase, StatsPlots, Dates, Pipe

# other scripts
dir_home = "x:/VIVES/1-Personal/Florian/git/2022_01_comext/src/"
include(dir_home * "functions.jl")

# location of data input/output (io)
dir_io = "C:/Users/u0148308/data/comext/" 
dir_dropbox = "C:/Users/u0148308/Dropbox/BREXIT/"



# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# VLAIO
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

df_VLAIO = CSV.read(dir_dropbox * "rawdata/" * "df_VLAIO" * ".csv", DataFrame)
transform!(df_VLAIO, ["PRODUCT_NC", "PRODUCT_NC_LAB1", "PRODUCT_NC_LAB2", "DECLARANT_ISO", "TRADE_TYPE", "PARTNER_ISO", "FLOW"] .=> ByRow(string), renamecols=false)
transform!(df_VLAIO, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> convert(Union{Missing, Float64}, x)), renamecols=false)
df_VLAIO.PRODUCT_NC = lpad.(string.(df_VLAIO.PRODUCT_NC), 8, '0') # needs to be done again

# remove missing, only for VALUES
#   - drop missing [VALUE, QUANTITY]: lose 1-16027346/21667553 ~26% observations
#   - drop missing VALUE: lose 1-21666543/21667553 ~ 0.1% observations
subset!(df_VLAIO, :VALUE_IN_EUROS .=> ByRow(x -> !ismissing(x)))

# not needed in case of VALUES!!
# MAD adjustment
#   - cutoff = 3: lose further 1-15177297/21666543 ~30% observations (~30% of total)
#   - cutoff = 4: lose further 1-15812254/21666543 ~27% observations (~27% of total)
# outlier_cutoff = 4
# gdf = groupby(df_VLAIO, "PRODUCT_NC")
# df_VLAIO = transform(gdf, :VALUE_IN_EUROS => MAD_method => :MAD)
# subset!(df_VLAIO, :MAD => ByRow(x -> x < outlier_cutoff))
# df_VLAIO = df_VLAIO[:, Not(:MAD)]


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
subset!(df_VLAIO, :VALUE_IN_EUROS => ByRow(x -> !ismissing(x))) # lose 0% observations

cols_subset = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD", "PRODUCT_NC", "VALUE_IN_EUROS", "QUANTITY_IN_KG"]
df_VLAIO = df_VLAIO[:, cols_subset]



# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 5
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# prepare data
df = copy(df_VLAIO)
partners = ["Verenigd Koninkrijk", "Duitsland", "Nederland", "Frankrijk", "Italië"]
subset!(df, :DECLARANT_ISO => ByRow(x -> x == "Vlaanderen"), :PARTNER_ISO => ByRow(x -> x in partners))

cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD"]
gdf = groupby(df, cols_grouping)
df = combine(gdf, nrow => :COUNT)

transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)
sort!(df)

# HP filter
λ = 20
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, :COUNT => (x -> HP(x, λ)) => :COUNT_HP)


for flow in ["imports", "exports"]

    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :COUNT,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="number of products", title="Flemish "* flow*": number of products")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/summary_stats/" * "fig5_" * flow * "_product_count" * ".png") # export image dropbox

    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :COUNT_HP,
    group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="number of products", title="Flemish "* flow*": number of products (HP, λ=$λ")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/summary_stats/" * "fig5_" * flow * "_product_count_HP" * ".png") # export image dropbox


end

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# comext
# -------------------------------------------------------------------------------------------------------------------------------------------------------------


# prepare data
df = copy(df_comext)
partners = ["GB", "DE", "NL", "FR", "IT"]
subset!(df, :DECLARANT_ISO => ByRow(x -> x == "BE"), :PARTNER_ISO => ByRow(x -> x in partners))
subset!(df, :PRODUCT_NC => ByRow(x -> x != "TOTAL"))


cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD"]
gdf = groupby(df, cols_grouping)
df = combine(gdf, nrow => :COUNT)

transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)
sort!(df)

# HP filter
λ = 20
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, :COUNT => (x -> HP(x, λ)) => :COUNT_HP)



for flow in ["imports", "exports"]

    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :COUNT,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="number of products", title="Belgian "* flow*": number of products")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/summary_stats/" * "fig5_BE_" * flow * "_product_count" * ".png") # export image dropbox

    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :COUNT_HP,
    group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="number of products", title="Belgian "* flow*": number of products (HP, λ=$λ")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/summary_stats/" * "fig5_BE_" * flow * "_product_count_HP" * ".png") # export image dropbox


end