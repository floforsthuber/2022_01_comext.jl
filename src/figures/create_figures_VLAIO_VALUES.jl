# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script to compile data for some descriptive statistics (VALUES)
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

using DataFrames, CSV, XLSX, LinearAlgebra, Statistics, StatsBase, StatsPlots, Dates, Pipe

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
# Figure 1
# -------------------------------------------------------------------------------------------------------------------------------------------------------------


# prepare data
df = copy(df_VLAIO)
partners = ["Verenigd Koninkrijk", "EU", "Duitsland", "Nederland", "Frankrijk", "Italië"]
subset!(df, :DECLARANT_ISO => ByRow(x -> x == "Vlaanderen"), :PARTNER_ISO => ByRow(x -> x in partners))
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD"]
gdf = groupby(df, cols_grouping)
df = combine(gdf, :VALUE_IN_EUROS => sum, renamecols=false)

# formatting
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)

# need to sort before computing HP and 3MMA
sort!(df)

# HP filter
λ = 20
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, :VALUE_IN_EUROS => (x -> HP(x, λ)) => :VALUE_IN_EUROS_HP)

# create 3MMA as observations are very volatile
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, :VALUE_IN_EUROS => (x -> movingaverage(x,3)) => :VALUE_3MMA)

# rolling standard deviation
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, [:VALUE_IN_EUROS, :VALUE_3MMA] .=> (x -> rolling_std(x, 6)) .=> [:STD_VALUE, :STD_VALUE_3MMA])

# plotting
#sort!(df, :DATE)

for flow in ["imports", "exports"]

    # values
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "EU")) plot(:DATE, :VALUE_IN_EUROS/1e9,
    group=:PARTNER_ISO, lw=2, legend=:topleft, ylabel="euros (billion)", title="Flemish "* flow*": values")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/summary_stats/" * "fig1_" * flow * "_values" * ".png") # export image dropbox

    # HP
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "EU")) plot(:DATE, :VALUE_IN_EUROS_HP/1e9,
            group=:PARTNER_ISO, lw=2, legend=:topleft, ylabel="euros (billion)", title="Flemish "*flow*": values (HP, λ=$λ)")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/summary_stats/" * "fig1_" * flow * "_values" * "_HP" * ".png") # export image dropbox

    # 3MMA
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "EU")) plot(:DATE, :VALUE_3MMA/1e9,
            group=:PARTNER_ISO, lw=2, legend=:topleft, ylabel="euros (billion)", title="Flemish "*flow*": values (3MMA)")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/summary_stats/" * "fig1_" * flow * "_values" * "_3MMA" * ".png") # export image dropbox

    # STD
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "EU")) plot(:DATE, :STD_VALUE/1e9,
            group=:PARTNER_ISO, lw=2, legend=:topleft, ylabel="euros (billion)", title="Flemish "*flow*": 6 months STD \n (values, rolling window)")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/summary_stats/" * "fig4_" * flow * "_values" * "_STD" * ".png") # export image dropbox


end


# STD
p = @df subset(df, :PARTNER_ISO => ByRow(x -> x == "Verenigd Koninkrijk")) plot(:DATE, :STD_VALUE/1e9,
        group=:FLOW, lw=2, legend=:bottomleft, ylabel="euros (billion)", title="Flemish: 6 months STD \n (values, rolling window)")
vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
savefig(p, dir_dropbox * "results/images/VLAIO/summary_stats/" * "fig4_" * "_values" * "_STD" * ".png") # export image dropbox


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 2
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# prepare data
df = copy(df_VLAIO)
partners = ["Verenigd Koninkrijk"]
subset!(df, :DECLARANT_ISO => ByRow(x -> x == "Vlaanderen"), :PARTNER_ISO => ByRow(x -> x in partners))
transform!(df, :PRODUCT_NC => ByRow(x -> x[1:2]) => :PRODUCT_NC_digits)

cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD", "PRODUCT_NC_digits"]
gdf = groupby(df, cols_grouping)
df = combine(gdf, :VALUE_IN_EUROS => sum, renamecols=false)

# add DATE
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)
sort!(df)

# HP filter
λ = 20
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PRODUCT_NC_digits"]
gdf = groupby(df, cols_grouping)
# df = combine(gdf, x -> nrow(x) < 3 ? DataFrame() : x) # remove groups with only two observations otherwise we cannot apply HP filter
# gdf = groupby(df, cols_grouping)
df = transform(gdf, :VALUE_IN_EUROS => (x -> HP(x, λ)) => :VALUE_IN_EUROS_HP)

# plotting
sort!(df)
products = 1:8:89

for flow in ["imports", "exports"]
    for i in products
    
    product_range = lpad.(i:i+6,2,'0')
    
    # HP
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :VALUE_IN_EUROS/1e6,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="euros (million)", title="Flemish "*flow*": values")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/summary_stats/products/values/" * flow * "/" * "fig2_" * flow * "_" * product_range[1] * "_" * product_range[end] * "_values" * ".png") # export image

    # HP
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :VALUE_IN_EUROS_HP/1e6,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="euros (million)", title="Flemish "*flow*": values (HP, λ=$λ)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/summary_stats/products/values/HP/" * flow * "/" * "fig2_" * flow * "_" * product_range[1] * "_" * product_range[end] * "_values" * "_HP" * ".png") # export image

    end
end



# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 3
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# prepare data
df = copy(df_VLAIO)
partners = ["Verenigd Koninkrijk", "EU", "Duitsland", "Nederland", "Frankrijk", "WORLD", "Italië"]
subset!(df, :DECLARANT_ISO => ByRow(x -> x == "Vlaanderen"), :PARTNER_ISO => ByRow(x -> x in partners))
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD"]
gdf = groupby(df, cols_grouping)
df = combine(gdf, :VALUE_IN_EUROS => sum, renamecols=false)

df_WORLD = subset(df, :PARTNER_ISO => ByRow(x -> x == "WORLD"))
cols_grouping = ["DECLARANT_ISO", "FLOW", "PERIOD"]
gdf = groupby(df_WORLD, cols_grouping)
df_WORLD = combine(gdf, :VALUE_IN_EUROS => sum => :VALUE_WORLD)

df = leftjoin(subset(df, :PARTNER_ISO => ByRow(x -> x != "WORLD")), df_WORLD, on=[:DECLARANT_ISO, :FLOW, :PERIOD])

transform!(df, [:VALUE_IN_EUROS, :VALUE_WORLD] => ByRow((x,s) -> x/s) => :SHARE_VALUE)

# Date
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)
sort!(df)

# HP filter
λ = 20
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, :SHARE_VALUE => (x -> HP(x, λ)) => :SHARE_VALUE_HP)

# plotting
partners = ["Verenigd Koninkrijk", "Duitsland", "Nederland", "Frankrijk", "Italië"]

for flow in ["imports", "exports"]

    # values
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x in partners)) plot(:DATE, :SHARE_VALUE*100,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="percentages", title="Flemish "* flow*" share (values)")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/summary_stats/" * "fig3_" * flow * "_value_share" * ".png") # export image dropbox

    # HP
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x in partners)) plot(:DATE, :SHARE_VALUE_HP*100,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="percentages", title="Flemish "* flow*" share (values, λ=$λ)")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/summary_stats/" * "fig3_" * flow * "_value_share" * "_HP" * ".png") # export image dropbox

end


# VLA vis-a-vis GB
p = @df subset(df, :PARTNER_ISO => ByRow(x -> x == "Verenigd Koninkrijk")) plot(:DATE, :SHARE_VALUE_HP*100,
        group=:FLOW, lw=2, legend=:bottomleft, ylabel="percentages", title="Total trade share (values, λ=$λ): \n Flanders vis-a-vis Great Britain")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
savefig(p, dir_dropbox * "results/images/VLAIO/summary_stats/" * "fig3_" * "VLA_GB" * "_value_share" * "_HP" * ".png") # export image


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

for flow in ["imports", "exports"]

    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :COUNT,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="number of products", title="Flemish "* flow*": number of products")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/summary_stats/" * "fig5_" * flow * "_product_count" * ".png") # export image dropbox

end


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Table 1
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

df = copy(df_VLAIO)
partners = ["Verenigd Koninkrijk", "EU", "Duitsland", "Nederland", "Frankrijk"]
subset!(df, :DECLARANT_ISO => ByRow(x -> x == "Vlaanderen"), :PARTNER_ISO => ByRow(x -> x in [partners; "WORLD"]))
transform!(df, :PRODUCT_NC => ByRow(x -> x[1:2]) => :PRODUCT_NC_digits)
transform!(df, :PERIOD => ByRow(x -> string(x)[1:4]) => :YEAR)

cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "YEAR", "PRODUCT_NC_digits"]
gdf = groupby(df, cols_grouping)
df = combine(gdf, :VALUE_IN_EUROS => sum, renamecols=false)

df_join = leftjoin(subset(df, :PARTNER_ISO => ByRow(x -> x != "WORLD")), 
                    subset(df, :PARTNER_ISO => ByRow(x -> x == "WORLD")), on=[:DECLARANT_ISO, :FLOW, :PRODUCT_NC_digits, :YEAR], makeunique=true)

rename!(df_join, [:VALUE_IN_EUROS, :VALUE_IN_EUROS_1] .=> [:VALUE_PARTNER, :VALUE_WORLD])              
transform!(df_join, [:VALUE_PARTNER, :VALUE_WORLD] => ByRow((x,s) -> x/s) => :SHARE_VALUE)

cols_name = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PRODUCT_NC_digits", "YEAR", "VALUE_PARTNER", "VALUE_WORLD", "SHARE_VALUE"]
df = df_join[:, cols_name]

# ----------------

df_tab1 = subset(df, :PARTNER_ISO => ByRow(x -> x == "Verenigd Koninkrijk"))
cols_name = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PRODUCT_NC_digits", "YEAR", "SHARE_VALUE"]
df_tab1 = df_tab1[:, cols_name]

sort!(df_tab1)
transform!(df_tab1, :SHARE_VALUE => ByRow(x -> round(x*100, digits=2)), renamecols=false)
df_tab1_wide = unstack(df_tab1, :YEAR, :SHARE_VALUE)
    
# XLSX.writetable(dir_dropbox * "results/images/VLAIO/summary_stats/" * "table1_within_prod_importance" * ".xlsx", df_tab1_wide, overwrite=true)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Table 2
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

df = copy(df_VLAIO)
partners = ["Verenigd Koninkrijk", "EU", "Duitsland", "Nederland", "Frankrijk"]
subset!(df, :DECLARANT_ISO => ByRow(x -> x == "Vlaanderen"), :PARTNER_ISO => ByRow(x -> x in [partners; "WORLD"]))
transform!(df, :PRODUCT_NC => ByRow(x -> x[1:2]) => :PRODUCT_NC_digits)
transform!(df, :PERIOD => ByRow(x -> string(x)[1:4]) => :YEAR)

cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "YEAR", "PRODUCT_NC_digits"]
gdf = groupby(df, cols_grouping)
df = combine(gdf, :VALUE_IN_EUROS => sum => :VALUE_PARTNER)

df_WORLD = subset(df, :PARTNER_ISO => ByRow(x -> x == "WORLD"))
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "YEAR"]
gdf = groupby(df_WORLD, cols_grouping)
df_WORLD = combine(gdf, :VALUE_PARTNER => sum => :VALUE_WORLD)

df_join = leftjoin(subset(df, :PARTNER_ISO => ByRow(x -> x != "WORLD")), df_WORLD[:,Not(:PARTNER_ISO)], on=[:DECLARANT_ISO, :FLOW, :YEAR])
transform!(df_join, [:VALUE_PARTNER, :VALUE_WORLD] => ByRow((x,s) -> x/s) => :SHARE_VALUE)

df_tab2 = subset(df_join, :PARTNER_ISO => ByRow(x -> x == "Verenigd Koninkrijk"))
cols_name = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PRODUCT_NC_digits", "YEAR", "SHARE_VALUE"]
df_tab2 = df_tab2[:, cols_name]

sort!(df_tab2)
transform!(df_tab2, :SHARE_VALUE => ByRow(x -> round(x*100, digits=2)), renamecols=false)

df_tab2_wide = unstack(df_tab2, :YEAR, :SHARE_VALUE)

# XLSX.writetable(dir_dropbox * "results/images/VLAIO/summary_stats/" * "table2_overall_prod_importance" * ".xlsx", df_tab2_wide, overwrite=true)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 6
# y-on-y monthly growth rates
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

df = copy(df_VLAIO)
partners = ["Verenigd Koninkrijk", "EU", "Duitsland", "Nederland", "Frankrijk", "Italië"]
subset!(df, :DECLARANT_ISO => ByRow(x -> x == "Vlaanderen"), :PARTNER_ISO => ByRow(x -> x in [partners; "WORLD"]))
transform!(df, :PRODUCT_NC => ByRow(x -> x[1:2]) => :PRODUCT_NC_digits)

# aggregate to two digits
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD", "PRODUCT_NC_digits"]
gdf = groupby(df, cols_grouping)
df = combine(gdf, :VALUE_IN_EUROS => sum, renamecols=false)


# YoY percentage change
#   - a simple percentage change X(t)-X(t-1)/X(t-1)
#   - computes percentage change if difference between PERIOD == 100 (201512 - 201412 = 100)
function yoy_change(period::AbstractVector, input::AbstractVector)
    M = [period[i]-period[j] == 100 ? (input[i]-input[j])/input[j] : missing for i in eachindex(input), j in eachindex(input)] # matrix
    V = [all(ismissing.(M[i,:])) ? missing : M[i, findfirst(typeof.(M[i,:]) .== Float64)] for i in 1:size(M, 1)] # reduce to vector
    return V
end

# compute YOY monthly percentage change
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "PRODUCT_NC_digits", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, [:PERIOD, :VALUE_IN_EUROS] => yoy_change => :YOY_VALUE)

# ------------

# MAD adjustment
#   - need to drop missing
outlier_cutoff = 4
gdf = groupby(subset(df, :YOY_VALUE => ByRow(x -> !ismissing(x))), "PRODUCT_NC_digits")
df = transform(gdf, :YOY_VALUE => MAD_method => :MAD)
subset!(df, :MAD => ByRow(x -> x < outlier_cutoff))
df = df[:, Not(:MAD)]

# ------------

# Date
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)
sort!(df)

# HP filter
λ = 20
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PRODUCT_NC_digits"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, :YOY_VALUE => (x -> HP(x, λ)) => :YOY_VALUE_HP)

# plotting
products = 1:8:89

for flow in ["imports", "exports"]
    for i in products
    
    product_range = lpad.(i:i+6,2,'0')
    
    # values
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range), :PARTNER_ISO => ByRow(x -> x == "Verenigd Koninkrijk")) plot(:DATE, :YOY_VALUE*100,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="percentages", title="Flemish "*flow*": monthly y-on-y change")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/summary_stats/products/yoy_change/" * flow * "/" * "fig6_" * flow * "_" * product_range[1] * "_" * product_range[end] * "_yoy_change" * ".png") # export image

    # HP
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range), :PARTNER_ISO => ByRow(x -> x == "Verenigd Koninkrijk")) plot(:DATE, :YOY_VALUE_HP*100,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="percentages", title="Flemish "*flow*": monthly y-on-y change \n (values, λ=$λ)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/summary_stats/products/yoy_change/HP/" * flow * "/" * "fig2_" * flow * "_" * product_range[1] * "_" * product_range[end] * "_yoy_change" * "_HP" * ".png") # export image

    end
end


# ------------
# standard deviation of y-on-y growth
# ------------

# table
referendum = Date(2016, 07, 01)
brexit = Date(2020, 02, 01)
trade = Date(2021, 05, 01)

transform!(df, :DATE => ByRow(x -> ifelse(x in referendum-Month(1)-Year(1):Month(1):referendum-Month(1), "pre",
    ifelse(x in referendum:Month(1):brexit-Month(1), "brexit_1", ifelse(x in brexit:Month(1):trade-Month(1), "brexit_2", 
        ifelse(x in trade:Month(1):trade+Month(10), "brexit_3", "no"))))) => :BREXIT)

cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "BREXIT"]
gdf = groupby(df, cols_grouping)
tab_std = combine(gdf, :YOY_VALUE => std => :STD_VALUE)

tab_std_wide = unstack(tab_std, :BREXIT, :STD_VALUE)
sort!(tab_std_wide, :FLOW)

# XLSX.writetable(dir_dropbox * "results/images/VLAIO/summary_stats/" * "table3_std" * ".xlsx", tab_std_wide, overwrite=true)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 7
# STD of y-on-y monthly growth rates
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

df = copy(df_VLAIO)
partners = ["Verenigd Koninkrijk", "Duitsland", "Nederland", "Frankrijk", "Italië"]
subset!(df, :DECLARANT_ISO => ByRow(x -> x == "Vlaanderen"), :PARTNER_ISO => ByRow(x -> x in partners))

# aggregate over products
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD"]
gdf = groupby(df, cols_grouping)
df = combine(gdf, :VALUE_IN_EUROS => sum, renamecols=false)

# compute YOY monthly percentage change
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, [:PERIOD, :VALUE_IN_EUROS] => yoy_change => :YOY_VALUE)

# # ------------

# # MAD adjustment
# #   - need to drop missing
# outlier_cutoff = 4
# gdf = groupby(subset(df, :YOY_VALUE => ByRow(x -> !ismissing(x))), "PRODUCT_NC_digits")
# df = transform(gdf, :YOY_VALUE => MAD_method => :MAD)
# subset!(df, :MAD => ByRow(x -> x < outlier_cutoff))
# df = df[:, Not(:MAD)]

# # ------------

# Date
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)
sort!(df)

# HP filter
subset!(df, :YOY_VALUE => ByRow(x -> !ismissing(x))) # need to drop missing
λ = 20
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, :YOY_VALUE => (x -> HP(x, λ)) => :YOY_VALUE_HP)

# std
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, [:YOY_VALUE, :YOY_VALUE_HP] .=> (x -> rolling_std(x, 6)) .=> [:STD_YOY, :STD_YOY_HP])


for flow in ["imports", "exports"]

    # YOY HP
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :STD_YOY_HP,
            group=:PARTNER_ISO, lw=2, legend=:topleft, ylabel="percentages", title="Flemish "*flow*" : 6 months STD \n (YOY change, rolling window, λ=$λ)")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/summary_stats/" * "fig7_" * flow * "_YOY" * "_STD" * ".png") # export image dropbox

end

