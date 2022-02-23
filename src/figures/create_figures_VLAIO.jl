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

# take out EU/WORLD again for MAD adjustment
subset!(df_VLAIO, :PARTNER_ISO => ByRow(x -> !(x in ["EU", "WORLD"])))
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
# Figure 1
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# # prepare data
# df = transform(df_VLAIO, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> ifelse(iszero(x) | ismissing(x), missing, x)), renamecols=false) # need to remove missing also from EU
# subset!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> !ismissing(x))) # lose 1-18211714/24661479 ~26% observations

# # remove outliers based on medians abs dev.
# outlier_cutoff = 3
# transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)
# gdf = groupby(df, "PRODUCT_NC")
# df = transform(gdf, :UNIT_PRICE => MAD_method => :MAD)
# subset!(df, :MAD => ByRow(x -> x < outlier_cutoff)) # lose further 1-14861374/18211714 ~18% observations (~40% of total)

# produce data for plotting
partners = ["Verenigd Koninkrijk", "EU", "Duitsland", "Nederland", "Frankrijk"]
df = data_fig1(df_VLAIO, ["Vlaanderen"], partners)

# -----------------------

# formatting
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)

# HP filter
λ = 20
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG, :PRICE_INDEX] .=> (x -> HP(x, λ)) .=> [:VALUE_IN_EUROS_HP, :QUANTITY_IN_KG_HP, :PRICE_INDEX_HP])

# create unweighted unit prices
transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)
transform!(df, [:VALUE_IN_EUROS_HP, :QUANTITY_IN_KG_HP] => ByRow((v,q) -> v/q) => :UNIT_PRICE_HP)

# create 3MMA as observations are very volatile
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, :PRICE_INDEX => (x -> movingaverage(x,3)) => :PRICE_INDEX_3MMA)


# plotting
sort!(df, :DATE)

for flow in ["imports", "exports"]

    # values
    # Notes:
    #   - take out EU for scale
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "EU")) plot(:DATE, :VALUE_IN_EUROS,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="euros", title="Flemish "* flow*": values")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig1/" * "fig1_" * flow * "_values" * ".png") # export image dropbox

    # price index (weighted unit prices)
    # Notes:
    #   - unit prices are extremely volatile, 3 MMA would be useful
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :PRICE_INDEX,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="price index (weighted)", title="Flemish "*flow*": price index (weighted)")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig1/" * "fig1_" * flow * "_prices_weighted" * ".png")

    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :PRICE_INDEX_3MMA,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="price index (weighted, 3MMA)", title="Flemish "*flow*": price index (weighted, 3MMA)")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig1/" * "fig1_" * flow * "_prices_weighted_3MMA" * ".png") 

    # price index (unweighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :UNIT_PRICE,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="price index (unweighted)", title="Flemish "*flow*": price index (unweighted)")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig1/" * "fig1_" * flow * "_prices_unweighted" * ".png") 

end

# plotting of HP
for flow in ["imports", "exports"]

    # values
    # Notes:
    #   - take out EU for scale
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "EU")) plot(:DATE, :VALUE_IN_EUROS_HP,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="euros", title="Flemish "*flow*": values (HP, λ=$λ)")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig1/HP/" * "fig1_" * flow * "_values" * "_HP" * ".png") # export image dropbox

    # price index (weighted unit prices)
    # Notes:
    #   - unit prices are extremely volatile, 3 MMA would be useful
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :PRICE_INDEX_HP,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="price index (weighted)", title="Flemish "*flow*": price index (weighted, λ=$λ)")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig1/HP/" * "fig1_" * flow * "_prices_weighted" * "_HP" * ".png")

    # price index (unweighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :UNIT_PRICE_HP,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="price index (unweighted)", title="Flemish "*flow*": price index (unweighted, λ=$λ)")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig1/HP/" * "fig1_" * flow * "_prices_unweighted" * "_HP" * ".png") 

end


# VLA vis-a-vis GB
p = @df subset(df, :PARTNER_ISO => ByRow(x -> x == "Verenigd Koninkrijk")) plot(:DATE, :UNIT_PRICE_HP,
        group=:FLOW, lw=2, legend=:topleft, ylabel="euros", title="Price index (unweighted, λ=$λ): \n Flanders vis-a-vis Great Britain")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
savefig(p, dir_dropbox * "results/images/VLAIO/fig1/HP/" * "fig1_" * "VLA_GB" * "_prices_unweighted" * "_HP" * ".png") # export image


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 4
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# rolling standard deviation
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG, :PRICE_INDEX, :UNIT_PRICE, :VALUE_IN_EUROS_HP, :QUANTITY_IN_KG_HP, :PRICE_INDEX_HP, :UNIT_PRICE_HP] 
                .=> (x -> rolling_std(x, 6)) .=> 
                [:STD_VALUE, :STD_QUANTITY, :STD_PRICE, :STD_UNIT, :STD_VALUE_HP, :STD_QUANTITY_HP, :STD_PRICE_HP, :STD_UNIT_HP])


# plotting HP
for flow in ["imports", "exports"]

    # values
    # Notes:
    #   - take out EU for scale
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "EU")) plot(:DATE, :STD_UNIT_HP,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="euros", title="Flemish "* flow*": std unit prices (unweighted, λ=$λ)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig4/HP/" * "fig4_" * flow * "_std" * "_prices_unweighted" * "_HP" * ".png") # export image dropbox

end


# VLA vis-a-vis GB
p = @df subset(df, :PARTNER_ISO => ByRow(x -> x == "Verenigd Koninkrijk")) plot(:DATE, :STD_UNIT_HP,
        group=:FLOW, lw=2, legend=:topleft, ylabel="euros", title="6 months STD price index (unweighted, λ=$λ): \n Flanders vis-a-vis Great Britain")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
savefig(p, dir_dropbox * "results/images/VLAIO/fig4/HP/" * "fig4_" * "VLA_GB" * "_STD_prices_unweighted" * "_HP" * ".png") # export image



# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 2
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# # prepare data
# df = transform(df_VLAIO, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> ifelse(iszero(x) | ismissing(x), missing, x)), renamecols=false) # need to remove missing also from EU
# subset!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> !ismissing(x))) # lose 1-18211714/24661479 ~26% observations

# # remove outliers based on medians abs dev.
# outlier_cutoff = 3
# transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)
# gdf = groupby(df, "PRODUCT_NC")
# df = transform(gdf, :UNIT_PRICE => MAD_method => :MAD)
# subset!(df, :MAD => ByRow(x -> x < outlier_cutoff)) # lose further 1-14861374/18211714 ~18% observations (~40% of total)

# produce data for plotting
df = data_fig2(df_VLAIO, ["Vlaanderen"], ["Verenigd Koninkrijk"], 2)

# ------------------

# formatting
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)

# HP filter
λ = 20
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PRODUCT_NC_digits"]
gdf = groupby(df, cols_grouping)
transform!(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG, :PRICE_INDEX] .=> (x -> HP(x, λ)) .=> [:VALUE_IN_EUROS_HP, :QUANTITY_IN_KG_HP, :PRICE_INDEX_HP])

# create unweighted unit prices
transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)
transform!(df, [:VALUE_IN_EUROS_HP, :QUANTITY_IN_KG_HP] => ByRow((v,q) -> v/q) => :UNIT_PRICE_HP)

# plotting
# subsetting products for figures
sort!(df, :DATE) # for some reason needed to have correct DATE axis
products = 7:8:89

for flow in ["imports", "exports"]
    for i in products
    
    product_range = string.(i:i+7)
    end_range = string(product_range[end])
    
    # values
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :VALUE_IN_EUROS,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="euros", title="Flemish "*flow*": values")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig2/values/" * "fig2_" * flow * "_" * end_range * "_values" * ".png") # export image

    # price index (weighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :PRICE_INDEX,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="unit prices (weighted)", title="Flemish "*flow*": unit prices (weighted)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig2/prices_weighted/" * "fig2_" * flow * "_" * end_range * "_prices_weighted" * ".png")

    # price index (unweighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :UNIT_PRICE,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="unit prices (unweighted)", title="Flemish "*flow*": unit prices (unweighted)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig2/prices_unweighted/" * "fig2_" * flow * "_" * end_range * "_prices_unweighted" * ".png") 

    end
end

for flow in ["imports", "exports"]
    for i in products
    
    product_range = string.(i:i+7)
    end_range = string(product_range[end])
    
    # values
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :VALUE_IN_EUROS_HP,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="euros", title="Flemish "*flow*": values (HP, λ=$λ)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig2/HP/values/" * "fig2_" * flow * "_" * end_range * "_values" * "_HP" * ".png") # export image

    # price index (weighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :PRICE_INDEX_HP,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="unit prices (weighted)", title="Flemish "*flow*": unit prices (weighted, λ=$λ)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig2/HP/prices_weighted/" * "fig2_" * flow * "_" * end_range * "_prices_weighted" * "_HP" * ".png")

    # price index (unweighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :UNIT_PRICE_HP,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="unit prices (unweighted)", title="Flemish "*flow*": unit prices (unweighted, λ=$λ)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig2/HP/prices_unweighted/" * "fig2_" * flow * "_" * end_range * "_prices_unweighted" * "_HP" * ".png") 

    end
end



# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 3
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# # prepare data
# df = transform(df_VLAIO, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> ifelse(iszero(x) | ismissing(x), missing, x)), renamecols=false) # need to remove missing also from EU
# subset!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> !ismissing(x))) # lose 1-18211714/24661479 ~26% observations

# # remove outliers based on medians abs dev.
# outlier_cutoff = 3
# transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)
# gdf = groupby(df, "PRODUCT_NC")
# df = transform(gdf, :UNIT_PRICE => MAD_method => :MAD)
# subset!(df, :MAD => ByRow(x -> x < outlier_cutoff)) # lose further 1-14861374/18211714 ~18% observations (~40% of total)

# produce data for plotting
partners = ["Verenigd Koninkrijk", "EU", "Duitsland", "Nederland", "Frankrijk"]
df = data_fig3(df_VLAIO, ["Vlaanderen"], partners)

# ------------------

# formatting
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)

# HP filter
λ = 10
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
transform!(gdf, [:VALUE_SHARE, :QUANTITY_SHARE] .=> (x -> HP(x, λ)) .=> [:VALUE_SHARE_HP, :QUANTITY_SHARE_HP])

# plotting
sort!(df, :DATE)

for flow in ["imports", "exports"]

    # value share
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> !(x == "EU"))) plot(:DATE, :VALUE_SHARE,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="percentages", title="Flemish "*flow*": value share")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig3/" * "fig3_" * flow * "_value_share" * ".png") # export image

    # quantity share
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> !(x == "EU"))) plot(:DATE, :QUANTITY_SHARE,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="kg", title="Flemish "*flow*": quantity share")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig3/" * "fig3_" * flow * "_quantity_share" * ".png") # export image

end

# plotting HP
for flow in ["imports", "exports"]

    # value share
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> !(x == "EU"))) plot(:DATE, :VALUE_SHARE_HP,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="percentages", title="Flemish "*flow*": value share (HP, λ=$λ)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig3/HP/" * "fig3_" * flow * "_value_share" * "_HP" * ".png") # export image

    # quantity share
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> !(x == "EU"))) plot(:DATE, :QUANTITY_SHARE_HP,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="kg", title="Flemish "*flow*": quantity share (HP, λ=$λ)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig3/HP/" * "fig3_" * flow * "_quantity_share" * "_HP" * ".png") # export image

end

# VLA vis-a-vis GB
p = @df subset(df, :PARTNER_ISO => ByRow(x -> x == "Verenigd Koninkrijk")) plot(:DATE, :VALUE_SHARE_HP,
        group=:FLOW, lw=2, legend=:bottomleft, ylabel="percentages", title="Total trade share (values, λ=$λ): \n Flanders vis-a-vis Great Britain")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
savefig(p, dir_dropbox * "results/images/VLAIO/fig3/HP/" * "fig3_" * "VLA_GB" * "_value_share" * "_HP" * ".png") # export image
