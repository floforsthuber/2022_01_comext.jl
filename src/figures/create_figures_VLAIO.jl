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

cols_subset = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD", "PRODUCT_NC", "VALUE_IN_EUROS", "QUANTITY_IN_KG"]
df_VLAIO = df_VLAIO[:, cols_subset]
sort!(df_VLAIO, :PERIOD)


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
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "EU")) plot(:DATE, :VALUE_IN_EUROS/1e9,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="euros (billion)", title="Flemish "* flow*": values")
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
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "EU")) plot(:DATE, :VALUE_IN_EUROS_HP/1e9,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="euros (billion)", title="Flemish "*flow*": values (HP, λ=$λ)")
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
df = combine(gdf, x -> nrow(x) < 3 ? DataFrame() : x) # remove groups with only two observations otherwise we cannot apply HP filter
gdf = groupby(df, cols_grouping)
df = transform(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG, :PRICE_INDEX] .=> (x -> HP(x, λ)) .=> [:VALUE_IN_EUROS_HP, :QUANTITY_IN_KG_HP, :PRICE_INDEX_HP])

# create unweighted unit prices
transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)
transform!(df, [:VALUE_IN_EUROS_HP, :QUANTITY_IN_KG_HP] => ByRow((v,q) -> v/q) => :UNIT_PRICE_HP)

# plotting
# subsetting products for figures
sort!(df) # for some reason needed to have correct DATE axis
products = 1:8:89

for flow in ["imports", "exports"]
    for i in products
    
    product_range = lpad.(i:i+6,2,'0')
    
    # values
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :VALUE_IN_EUROS,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="euros", title="Flemish "*flow*": values")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig2/values/" * "fig2_" * flow * "_" * product_range[1] * "_" * product_range[end] * "_values" * ".png") # export image

    # price index (weighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :PRICE_INDEX,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="unit prices (weighted)", title="Flemish "*flow*": unit prices (weighted)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig2/prices_weighted/" * "fig2_" * flow * "_" * product_range[1] * "_" * product_range[end] * "_prices_weighted" * ".png")

    # price index (unweighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :UNIT_PRICE,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="unit prices (unweighted)", title="Flemish "*flow*": unit prices (unweighted)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig2/prices_unweighted/" * "fig2_" * flow * "_" * product_range[1] * "_" * product_range[end] * "_prices_unweighted" * ".png") 

    end
end

for flow in ["imports", "exports"]
    for i in products
    
    product_range = lpad.(i:i+6,2,'0')
    
    # values
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :VALUE_IN_EUROS_HP/1e6,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="euros (million)", title="Flemish "*flow*": values (HP, λ=$λ)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig2/HP/values/" * flow * "/" * "fig2_" * flow * "_" * product_range[1] * "_" * product_range[end] * "_values" * "_HP" * ".png") # export image

    # price index (weighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :PRICE_INDEX_HP,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="unit prices (weighted)", title="Flemish "*flow*": unit prices (weighted, λ=$λ)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig2/HP/prices_weighted/" * "fig2_" * flow * "_" * product_range[1] * "_" * product_range[end] * "_prices_weighted" * "_HP" * ".png")

    # price index (unweighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :UNIT_PRICE_HP,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="unit prices (unweighted)", title="Flemish "*flow*": unit prices (unweighted, λ=$λ)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig2/HP/prices_unweighted/" * "fig2_" * flow * "_" * product_range[1] * "_" * product_range[end] * "_prices_unweighted" * "_HP" * ".png") 

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

# function to create data for figure 3
function data_fig3(df::DataFrame, declarants::Vector{String}, partners::Vector{String})
    
    # clean df
    df = subset(df, :PRODUCT_NC => ByRow(x -> x != "TOTAL"), :DECLARANT_ISO => ByRow(x -> x in declarants),
                    :PARTNER_ISO => ByRow(x -> x in [partners; "WORLD"])) # take out TOTAL

    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD"]
    gdf = groupby(df, cols_grouping)
    df = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> sum, renamecols=false)

    df_join = leftjoin(subset(df, :PARTNER_ISO => ByRow(x -> x != "WORLD")), 
                    subset(df, :PARTNER_ISO => ByRow(x -> x == "WORLD")), on=[:DECLARANT_ISO, :FLOW, :PERIOD], makeunique=true)

    transform!(df_join, [:VALUE_IN_EUROS, :VALUE_IN_EUROS_1] => ByRow((x,s) -> x/s*100) => :VALUE_SHARE)
    transform!(df_join, [:QUANTITY_IN_KG, :QUANTITY_IN_KG_1] => ByRow((x,s) -> x/s*100) => :QUANTITY_SHARE)

    cols_name = ["VALUE_PARTNER", "QUANTITY_PARTNER", "VALUE_WORLD", "QUANTITY_WORLD"]
    rename!(df_join, [:VALUE_IN_EUROS, :QUANTITY_IN_KG, :VALUE_IN_EUROS_1, :QUANTITY_IN_KG_1] .=> cols_name)

    return df_join
end

df = data_fig3(df_VLAIO, ["Vlaanderen"], partners)


# ------------------

# formatting
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)

# HP filter
λ = 20
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


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Table 1
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

partners = ["Verenigd Koninkrijk", "EU", "Duitsland", "Nederland", "Frankrijk"]
df = tab1(df_VLAIO, ["Vlaanderen"], partners, 2)

# aggregate to yearly data
transform!(df, :PERIOD => ByRow(x -> string(x)[1:4]) => :YEAR)
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PRODUCT_NC_digits", "YEAR"]
gdf = groupby(df, cols_grouping)
df = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> sum, renamecols=false)

df_join = leftjoin(subset(df, :PARTNER_ISO => ByRow(x -> x != "WORLD")), 
                    subset(df, :PARTNER_ISO => ByRow(x -> x == "WORLD")), on=[:DECLARANT_ISO, :FLOW, :PRODUCT_NC_digits, :YEAR], makeunique=true)

transform!(df_join, [:VALUE_IN_EUROS, :VALUE_IN_EUROS_1] => ByRow((x,s) -> x/s) => :SHARE_VALUE)
transform!(df_join, [:QUANTITY_IN_KG, :QUANTITY_IN_KG_1] => ByRow((x,s) -> x/s) => :SHARE_QUANTITY)

cols_name = ["VALUE_PARTNER", "QUANTITY_PARTNER", "VALUE_WORLD", "QUANTITY_WORLD"]
rename!(df_join, [:VALUE_IN_EUROS, :QUANTITY_IN_KG, :VALUE_IN_EUROS_1, :QUANTITY_IN_KG_1] .=> cols_name)
cols_name = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PRODUCT_NC_digits", "YEAR", "VALUE_PARTNER", "QUANTITY_PARTNER", "VALUE_WORLD", "QUANTITY_WORLD", "SHARE_VALUE", "SHARE_QUANTITY"]
df = df_join[:, cols_name]

# ----------------

df_tab1 = subset(df, :PARTNER_ISO => ByRow(x -> x == "Verenigd Koninkrijk"))
cols_name = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PRODUCT_NC_digits", "YEAR", "SHARE_VALUE"]
df_tab1 = df_tab1[:, cols_name]

# # only keep top 10
# df_tab1 = @pipe df_tab1 |>
#     groupby(_, cols_grouping) |>
#     combine(_) do sdf
#         sorted = sort(sdf, order(:SHARE_VALUE, rev=true))
#         first(sorted, 10)
#     end

sort!(df_tab1)
transform!(df_tab1, :SHARE_VALUE => ByRow(x -> round(x*100, digits=2)), renamecols=false)
df_tab1_wide = unstack(df_tab1, :YEAR, :SHARE_VALUE)
    
XLSX.writetable(dir_dropbox * "results/images/VLAIO/tab/" * "table1_within_prod_importance" * ".xlsx", df_tab1_wide, overwrite=true)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Table 2
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

partners = ["Verenigd Koninkrijk", "EU", "Duitsland", "Nederland", "Frankrijk"]
df = tab1(df_VLAIO, ["Vlaanderen"], partners, 2)

# aggregate to yearly data
transform!(df, :PERIOD => ByRow(x -> string(x)[1:4]) => :YEAR)
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PRODUCT_NC_digits", "YEAR"]
gdf = groupby(df, cols_grouping)
df = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> sum, renamecols=false)

# create total world exports/imports per year
df_WORLD = subset(df, :PARTNER_ISO => ByRow(x -> x == "WORLD"))
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "YEAR"]
gdf = groupby(df_WORLD, cols_grouping)
df_WORLD = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> sum .=> [:VALUE_WORLD, :QUANTITY_WORLD])

df_join = leftjoin(subset(df, :PARTNER_ISO => ByRow(x -> x != "WORLD")), df_WORLD[:,Not(:PARTNER_ISO)], on=[:DECLARANT_ISO, :FLOW, :YEAR])

transform!(df_join, [:VALUE_IN_EUROS, :VALUE_WORLD] => ByRow((x,s) -> x/s) => :SHARE_VALUE)
transform!(df_join, [:QUANTITY_IN_KG, :QUANTITY_WORLD] => ByRow((x,s) -> x/s) => :SHARE_QUANTITY)



# ----------------

df_tab2 = subset(df_join, :PARTNER_ISO => ByRow(x -> x == "Verenigd Koninkrijk"))
cols_name = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PRODUCT_NC_digits", "YEAR", "SHARE_VALUE"]
df_tab2 = df_tab2[:, cols_name]

# # only keep top 10
# df_tab2 = @pipe df_tab2 |>
#     groupby(_, cols_grouping) |>
#     combine(_) do sdf
#         sorted = sort(sdf, order(:SHARE_VALUE, rev=true))
#         first(sorted, 10)
#     end

sort!(df_tab2)
transform!(df_tab2, :SHARE_VALUE => ByRow(x -> round(x*100, digits=2)), renamecols=false)

df_tab2_wide = unstack(df_tab2, :YEAR, :SHARE_VALUE)

XLSX.writetable(dir_dropbox * "results/images/VLAIO/tab/" * "table2_overall_prod_importance" * ".xlsx", df_tab2_wide, overwrite=true)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 5
# -------------------------------------------------------------------------------------------------------------------------------------------------------------


# function to create data for figure 5
function fig5(df::DataFrame, declarants::Vector{String}, partners::Vector{String})

    # clean df
    subset!(df, :DECLARANT_ISO => ByRow(x -> x in declarants), :PARTNER_ISO => ByRow(x -> x in partners))
    subset!(df, :PRODUCT_NC => ByRow(x -> x != "TOTAL")) # take out TOTAL

    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD"]
    gdf = groupby(df, cols_grouping)
    df = combine(gdf, nrow => :COUNT)

    return df
end


partners = ["Verenigd Koninkrijk", "Duitsland", "Nederland", "Frankrijk"]
df = fig5(df_VLAIO, ["Vlaanderen"], partners)
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)


for flow in ["imports", "exports"]

    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :COUNT,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="number of products", title="Flemish "* flow*": number of products")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/VLAIO/fig5/" * "fig5_" * flow * "_product_count" * ".png") # export image dropbox

end