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

# Notes:
#   - figure 1: evolution of total imports/exports from BE to UK vis-a-vis FR, NL, DE and EU
#       + HP filter available, what do chose for λ?
#   - figure 2: evolution of imports/exports from BE to UK at 2digit level
#       + HP filter available, what do chose for λ?
#   - figure 3: evolution of share of imports/exports from BE to UK of total imports/exports from BE
#       + also add FR, DE, NL, EU27
#   - table 1: evolution of most important products at 2digit level
#       + shares per product available for bilteral trade (i.e. sum to 100%) or for total trade (i.e. sum to figure 3)
#       + could compute figure 3 from data of table 1 as well!
#
#   - all figures in values and unit values (unit prices):
#       + compute a price index as using a weighted sum
#   - include vertical lines for different Brexit events

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Brexit events
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# vertical line
# vline!([Date(2016,6,23)], label="", color=:black, lw=2) # referendum
# vline!([Date(2016,12,07)], label="") # vote to trigger Article 50
# vline!([Date(2017,03,29)], label="") # Article 50
# vline!([Date(2017,06,19)], label="") # negotiations commence
# vline!([Date(2019,03,21)], label="") # first extension of Article 50
# vline!([Date(2019,04,10)], label="") # second extension of Article 50
# vline!([Date(2019,10,28)], label="") # third extension of Article 50
# vline!([Date(2019,12,20)], label="") # withdrawl bill passes UK
# vline!([Date(2020,01,31)], label="") # exit of UK from EU
# vline!([Date(2020,12,20)], label="") # UK passes Trade and Cooperation Agreement
# vline!([Date(2020,12,31)], label="") # transition period ends
# vline!([Date(2021,04,27)], label="") # EU passes Trade and Cooperation Agreement


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Raw data
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

df_comext = CSV.read(dir_dropbox * "rawdata/" * "df_comext_BE" * ".csv", DataFrame)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 1
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# prepare data
df = transform(df_comext, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> ifelse(iszero(x) | ismissing(x), missing, x)), renamecols=false) # need to remove missing also from EU
subset!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> !ismissing(x)))

# remove outliers based on medians abs dev.
# Notes:
#   - where to put MAD (need to drop EU/WORLD for sure)
outlier_cutoff = 3
transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)
gdf = groupby(df, "PRODUCT_NC")
df = transform(gdf, :UNIT_PRICE => MAD_method => :MAD)
subset!(df, :MAD => ByRow(x -> x < outlier_cutoff)) # lose further 1-42083391/50055769 ~16% observations

# produce data for plotting
df = data_fig1(df, ["BE"], ["GB", "EU", "DE", "FR", "NL"])

# -----------------------

# formatting
transform!(df, [:DECLARANT_ISO, :PARTNER_ISO, :FLOW] .=> ByRow(string), renamecols=false)
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)

# HP filter
λ = 20
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
transform!(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG, :PRICE_INDEX] .=> (x -> HP(x, λ)) .=> [:VALUE_IN_EUROS_HP, :QUANTITY_IN_KG_HP, :PRICE_INDEX_HP])

# create unweighted unit prices
transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)
transform!(df, [:VALUE_IN_EUROS_HP, :QUANTITY_IN_KG_HP] => ByRow((v,q) -> v/q) => :UNIT_PRICE_HP)

# create 3MMA as observations are very volatile
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, :PRICE_INDEX => (x -> movingaverage(x,3)) => :PRICE_INDEX_3MMA)


# plotting
for flow in ["imports", "exports"]

    # values
    # Notes:
    #   - take out EU for scale
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "EU")) plot(:DATE, :VALUE_IN_EUROS,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="euros", title="Belgian "* flow*": values")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/comext/fig1/" * "fig1_" * flow * "_values" * ".png") # export image dropbox

    # price index (weighted unit prices)
    # Notes:
    #   - unit prices are extremely volatile, 3 MMA would be useful
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :PRICE_INDEX,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="price index (weighted)", title="Belgian "*flow*": price index (weighted)")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/comext/fig1/" * "fig1_" * flow * "_prices_weighted" * ".png")

    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :PRICE_INDEX_3MMA,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="price index (weighted, 3MMA)", title="Belgian "*flow*": price index (weighted, 3MMA)")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/comext/fig1/" * "fig1_" * flow * "_prices_weighted_3MMA" * ".png") 

    # price index (unweighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :UNIT_PRICE,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="price index (unweighted)", title="Belgian "*flow*": price index (unweighted)")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/comext/fig1/" * "fig1_" * flow * "_prices_unweighted" * ".png") 

end

# plotting of HP
for flow in ["imports", "exports"]

    # values
    # Notes:
    #   - take out EU for scale
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "EU")) plot(:DATE, :VALUE_IN_EUROS_HP,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="euros", title="Belgian "*flow*": values (HP, λ=$λ)")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/comext/fig1/HP/" * "fig1_" * flow * "_values" * "_HP" * ".png") # export image dropbox

    # price index (weighted unit prices)
    # Notes:
    #   - unit prices are extremely volatile, 3 MMA would be useful
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :PRICE_INDEX_HP,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="price index (weighted)", title="Belgian "*flow*": price index (weighted, λ=$λ)")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/comext/fig1/HP/" * "fig1_" * flow * "_prices_weighted" * "_HP" * ".png")

    # price index (unweighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :UNIT_PRICE_HP,
        group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="price index (unweighted)", title="Belgian "*flow*": price index (unweighted, λ=$λ)")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/comext/fig1/HP/" * "fig1_" * flow * "_prices_unweighted" * "_HP" * ".png") 

end


# BE vis-a-vis GB
p = @df subset(df, :PARTNER_ISO => ByRow(x -> x == "GB")) plot(:DATE, :UNIT_PRICE_HP,
        group=:FLOW, lw=2, legend=:topleft, ylabel="euros", title="Price index (unweighted, λ=$λ): \n Belgium vis-a-vis Great Britain")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
savefig(p, dir_dropbox * "results/images/comext/fig1/HP/" * "fig1_" * "BE_GB" * "_prices_unweighted" * "_HP" * ".png") # export image


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
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="euros", title="Belgian "* flow*": std unit prices (unweighted, λ=$λ)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/comext/fig4/HP/" * "fig4_" * flow * "_std" * "_prices_unweighted" * "_HP" * ".png") # export image dropbox

end


# BE vis-a-vis GB
p = @df subset(df, :PARTNER_ISO => ByRow(x -> x == "GB")) plot(:DATE, :STD_UNIT_HP,
        group=:FLOW, lw=2, legend=:topleft, ylabel="euros", title="6 months STD price index (unweighted, λ=$λ): \n Belgium vis-a-vis Great Britain")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
savefig(p, dir_dropbox * "results/images/comext/fig4/HP/" * "fig4_" * "BE_GB" * "_STD_prices_unweighted" * "_HP" * ".png") # export image


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 2
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# prepare data
df = transform(df_comext, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> ifelse(iszero(x) | ismissing(x), missing, x)), renamecols=false) # need to remove missing also from EU
subset!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> !ismissing(x)))
subset!(df, :PARTNER_ISO => ByRow(x -> !(x in ["EU", "WORLD"])))

# remove outliers based on medians abs dev.
outlier_cutoff = 3
transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)
gdf = groupby(df, "PRODUCT_NC")
df = transform(gdf, :UNIT_PRICE => MAD_method => :MAD)
subset!(df, :MAD => ByRow(x -> x < outlier_cutoff))

# produce data for plotting
df = data_fig2(df, ["BE"], ["GB"], 2)

# ------------------

# formatting
transform!(df, [:DECLARANT_ISO, :PARTNER_ISO, :FLOW] .=> ByRow(string), renamecols=false)
transform!(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)

df.PRODUCT_NC_digits = parse.(Int64, String.(df.PRODUCT_NC_digits)) # transform into Int64

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
products = 1:8:89

for flow in ["imports", "exports"]
    for i in products
    
    product_range = i:i+7
    end_range = string(product_range[end])
    
    # values
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :VALUE_IN_EUROS,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="euros", title="Belgian "*flow*": values")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/comext/fig2/values/" * "fig2_" * flow * "_" * end_range * "_values" * ".png") # export image

    # price index (weighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :PRICE_INDEX,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="unit prices (weighted)", title="Belgian "*flow*": unit prices (weighted)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/comext/fig2/prices_weighted/" * "fig2_" * flow * "_" * end_range * "_prices_weighted" * ".png")

    # price index (unweighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :UNIT_PRICE,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="unit prices (unweighted)", title="Belgian "*flow*": unit prices (unweighted)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/comext/fig2/prices_unweighted/" * "fig2_" * flow * "_" * end_range * "_prices_unweighted" * ".png") 

    end
end

for flow in ["imports", "exports"]
    for i in products
    
    product_range = i:i+7
    end_range = string(product_range[end])
    
    # values
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :VALUE_IN_EUROS_HP,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="euros", title="Belgian "*flow*": values (HP, λ=$λ)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/comext/fig2/HP/values/" * "fig2_" * flow * "_" * end_range * "_values" * "_HP" * ".png") # export image

    # price index (weighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :PRICE_INDEX_HP,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="unit prices (weighted)", title="Belgian "*flow*": unit prices (weighted, λ=$λ)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/comext/fig2/HP/prices_weighted/" * "fig2_" * flow * "_" * end_range * "_prices_weighted" * "_HP" * ".png")

    # price index (unweighted unit prices)
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PRODUCT_NC_digits => ByRow(x -> x in product_range)) plot(:DATE, :UNIT_PRICE_HP,
            group=:PRODUCT_NC_digits, lw=2, legend=:bottomleft, ylabel="unit prices (unweighted)", title="Belgian "*flow*": unit prices (unweighted, λ=$λ)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/comext/fig2/HP/prices_unweighted/" * "fig2_" * flow * "_" * end_range * "_prices_unweighted" * "_HP" * ".png") 

    end
end


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Figure 3
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# prepare data
df = transform(df_comext, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> ifelse(iszero(x) | ismissing(x), missing, x)), renamecols=false) # need to remove missing also from EU
subset!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> ByRow(x -> !ismissing(x)))
#subset!(df, :PARTNER_ISO => ByRow(x -> !(x in ["EU", "WORLD"])))

# remove outliers based on medians abs dev.
outlier_cutoff = 3
transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)
gdf = groupby(df, "PRODUCT_NC")
df = transform(gdf, :UNIT_PRICE => MAD_method => :MAD)
subset!(df, :MAD => ByRow(x -> x < outlier_cutoff))

# produce data for plotting
df = data_fig3(df, ["BE"], ["GB", "EU", "DE", "FR", "NL"])

# ------------------

# formatting
transform!(df, [:DECLARANT_ISO, :PARTNER_ISO, :FLOW] .=> ByRow(string), renamecols=false)
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
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="percentages", title="Belgian "*flow*": value share")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/comext/fig3/" * "fig3_" * flow * "_value_share" * ".png") # export image

    # quantity share
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> !(x == "EU"))) plot(:DATE, :QUANTITY_SHARE,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="kg", title="Belgian "*flow*": quantity share")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/comext/fig3/" * "fig3_" * flow * "_quantity_share" * ".png") # export image

end

# plotting HP
for flow in ["imports", "exports"]

    # value share
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> !(x == "EU"))) plot(:DATE, :VALUE_SHARE_HP,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="percentages", title="Belgian "*flow*": value share (HP, λ=$λ)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/comext/fig3/HP/" * "fig3_" * flow * "_value_share" * "_HP" * ".png") # export image

    # quantity share
    p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> !(x == "EU"))) plot(:DATE, :QUANTITY_SHARE_HP,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="kg", title="Belgian "*flow*": quantity share (HP, λ=$λ)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, dir_dropbox * "results/images/comext/fig3/HP/" * "fig3_" * flow * "_quantity_share" * "_HP" * ".png") # export image

end

# BE vis-a-vis GB
p = @df subset(df, :PARTNER_ISO => ByRow(x -> x == "GB")) plot(:DATE, :VALUE_SHARE_HP,
        group=:FLOW, lw=2, legend=:bottomleft, ylabel="percentages", title="Total trade share (values, λ=$λ): \n Belgium vis-a-vis Great Britain")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
savefig(p, dir_dropbox * "results/images/comext/fig3/HP/" * "fig3_" * "BE_GB" * "_value_share" * "_HP" * ".png") # export image
