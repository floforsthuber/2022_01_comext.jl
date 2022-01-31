
# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Unit prices
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# compute UNIT_PRICE
transform!(df, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] => ByRow((v,q) -> v/q) => :UNIT_PRICE)

# compute UNIT_PRICE_CHANGE
cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "PRODUCT_NC", "FLOW"]
gdf = groupby(df, cols_grouping)
df = transform(gdf, :UNIT_PRICE => pct_change => :UNIT_PRICE_CHANGE)

# compute MOM_PRICE_CHANGE
gdf = groupby(df, cols_grouping)
df = transform(gdf, [:PERIOD, :UNIT_PRICE_CHANGE] => mom_change => :MOM)

# export data
df_export = subset(df, :PERIOD => ByRow( x-> string(x) == year*sub_month))
CSV.write(dir_io * "clean/" * "df_prices_" * year * sub_month * ".csv", df_export)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Share of Belgium
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# compute EU_WORLD (EU imports/exports to the entire world)
cols_grouping = ["PRODUCT_NC", "FLOW", "PERIOD"]
gdf = groupby(df, cols_grouping)
df_sum_EU_WORLD = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> (x -> sum(skipmissing(x))), renamecols=false)
df_sum_EU_WORLD.TRADE_TYPE .= "total"

# compute EU_EXTRA and EU_INTRA (EU imports/exports to extra/intra EU countries)
cols_grouping = ["TRADE_TYPE", "PRODUCT_NC", "FLOW", "PERIOD"]
gdf = groupby(df, cols_grouping)
df_sum_EU_EXTRA_INTRA = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> (x -> sum(skipmissing(x))), renamecols=false)

# compute BE_WORLD (BE imports/exports to the entire world)
df_BE = subset(df, :DECLARANT_ISO => ByRow(x -> x == "BE"))
cols_grouping = ["PRODUCT_NC", "FLOW", "PERIOD"]
gdf = groupby(df_BE, cols_grouping)
df_sum_BE_WORLD = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> (x -> sum(skipmissing(x))), renamecols=false)
df_sum_BE_WORLD.TRADE_TYPE .= "total"

# compute BE_EXTRA and BE_INTRA (BE imports/exports to extra/intra EU countries)
cols_grouping = ["TRADE_TYPE", "PRODUCT_NC", "FLOW", "PERIOD"]
gdf = groupby(df_BE, cols_grouping)
df_sum_BE_EXTRA_INTRA = combine(gdf, [:VALUE_IN_EUROS, :QUANTITY_IN_KG] .=> (x -> sum(skipmissing(x))), renamecols=false)

# merge tables
df_sum_EU = vcat(df_sum_EU_EXTRA_INTRA, df_sum_EU_WORLD)
df_sum_BE = vcat(df_sum_BE_EXTRA_INTRA, df_sum_BE_WORLD)

# join, rename and compute shares in percentages (?)
df_join = leftjoin(df_sum_EU, df_sum_BE, on=[:TRADE_TYPE, :PRODUCT_NC, :FLOW, :PERIOD], makeunique=true)
rename!(df_join, ["TRADE_TYPE", "PRODUCT_NC", "FLOW", "PERIOD", "VALUE_EU", "QUANITY_EU", "VALUE_BE", "QUANITY_BE"])
df_join.VALUE_SHARE = df_join.VALUE_BE ./ df_join.VALUE_EU .* 100
df_join.QUANTITY_SHARE = df_join.QUANITY_BE ./ df_join.QUANITY_EU .* 100

# possibly need to sort before grouping, seems that grouping sorts incorrectly sometimes?
sort!(df_join, [:PRODUCT_NC, :FLOW, :TRADE_TYPE, :PERIOD])

# compute SHARE_CHANGE
cols_grouping = ["TRADE_TYPE", "PRODUCT_NC", "FLOW"]
gdf = groupby(df_join, cols_grouping)
df_share_BE = transform(gdf, [:VALUE_SHARE, :QUANTITY_SHARE] .=> pct_change .=> [:VALUE_SHARE_CHANGE, :QUANTITY_SHARE_CHANGE])

# compute MOM_CHANGE
gdf = groupby(df_share_BE, cols_grouping)
df_share_BE = transform(gdf, [:PERIOD, :VALUE_SHARE_CHANGE] => mom_change => :MOM_VALUE,
                             [:PERIOD, :QUANTITY_SHARE_CHANGE] => mom_change => :MOM_QUANTITY)


# export data
df_export = subset(df_share_BE, :PERIOD => ByRow( x-> string(x) == year*sub_month))
CSV.write(dir_io * "clean/" * "df_share_BE_" * year * sub_month * ".csv", df_export)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
