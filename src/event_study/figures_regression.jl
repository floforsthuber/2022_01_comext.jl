
# Setup

# Brexit dates
referendum = Date(2016, 07, 01) # referendum to leave the EU, Date(2016, 06, 23)
exit = Date(2020, 02, 01) # formal exit of GB from the EU, Date(2020,01,31)
trade = Date(2021, 05, 01) # Trade and Cooperation Agreement becomes effective, Date(2021,04,27)
brexit_dates = [referendum; exit; trade]

treated = "Verenigd Koninkrijk" # treated group
control = ["Nederland", "Duitsland", "Frankrijk", "Italië"] # conrol group
flow = "exports"
interval = 12 # 12 months before and after brexit scenarios

# -------------------------------------------------------------------------------------------------------------------------------------------------------------


function figures_reg(df::DataFrame, treated::String, control::Vector{String}, brexit_dates::Vector{Date}, interval::Int64, products::Vector{String})

    # create directory for specification
    folder = ""
    for i in [treated; control] folder = folder * "_" * i end
    folder = dir_dropbox * "results/regression/" * folder

    if isdir(folder)
        println(" ✓ The specification with treated: $treated and controls: $control are overwritten.")
    else
        mkdir(folder)
        println(" ✓ The specification with treated: $treated and controls: $control are added.")
    end

    # timeframe
    timeframe = brexit_dates[1]-Month(interval+1):Month(1):brexit_dates[end]+Month(interval)

    # subset data
    df = transform(df, :PERIOD => ByRow(x -> Date(string(x), DateFormat("yyyymm"))) => :DATE)
    # subset!(df, :PARTNER_ISO => ByRow(x -> x in ["WORLD"; treated; control]), :PRODUCT_NC => ByRow(x -> x in products), :DATE => ByRow(x -> x in timeframe))
    subset!(df, :PARTNER_ISO => ByRow(x -> x in ["WORLD"; treated; control]), :PRODUCT_NC => ByRow(x -> x in products))

    # create figures

    return df, folder
end


df_new, folder = figures_reg(df_VLAIO, treated, control, brexit_dates, interval, prod_exports)





function figure_1(df::DataFrame, folder::String, treated::String)

    # aggregate over products
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD", "DATE"]
    gdf = groupby(df, cols_grouping)
    df = combine(gdf, :VALUE_IN_EUROS => sum, renamecols=false)
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

    for flow in ["exports", "imports"]

        # values
        p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "WORLD")) plot(:DATE, :VALUE_IN_EUROS/1e9,
        group=:PARTNER_ISO, lw=2, legend=:topleft, ylabel="euros (billion)", title="Flemish "* flow*": values")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
        savefig(p, folder * "/" * "fig1_" * flow * "_values" * ".png") # export image dropbox

        # HP
        p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "WORLD")) plot(:DATE, :VALUE_IN_EUROS_HP/1e9,
                group=:PARTNER_ISO, lw=2, legend=:topleft, ylabel="euros (billion)", title="Flemish "*flow*": values (HP, λ=$λ)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
        savefig(p, folder * "/" * "fig1_" * flow * "_values" * "_HP" * ".png") # export image dropbox

        # 3MMA
        p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "WORLD")) plot(:DATE, :VALUE_3MMA/1e9,
                group=:PARTNER_ISO, lw=2, legend=:topleft, ylabel="euros (billion)", title="Flemish "*flow*": values (3MMA)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
        savefig(p, folder * "/" * "fig1_" * flow * "_values" * "_3MMA" * ".png") # export image dropbox

        # STD
        p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "WORLD")) plot(:DATE, :STD_VALUE/1e9,
                group=:PARTNER_ISO, lw=2, legend=:topleft, ylabel="euros (billion)", title="Flemish "*flow*": 6 months STD \n (values, rolling window)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
        savefig(p, folder * "/" * "fig4_" * flow * "_values" * "_STD" * ".png") # export image dropbox

    end

    # STD
    p = @df subset(df, :PARTNER_ISO => ByRow(x -> x == treated)) plot(:DATE, :STD_VALUE/1e9,
            group=:FLOW, lw=2, legend=:topleft, ylabel="euros (billion)", title="Flemish vs $treated: 6 months STD \n (values, rolling window)")
    vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
    vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
    vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, folder * "/" * "fig4_" * "_values" * "_STD" * ".png") # export image dropbox


end

figure_1(df_new, folder, treated)



function figure_3(df::DataFrame, folder::String, treated::String)

    # aggregate over products
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD", "DATE"]
    gdf = groupby(df, cols_grouping)
    df = combine(gdf, :VALUE_IN_EUROS => sum, renamecols=false)
    
    # create total
    df_WORLD = subset(df, :PARTNER_ISO => ByRow(x -> x == "WORLD"))
    cols_grouping = ["DECLARANT_ISO", "FLOW", "PERIOD", "DATE"]
    gdf = groupby(df_WORLD, cols_grouping)
    df_WORLD = combine(gdf, :VALUE_IN_EUROS => sum => :VALUE_WORLD)
    
    # join to create shares
    df = leftjoin(subset(df, :PARTNER_ISO => ByRow(x -> x != "WORLD")), df_WORLD, on=[:DECLARANT_ISO, :FLOW, :PERIOD, :DATE])
    transform!(df, [:VALUE_IN_EUROS, :VALUE_WORLD] => ByRow((x,s) -> x/s) => :SHARE_VALUE)
    sort!(df)

    # HP filter
    λ = 20
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
    gdf = groupby(df, cols_grouping)
    df = transform(gdf, :SHARE_VALUE => (x -> HP(x, λ)) => :SHARE_VALUE_HP)


    for flow in ["imports", "exports"]

        # values
        p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :SHARE_VALUE*100,
                group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="percentages", title="Flemish "* flow*" share (values)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
        savefig(p, folder * "/" * "fig3_" * flow * "_value_share" * ".png") # export image dropbox
    
        # HP
        p = @df subset(df, :FLOW => ByRow(x -> x == flow)) plot(:DATE, :SHARE_VALUE_HP*100,
            group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="percentages", title="Flemish "* flow*" share (values, λ=$λ)")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
        savefig(p, folder * "/" * "fig3_" * flow * "_value_share" * "_HP" * ".png") # export image dropbox
    
    end

    # VLA vis-a-vis treated
    p = @df subset(df, :PARTNER_ISO => ByRow(x -> x == treated)) plot(:DATE, :SHARE_VALUE_HP*100,
            group=:FLOW, lw=2, legend=:bottomleft, ylabel="percentages", title="Total trade share (values, λ=$λ): \n Flanders vs $treated")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
    savefig(p, folder * "/" * "fig3_" * "VLA_treated" * "_value_share" * "_HP" * ".png") # export image

end

figure_3(df_new, folder, treated)



function figure_5(df::DataFrame, folder::String)

    # aggregate 
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD", "DATE"]
    gdf = groupby(df, cols_grouping)
    df = combine(gdf, nrow => :COUNT)

    for flow in ["imports", "exports"]

        p = @df subset(df, :FLOW => ByRow(x -> x == flow), :PARTNER_ISO => ByRow(x -> x != "WORLD")) plot(:DATE, :COUNT,
                group=:PARTNER_ISO, lw=2, legend=:bottomleft, ylabel="number of products", title="Flemish "* flow*": number of products")
        vline!([Date(2016,6,23)], label="vote", color=:black, lw=1, ls=:solid) # refer
        vline!([Date(2020,01,31)], label="exit", color=:black, lw=1, ls=:dash) # exit
        vline!([Date(2020,12,31)], label="trans end", color=:black, lw=1.5, ls=:dot) # trans end
        savefig(p, folder * "/" * "fig5_" * flow * "_product_count" * ".png") # export image dropbox
    
    end

end

figure_5(df_new, folder)


# YoY percentage change
#   - computes percentage change if difference between PERIOD == 100 (201512 - 201412 = 100)
function yoy_change(period::AbstractVector, input::AbstractVector)
    M = [period[i]-period[j] == 100 ? log(input[i]/input[j]) : missing for i in eachindex(input), j in eachindex(input)] # matrix
    V = [all(ismissing.(M[i,:])) ? missing : M[i, findfirst(typeof.(M[i,:]) .== Float64)] for i in 1:size(M, 1)] # reduce to vector
    return V
end


function figure_7(df::DataFrame, folder::String)

    # aggregate
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD", "DATE"]
    gdf = groupby(df, cols_grouping)
    df = combine(gdf, :VALUE_IN_EUROS => sum, renamecols=false)

    # compute YOY monthly percentage change
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
    gdf = groupby(df, cols_grouping)
    df = transform(gdf, [:PERIOD, :VALUE_IN_EUROS] => yoy_change => :YOY_VALUE)
    subset!(df, :YOY_VALUE => ByRow(x -> !ismissing(x))) # need to drop missing for MAD/HP

    # # MAD adjustment
    # #   - need to drop missing
    # subset!(df, :YOY_VALUE => ByRow(x -> !ismissing(x)))
    # outlier_cutoff = 4
    # gdf = groupby(df, "PRODUCT_NC_digits")
    # df = transform(gdf, :YOY_VALUE => MAD_method => :MAD)
    # subset!(df, :MAD => ByRow(x -> x < outlier_cutoff))
    # df = df[:, Not(:MAD)]

    # HP filter
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
        savefig(p, folder * "/" * "fig7_" * flow * "_YOY" * "_STD_HP" * ".png") # export image dropbox

    end

end

figure_7(df_new, folder)


function table_3(df::DataFrame, folder::String, brexit_dates::Vector{Date})

    # aggregate
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "PERIOD", "DATE"]
    gdf = groupby(df, cols_grouping)
    df = combine(gdf, :VALUE_IN_EUROS => sum, renamecols=false)

    # compute YOY monthly percentage change
    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW"]
    gdf = groupby(df, cols_grouping)
    df = transform(gdf, [:PERIOD, :VALUE_IN_EUROS] => yoy_change => :YOY_VALUE)
    subset!(df, :YOY_VALUE => ByRow(x -> !ismissing(x))) # need to drop missing for MAD/HP

    tab_std = transform(df, :DATE => ByRow(x -> ifelse(x in brexit_dates[1]-Month(1)-Year(1):Month(1):brexit_dates[1]-Month(1), "pre",
        ifelse(x in brexit_dates[1]:Month(1):brexit_dates[2]-Month(1), "brexit_1", ifelse(x in brexit_dates[2]:Month(1):brexit_dates[3]-Month(1), "brexit_2", 
            ifelse(x in brexit_dates[3]:Month(1):brexit_dates[3]+Month(10), "brexit_3", "outside"))))) => :BREXIT)

    cols_grouping = ["DECLARANT_ISO", "PARTNER_ISO", "FLOW", "BREXIT"]
    gdf = groupby(tab_std, cols_grouping)
    tab_std = combine(gdf, :YOY_VALUE => std => :STD_VALUE)

    tab_std_wide = unstack(tab_std, :BREXIT, :STD_VALUE)
    tab_std_wide = tab_std_wide[:, Not(:outside)]
    sort!(tab_std_wide, :FLOW)

    XLSX.writetable(folder * "/" * "table3_std" * ".xlsx", tab_std_wide, overwrite=true)


end

table_3(df_new, folder, brexit_dates)
