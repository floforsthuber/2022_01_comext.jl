# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script for CN classification 
# -------------------------------------------------------------------------------------------------------------------------------------------------------------


using DataFrames, CSV, XLSX, LinearAlgebra, Statistics

dir_io = "C:/Users/u0148308/data/comext/" # location of input/output (io)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Download CN8 codes from RAMON
#   - needs to be done manually from:
#       https://ec.europa.eu/eurostat/ramon/nomenclatures/index.cfm?TargetUrl=LST_NOM&StrGroupCode=CLASSIFIC&StrLanguageCode=EN
#   - manually converted the .xls files from 2007-2013 to .csv
# -------------------------------------------------------------------------------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Import raw data into Julia and do initial cleaning
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# Notes:
#   - different column names throughout the years
function initial_cleaning(year::String)

    # load data
    path = dir_io * "raw/correspondence/CN/" * "CN_" * year * ".csv"
    df = CSV.read(path, DataFrame)

    # raw data has different structure
    if year in ["2007", "2009", "2012", "2013"]
        col_CN_codes = :CN
        col_subset = ["PROD_CODE", "EN", "SU"]
    elseif year in ["2008"]
        col_CN_codes = :CN
        col_subset = ["PROD_CODE", "DESC_EN", "SU"]
    elseif year in ["2010"]
        col_CN_codes = :CN
        col_subset = ["PROD_CODE", "DM_EN", "SU"]
    elseif year in [string.(2001:2006) ; string.(2014:2022)]
        col_CN_codes = :Code_1
        col_subset = ["PROD_CODE", "Description", "Supplementary unit"]
    elseif year in ["2000"]
        col_CN_codes = :Code_1
        df.SU .= missing # no supplimentary unit in 2000
        col_subset = ["PROD_CODE", "Description", "SU"]
    elseif year in ["2011"]
        col_CN_codes = :CN
        col_subset = ["PROD_CODE", "DM", "SU"]
    else 
        println(" × The raw data for the year: $year has an unspecified format! \n")
    end

    # subset to CN8
    transform!(df, names(df) .=> ByRow(string), renamecols=false) # also missing are strings now
    transform!(df, col_CN_codes => ByRow(x -> replace(x, " " => "")) => :PROD_CODE) # remove whitespace
    subset!(df, :PROD_CODE => ByRow(x -> length(x) == 8)) # subset to CN8 codes

    # formatting
    df = df[:, col_subset] # select columns
    rename!(df, ["PROD_CODE", "PROD_LAB", "SUPP_UNIT"]) # rename columns
    transform!(df, names(df) .=> ByRow(x -> ifelse(x in ["missing", "-"], missing, x)), renamecols=false) # reintroduce missing

    return df
end

# # check if function works for all years
# for i in string.(2000:2021)
#     example = initial_cleaning(i)
# end

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Classify changes in CN codes
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# load cleaned file of CN changes
# Notes:
#   - script for obtaining this file: "CN_update.jl"
path = dir_io * "clean/" * "CN_update.csv"
df_CN_update = CSV.read(path, DataFrame, types=String)

# function to classify the different updates
function CN_update_classification(year::String)

    # subset data
    df = subset(df_CN_update, :PERIOD_END => x -> x .== year)

    # initialize object to store CN codes in
    singular = String[]
    one_to_many = String[]
    many_to_one = String[]
    many_to_many = String[]

    # classify changes
    for i in unique(df.ORIG_CODE)

        df_ORIG = subset(df, :ORIG_CODE => x -> x .== i)
        df_DEST = subset(df, :DEST_CODE => ByRow(x -> x in df_ORIG.DEST_CODE))
    
        df_ORIG_2 = subset(df, :ORIG_CODE => ByRow(x -> x in df_DEST.ORIG_CODE))
    
        # one-to-one vs many-to-one vs many-to-many
        if size(df_ORIG, 1) == 1
            if size(df_DEST, 1) == 1
                push!(singular, i)
            elseif length(unique(df_ORIG_2.DEST_CODE)) == 1
                push!(many_to_one, i)
            else
                push!(many_to_many, i)
            end
        # many-to-many vs one-to-many vs many-to-one
        else
            if length(unique(df_DEST.ORIG_CODE)) == 1
                push!(one_to_many, i)
            else
                push!(many_to_many, i)
            end
        end
    
    end

    non_singular = [one_to_many; many_to_one; many_to_many]

    # compute number of observations for table
    n_singular = length(singular)

    n_prev_one_to_many = length(one_to_many)
    n_prev_many_to_one = length(many_to_one)
    n_prev_many_to_many = length(many_to_many)
    n_prev_non_singular = length(non_singular)

    n_one_to_many = length(unique(subset(df, :ORIG_CODE => ByRow(x -> x in one_to_many)).DEST_CODE)) # unique since mapped to many
    n_many_to_one = length(unique(subset(df, :ORIG_CODE => ByRow(x -> x in many_to_one)).DEST_CODE))
    n_many_to_many = length(unique(subset(df, :ORIG_CODE => ByRow(x -> x in many_to_many)).DEST_CODE))
    n_non_singular = length(unique(subset(df, :ORIG_CODE => ByRow(x -> x in non_singular)).DEST_CODE))

    table_row = [n_singular n_non_singular n_prev_one_to_many n_one_to_many n_prev_many_to_one n_many_to_one n_prev_many_to_many n_many_to_many]

    return table_row
end

# # check if function works for all years
# for i in string.(2000:2021)
#     example = CN_update_classification(year_example)
# end

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# create table
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# load cleaned file of CN changes
path = dir_io * "clean/" * "CN_update.csv"
df_CN_update = CSV.read(path, DataFrame, types=String)

# initialize DataFrame
table_CN_changes = DataFrame(year=String[], products=Int64[], unchanged=Int64[], pct_changed=String[], singular=Int64[], non_singular=Int64[],
                             OtM_prev=Int64[], OtM=Int64[], MtO_prev=Int64[], MtO=Int64[], MtM_prev=Int64[], MtM=Int64[])

for i in string.(2000:2022)

    df = initial_cleaning(i) # actually only provides total number of observations
    n_update_classification = CN_update_classification(i) # classifies the changes

    # compute some more numbers for the table
    n_products = size(df, 1) # number of CN codes
    n_changed = n_update_classification[1] + n_update_classification[2] # number of changed CN codes
    n_unchanged = n_products - n_changed # number of unchanged CN codes
    pct_changed = string(round(n_changed / n_products * 100, digits=1)) * "%"

    push!(table_CN_changes, [i n_products n_unchanged pct_changed n_update_classification])

end

table_CN_changes
XLSX.writetable(dir_io * "clean/" * "table_CN_changes.xlsx", table_CN_changes, overwrite=true)
