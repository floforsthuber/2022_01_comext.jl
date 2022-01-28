# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script for CN classification 
# -------------------------------------------------------------------------------------------------------------------------------------------------------------


using DataFrames, CSV, XLSX, LinearAlgebra, Statistics

dir_io = "C:/Users/u0148308/data/comext/" # location of input/output (io)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Download CN8 codes from RAMON
#   - needs to be done manually from:
#       https://ec.europa.eu/eurostat/ramon/nomenclatures/index.cfm?TargetUrl=LST_NOM&StrGroupCode=CLASSIFIC&StrLanguageCode=EN
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

path = dir_io * "clean/" * "CN_update.csv"
df_CN_update = CSV.read(path, DataFrame, types=String)

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Import raw data into Julia and do initial cleaning
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

year = "2014"

path = dir_io * "raw/correspondence/" * "CN_" * year * ".csv"
df = CSV.read(path, DataFrame)

# subset to CN8
transform!(df, names(df) .=> ByRow(string), renamecols=false) # also missing are strings now
transform!(df, :Code_1 => ByRow(x -> replace(x, " " => "")) => :PROD_CODE) # remove whitespace
subset!(df, :PROD_CODE => ByRow(x -> length(x) == 8)) # subset to CN8 codes

# formatting
df = df[:, ["PROD_CODE", "Description", "Supplementary unit"]] # select columns
rename!(df, ["PROD_CODE", "PROD_LAB", "SUPP_UNIT"]) # rename columns
transform!(df, names(df) .=> ByRow(x -> ifelse(x in ["missing", "-"], missing, x)), renamecols=false) # reintroduce missing


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Compute number of changed/unchanged CN codes
# -------------------------------------------------------------------------------------------------------------------------------------------------------------


# works nicely

df = subset(df_CN_update, :PERIOD_END => x -> x .== year)

singular = String[]
one_to_many = String[]
many_to_one = String[]
many_to_many = String[]


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

unique(subset(df, :ORIG_CODE => ByRow(x -> x in one_to_many)).DEST_CODE)
unique(subset(df, :ORIG_CODE => ByRow(x -> x in many_to_one)).DEST_CODE)
unique(subset(df, :ORIG_CODE => ByRow(x -> x in many_to_many)).DEST_CODE)
unique(subset(df, :ORIG_CODE => ByRow(x -> x in non_singular)).DEST_CODE)
# -------------------------------------------------------------------------------------------------------------------------------------------------------------


