"""
2022-11-10
update WIAG data in a Jupyter-Notebook
item type: Bistum
preparation
   log in to a database (`Wds.dbwiag`)
   set `datapath`

 - incomplete -
"""

using Dates

function check_globals()
    @info Wds
    @info "" item_type_id
end

"""
    read_diocese(data_file, col_id)

return DataFrame with diocese data

col_id: name of column that contains the ID
"""
function read_diocese(data_file, col_id)
    df_d = CSV.read(
        data_file,
        DataFrame,
        types = Dict(
            col_id => String,
            :date_of_founding => String,
            :date_of_dissolution => String,
            :gatz_pages => String,
        ))
    Wds.clean_up!(df_d)

    @info "Zeilen, Spalten: " size(df_d)

    return df_d;
end

"""
    read_place(data_file)

return DataFrame with place data

"""
function read_place(data_file)
    df_p = CSV.read(
        data_file,
        DataFrame)
    Wds.clean_up!(df_p)

    @info "Zeilen, Spalten: " size(df_p)

    return df_p;
end
