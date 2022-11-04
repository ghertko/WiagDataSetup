"""
2022-10-24
update WIAG data in a Jupyter-Notebook
item type: Bischof
preparation
   log in to a database (`Wds.dbwiag`)
   set `datapath`
"""

using Dates

function check_globals()
    @info Wds
    @info item_type_id
end

"""
    insert_reference_volume(Wds, data_file)

insert references into reference_volume
"""
function insert_reference_volume!(table, data_file)
    @assert @isdefined(Wds) "'Wds' ist nicht definiert"

    @info "reading " table
    df = CSV.read(data_file, DataFrame)
    Wds.clean_up!(df)

    @info "Zeilen, Spalten: " size(df)

    insertcols!(df, :item_type_id => item_type_id)

    columns = [
        :item_type_id => :item_type_id,
        Symbol("ID_Ref") => :reference_id,
        Symbol("full_citation") => :full_citation,
        Symbol("author_editor") => :author_editor,
        Symbol("OnlineRessource") => :online_resource,
        Symbol("short_title") => :title_short,
        Symbol("ri_opac_id") => :ri_opac_id,
        Symbol("year of publication") => :year_publication,
        Symbol("ISBN") => :isbn,
    ];

    Wds.filltable!(table, select(df, columns));

end

"""
    read_person(data_file)

return DataFrame with person data
"""
function read_person(data_file, col_id = :ID_Bischof)
    df_p = CSV.read(
        data_file,
        DataFrame,
        types = Dict(
            col_id => String,
            :VIAF_ID => String,
            :GND_ID => String,
            :Wikidata_ID => String,
            :GSN_ID => String,
        ))
    Wds.clean_up!(df_p)

    @info "Zeilen, Spalten: " size(df_p)

    return df_p;
end

"""
    insert_item!(table,
                 data_file;
                 online_status = "fertig",
                 id_public_key = "Pers-EPISCGatz",
                 user_id = 40)

insert bishop meta data into item
"""
function insert_item!(table,
                      df_p;
                      online_status = "fertig",
                      id_public_key = "Pers-EPISCGatz",
                      user_id = 40)

    col_id = :ID_Bischof

    columns = [
        col_id => :id_in_source,
        :fertig => :fertig,

        Symbol("Eintrag RI OPAC") => :item_in_source,
    ]
    df_item = select(df_p, columns);

    # is_online; obsolete 2022-11-04
    # bishop data have no column with edit status information but "fertig"
    # is_online(x) = ismissing(x) ? 0 : (x == online_status ? 1 : 0)
    # transform!(df_item, :fertig => ByRow(is_online) => :is_online)

    # bishop data have no column with edit status information but "fertig"
    make_edit_status(x) = !ismissing(x) && x == 1 ? online_status : missing
    transform!(df_item, :fertig => ByRow(make_edit_status) => :edit_status);

    # id_public
    make_id_public(x) = Wds.make_id_public(x, 5, id_public_key)
    transform!(df_item, :id_in_source => ByRow(make_id_public) => :id_public);

    # meta data
    date_time_now = Dates.format(now(), Dates.dateformat"yyyy-mm-dd HH:MM")

    insertcols!(
        df_item,
        :created_by => user_id,
        :date_created => date_time_now,
        :changed_by => user_id,
        :date_changed => date_time_now,
        :item_type_id => item_type_id,
    );

    Wds.filltable!(table, select(df_item, Not(:fertig)))

end

"""
    insert_person(df_p::AbstractDataFrame)

get IDs from `item` and `religious_order`; insert data into `person`
"""
function insert_person!(table, df_p::AbstractDataFrame)

    col_id = :ID_Bischof

    # read ids
    sql = "SELECT id, id_in_source FROM item where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_ep = innerjoin(df_idx, df_p, on = :id_in_source => col_id);

    # assign religious order
    sql = "SELECT id as religious_order_id, abbreviation FROM religious_order";
    df_rel_order = Wds.sql_df(sql);
    df_ep = leftjoin(df_ep, df_rel_order, on = :Orden => :abbreviation, matchmissing = :notequal);

    # - info
    unknown_order = String[];
    for row in eachrow(df_ep)
        if !ismissing(row[:Orden]) && ismissing(row[:religious_order_id])
            push!(unknown_order, row[:Orden])
        end
    end
    @info "unbekannte Ordensbezeichnungen" unique(unknown_order);

    # numeric dates
    parse_date_birth(s) = Wds.parsemaybe(s, :lower)
    parse_date_death(s) = Wds.parsemaybe(s, :upper)

    transform!(df_ep, :Geburtsdatum => ByRow(parse_date_birth) => :num_date_birth);
    transform!(df_ep, :Sterbedatum => ByRow(parse_date_death) => :num_date_death);

    # item_type_id
    insertcols!(df_ep, :item_type_id => item_type_id);

    columns = [
        :id => :id,
        :item_type_id => :item_type_id,
        :Praefix => :prefixname,
        :Vorname => :givenname,
        :Familienname => :familyname,
        :Sterbedatum => :date_death,
        :Geburtsdatum => :date_birth,
        :Bemerkung_Red => :comment,
        :Kommentar_Name => :note_name,
        :Kommentar_Person => :note_person,
        :religious_order_id => :religious_order_id,
    ]

    Wds.filltable!(table, select(df_ep, columns))

end

"""
    insert_item_reference!(table, df_p)

inset references into `item_reference`
"""
function insert_item_reference!(table, df_p)

    col_id = :ID_Bischof

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_ep = innerjoin(df_idx, df_p, on = :id_in_source => col_id);

    n_missing = count(ismissing, df_ep.Quelle)
    @info "Einträge ohne Quelle" n_missing

    dropmissing!(df_ep, :Quelle)

    insertcols!(df_ep, :item_type_id => item_type_id)

    columns = [
        :id => :item_id,
        :item_type_id => :item_type_id,
        Symbol("Seite Gatz") => :page,
        :Quelle => :reference_id
    ]

    df_iref = select(df_ep, columns)

    Wds.filltable!(table, df_iref)

end


"""
    join_gsn(table, df_p::AbstractDataFrame, gso_db)

return joined DataFrame and DataFrame with new entries
auxiliary function
"""
function join_new_gsn(df_p::AbstractDataFrame, gso_db)

    col_id = :ID_Bischof

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_ep = innerjoin(df_idx, df_p, on = :id_in_source => col_id);

    # GSN
    # find the current GSN
    # table gsn_gsn is an auxiliary table, see notebook 'Domherr-GS'
    sql = "SELECT id_new AS gs_gsn_id, nummer, nummer_neu " *
        "FROM $(gso_db).gsn_gsn";
    df_gsn_gsn = Wds.sql_df(sql);
    df_ep_gsn = innerjoin(df_ep, df_gsn_gsn, on = :GSN_ID => :nummer, matchmissing = :notequal)

    # - info about new gsn
    df_info = subset(df_ep_gsn, [:GSN_ID, :nummer_neu] => ByRow(!isequal));
    df_info_out = select(df_info, [:id_in_source, :Vorname, :Familienname, :GSN_ID, :nummer_neu])

    return df_ep_gsn, df_info_out
end


function insert_gsn!(table, df_ep_gsn::AbstractDataFrame)
    authority_id = 200
    @info "" authority_id

    return insert_id_external_auth!(table, df_ep_gsn, authority_id, :nummer_neu)
end

"""
    insert_id_external(df_p::AbstractDataFrame)

insert into `id_external`
"""
function insert_id_external!(table, df_p::AbstractDataFrame)
    col_id = :ID_Bischof

    # id in table authority and column name in df_p
    authorities = [
        1 => :GND_ID,
        2 => :Wikidata_ID,
        4 => :VIAF_ID,
    ]

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_ep = innerjoin(df_idx, df_p, on = :id_in_source => col_id);

    for auth in authorities
        @info "Normdatum " auth.second
        insert_id_external_auth!(table, df_ep, auth.first, auth.second)
    end

end


function insert_wikipedia_url!(table, df_p::AbstractDataFrame)
    col_id = :ID_Bischof

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_ep = innerjoin(df_idx, df_p, on = :id_in_source => col_id);

    authority = (3 => :URL_Wikipedia)

    transform!(df_ep, authority.second => ByRow(Wds.fix_Wikipedia_URL) => authority.second);

    return insert_id_external_auth!(table, df_ep, authority.first, authority.second)
end

"""
    insert_id_exteral_auth!(table, df_p::AbstractDataFrame, auth_id, auth_col::Symbol)

insert values for id_external by authority
"""
function insert_id_external_auth!(table, df_ep::AbstractDataFrame, auth_id, auth_col::Symbol)

    columns = [
        :id => :item_id,
        auth_col => :value
    ]
    df_id_ext = select(df_ep, columns);
    df_id_ext = dropmissing(df_id_ext, :value)

    insertcols!(df_id_ext, :authority_id => auth_id)

    Wds.filltable!(table, df_id_ext)

end


"""
    insert_name_variant!(table, df_p::AbstractDataFrame, col::Symbol)

insert name variants into `familyname_variant` and `givenname_variant`
"""
function insert_name_variant!(table, df_p::AbstractDataFrame, col::Symbol)

    col_id = :ID_Bischof

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_ep = innerjoin(df_idx, df_p, on = :id_in_source => col_id);

    df_nv = dropmissing(df_ep, col)

    df_exp = Wds.expand_column(df_nv, col, delim = ",");

    columns = [
        :id => :person_id,
        col => :name,
    ]

    return Wds.filltable!(table, select(df_exp, columns))

end

function insert_name_lookup!(table, df_p)
    col_id = :ID_Bischof

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_ep = innerjoin(df_idx, df_p, on = :id_in_source => col_id);

    columns = [
        :id => :id,
        :Vorname => :givenname,
        :Praefix => :prefix_name,
        :Familienname => :familyname,
        :Vorname_Varianten => :givenname_variant,
        :Familienname_Variante => :familyname_variant,
    ]

    df_nl = Wds.create_name_lookup(select(df_ep, columns))

    return Wds.filltable!(table, df_nl)

end
