"""
2022-10-24
update WIAG data in a Jupyter-Notebook
item type: Domherr
preparation
log in to a database (`Wds.dbwiag`)
set `datapath`
"""

using Dates

function check_globals()
    @info Wds
    @info "" item_type_id
    @info "" bishop_item_type
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
        :ID_Ref => :reference_id,
        :Literatur => :full_citation,
        :Verfasser => :author_editor,
        :OnlineRessource => :online_resource,
        :Kurztitel => :title_short,
        :Sort => :display_order,
        :ZitationGS => :gs_citation
    ]


    Wds.filltable!(table, select(df, columns));

end

"""
    read_person(data_file, col_id)

return DataFrame with person data

col_id: name of column that contains the ID
"""
function read_person(data_file, col_id)
    df_p = CSV.read(
        data_file,
        DataFrame,
        types = Dict(
            col_id => String,
            :Merged_Into => String,
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
                 df_p::AbstractDataFrame,
                 online_status = "fertig",
                 id_public_key = "Pers-EPISCGatz",
                 user_id = 40)

insert bishop meta data into item
"""
function insert_item!(table,
                      df_p::AbstractDataFrame;
                      online_status = "online",
                      id_public_key = "Pers-CANON",
                      user_id = 40)

    columns = [
        :ID_Domherr => :id_in_source,
        :Status => :edit_status,
        Symbol("Eintrag_Quelle") => :item_in_source,
    ]
    df_item = select(df_p, columns);

    # is_online
    # bishop data have no column edit_status but "fertig"
    is_online(x) = ismissing(x) ? 0 : (x == online_status ? 1 : 0)
    transform!(df_item, :edit_status => ByRow(is_online) => :is_online)

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

    # return df_item


    Wds.filltable!(table, df_item)


end

function update_merge_status!(table, df_p::AbstractDataFrame)
    # read IDs
    table = "item";
    sql = "SELECT id, id_in_source FROM $(table) WHERE item_type_id = $(item_type_id)";
    df_id_in_source = Wds.sql_df(sql);

    is_valid_id(id) = !ismissing(id) && id != "0"

    columns = [
        :ID_Domherr,
        :Vorname,
        :Familienname,
        :Merged_Into,
        :Status
    ]

    df_merged = subset(select(df_p, columns), :Merged_Into => ByRow(is_valid_id));

    df_merged = leftjoin(df_merged, df_id_in_source, on = :Merged_Into => :id_in_source);

    rename!(df_merged, :id => :merged_into_id);

    df_merged = leftjoin(df_merged, df_id_in_source, on = :ID_Domherr => :id_in_source);

    @info "set merge_status"
    # child
    table = "item";
    sql = "UPDATE $(table) SET merge_status = 'child' WHERE id = ?";
    updstmt = DBInterface.prepare(Wds.dbwiag, sql);

    c_id_list = unique(df_merged.merged_into_id);
    for c_id in c_id_list
        DBInterface.execute(updstmt, c_id)
    end

    DBInterface.close!(updstmt)

    table = "item";
    sql = "UPDATE $(table) SET merge_status = 'parent' WHERE id = ?";
    updstmt = DBInterface.prepare(Wds.dbwiag, sql);

    p_id_list = unique(df_merged.id);

    for p_id in p_id_list
        DBInterface.execute(updstmt, p_id)
    end

    DBInterface.close!(updstmt)

    @info "set merge_status"

    table = "item";
    sql = "UPDATE $(table) SET merged_into_id = ? WHERE id = ?";
    updstmt = DBInterface.prepare(Wds.dbwiag, sql);

    for row in eachrow(df_merged)
        set_id_merged_into_id = collect(row[[:merged_into_id, :id]])
        DBInterface.execute(updstmt, set_id_merged_into_id)
    end

    DBInterface.close!(updstmt)

    @info "Zahl der zusammengeführten Einträge (parent)", size(df_merged, 1)

end

"""
    insert_person(df_p::AbstractDataFrame)

get IDs from `item` and `religious_order`; insert data into `person`
"""
function insert_person!(table, df_p::AbstractDataFrame)

    # read ids
    sql = "SELECT id, id_in_source FROM item where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_cn = innerjoin(df_idx, df_p, on = :id_in_source => :ID_Domherr);

    # assign religious order
    sql = "SELECT id as religious_order_id, abbreviation FROM religious_order";
    df_rel_order = Wds.sql_df(sql);
    df_cn = leftjoin(df_cn, df_rel_order, on = :Orden => :abbreviation, matchmissing = :notequal);

    # - info
    unknown_order = String[];
    for row in eachrow(df_cn)
        if !ismissing(row[:Orden]) && ismissing(row[:religious_order_id])
            push!(unknown_order, row[:Orden])
        end
    end
    @info "unbekannte Ordensbezeichnungen" unique(unknown_order);

    # numeric dates
    parse_date_birth(s) = Wds.parsemaybe(s, :lower)
    parse_date_death(s) = Wds.parsemaybe(s, :upper)

    transform!(df_cn, :Geburtsdatum => ByRow(parse_date_birth) => :num_date_birth);
    transform!(df_cn, :Sterbedatum => ByRow(parse_date_death) => :num_date_death);

    # item_type_id
    insertcols!(df_cn, :item_type_id => item_type_id);

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

    # return df_cn

    Wds.filltable!(table, select(df_cn, columns))

end

"""
"""
function insert_item_property!(table,
                               df_p::AbstractDataFrame;
                               property_column = Symbol("AkademischerTitel"),
                               property_name = "academic_title")
    @assert @isdefined(Wds) "'Wds' ist nicht definiert"

    # read ids
    sql = "SELECT id, id_in_source FROM item where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_cn = innerjoin(df_idx, df_p, on = :id_in_source => :ID_Domherr);


    # get property type id
    sql = "SELECT id FROM item_property_type " *
        "WHERE name = '$(property_name)'"
    c = DBInterface.execute(Wds.dbwiag, sql)
    property_type_id = first(c)[:id]

    columns = [
        :id => :item_id,
        property_column => :value,
    ]

    dropmissing!(df_cn, property_column)
    df_ip = select(df_cn, columns)

    insertcols!(
        df_ip,
        :name => property_name, # redundant because of property_type_id
        :property_type_id => property_type_id,
    )

    Wds.filltable!(table, df_ip)

end

"""
    insert_item_reference!(table, df_p)

insert references into `item_reference`
"""
function insert_item_reference!(table, df_p)
    @assert @isdefined(Wds) "'Wds' ist nicht definiert"

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_cn = innerjoin(df_idx, df_p, on = :id_in_source => :ID_Domherr);

    n_missing = count(ismissing, df_cn.Quelle)
    @info "Einträge ohne Quelle" n_missing

    dropmissing!(df_cn, :Quelle)

    insertcols!(df_cn, :item_type_id => item_type_id)

    columns = [
        :id => :item_id,
        :item_type_id => :item_type_id,
        Symbol("Seite_Quelle") => :page,
        :Quelle => :reference_id,
        :ID_Quelle => :id_in_reference,
        :Merged_Into => :merged_into_id,
    ]

    df_iref = select(df_cn, columns)


    Wds.filltable!(table, select(df_iref, Not(:merged_into_id)))

end

"""
    insert_merged_item_reference!(table, df_p)

insert references from merged entries
"""
function insert_merged_item_reference!(table, df_p)
    @assert @isdefined(Wds) "'Wds' ist nicht definiert"

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_cn = innerjoin(df_idx, df_p, on = :id_in_source => :ID_Domherr);

    columns = [
        :id => :item_id,
        :id_in_source => :id_in_source,
        Symbol("Seite_Quelle") => :page,
        :Quelle => :reference_id,
        :ID_Quelle => :id_in_reference,
        :Merged_Into => :merged_into_id,
        :Status => :edit_status,
    ]

    df_iref = select(df_cn, columns)

    insertcols!(df_iref, :item_type_id => item_type_id)

    # find merged entries
    is_ref_2(merged_into_id, status) = !ismissing(merged_into_id) && isequal(status, "merged")
    df_iref_2 = subset(df_iref, [:merged_into_id, :edit_status] => ByRow(is_ref_2))


    # map IDs
    item_id_lookup = Dict(df_cn.id_in_source .=> df_cn.id)

    # @info item_id_lookup

    get_item_id(id_in_source) = get(item_id_lookup, id_in_source, missing)

    transform!(df_iref_2, :merged_into_id => ByRow(get_item_id) => :item_id_2)

    columns = [
        :page => :page,
        :reference_id => :reference_id,
        :id_in_reference => :id_in_reference,
        :item_id_2 => :item_id,
        :item_type_id => :item_type_id,
    ]

    Wds.filltable!(table, select(df_iref_2, columns), clear_table = false)

end

"""
    insert_id_external(df_p::AbstractDataFrame)

insert into `id_external`; all authorities except GS
"""
function insert_id_external!(table, df_p::AbstractDataFrame)
    @assert @isdefined(Wds) "'Wds' ist nicht definiert"

    # id in table authority and column name in df_p
    # +-----+--------------------------------------+
    # | id  | url_name_formatter                   |
    # +-----+--------------------------------------+
    # |   1 | Gemeinsame Normdatei (GND) ID        |
    # |   2 | Wikidata                             |
    # |   3 | Wikipedia-Artikel                    |
    # |   4 | VIAF-ID                              |
    # |   5 | WIAG-ID                              |
    # | 200 | Personendatenbank der Germania Sacra |
    # +-----+--------------------------------------+
    authorities = [
        1 => :GND_ID,
        2 => :Wikidata_ID,
        4 => :VIAF_ID,
        5 => :WIAG_ID_Bischof,
    ]

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_cn = innerjoin(df_idx, df_p, on = :id_in_source => :ID_Domherr);

    for auth in authorities
        @info "Normdatum " auth.second
        insert_id_external_auth!(table, df_cn, auth.first, auth.second)
    end

end

"""

"""
function join_ep_gsn(df_p::AbstractDataFrame)::AbstractDataFrame
    authority_id = 200
    @info "" authority_id

    table_item = "item"
    table_id_external = "id_external"
    sql = "SELECT i.id as ep, i.id_public, e1.value AS ep_gsn " *
        "FROM $(table_item) AS i " *
        "JOIN $(table_id_external) AS e1 ON e1.item_id = i.id AND e1.authority_id = $(authority_id) " *
        "WHERE i.item_type_id = $(bishop_item_type) AND i.is_online";
    df_ep = Wds.sql_df(sql)

    df_p_gsn = leftjoin(df_p, df_ep, on = :WIAG_ID_Bischof => :id_public, matchmissing = :notequal)

    # use bishop's GSN where appropriate
    for row in eachrow(df_p_gsn)
        if ismissing(row[:GSN_ID]) && !ismissing(row[:ep_gsn])
            row[:GSN_ID] = row[:ep_gsn]
        end
    end

    dropmissing!(df_p_gsn, :GSN_ID)

    insertcols!(df_p_gsn, :authority_id => authority_id)

    return df_p_gsn
end

"""
    join_new_gsn(table, df_p::AbstractDataFrame, gso_db)

return joined DataFrame and DataFrame with new entries
auxiliary function
"""
function join_new_gsn(df_p::AbstractDataFrame, gso_db)

    col_id = :ID_Domherr

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_p = innerjoin(df_idx, df_p, on = :id_in_source => col_id);

    # GSN
    # find the current GSN
    # table gsn_gsn is an auxiliary table, see notebook 'Domherr-GS'
    sql = "SELECT id_new AS gs_gsn_id, nummer, nummer_neu
    FROM $(gso_db).gsn_gsn";
    df_gsn_gsn = Wds.sql_df(sql);
    df_p_gsn = innerjoin(df_p, df_gsn_gsn, on = :GSN_ID => :nummer, matchmissing = :notequal)

    # - info about new gsn
    df_info_0 = subset(df_p_gsn, [:GSN_ID, :nummer_neu] => ByRow(!isequal));
    df_info = select(df_info_0, [:id_in_source, :Vorname, :Familienname, :GSN_ID, :nummer_neu])

    return df_p_gsn, df_info
end

"""
    function insert_gsn!(table, df_p_gsn::AbstractDataFrame)

insert GSN into `id_external`
"""
function insert_gsn!(table, df_ep_gsn::AbstractDataFrame)
    authority_id = 200
    @info "" authority_id

    return insert_id_external_auth!(table, df_ep_gsn, authority_id, :nummer_neu)
end


"""
    insert_wikipedia_url!(table, df_p::AbstractDataFrame)

insert Wikipedia URL into `id_external`
"""
function insert_wikipedia_url!(table, df_p::AbstractDataFrame)
    @assert @isdefined(Wds) "'Wds' ist nicht definiert"

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_p = innerjoin(df_idx, df_p, on = :id_in_source => :ID_Domherr);

    authority = (3 => :URL_Wikipedia)

    transform!(df_p, authority.second => ByRow(Wds.fix_Wikipedia_URL) => authority.second);

    return insert_id_external_auth!(table, df_p, authority.first, authority.second)
end

"""
    insert_id_exteral_auth!(table, df_p::AbstractDataFrame, auth_id, auth_col::Symbol)

auxiliary function: insert values for id_external by authority
"""
function insert_id_external_auth!(table, df_p::AbstractDataFrame, auth_id, auth_col::Symbol)

    columns = [
        :id => :item_id,
        auth_col => :value
    ]
    df_id_ext = select(df_p, columns);
    df_id_ext = dropmissing(df_id_ext, :value)

    # some entries contain a complete URL, e.g. for WIAG_ID_Bischof
    function extract_id_value(data)
        data = strip(data)
        val = data
        if data[1:4] == "http"
            p_list = split(data, "/")
            val = p_list[end]
        end
        return val
    end

    transform!(df_id_ext, :value => ByRow(extract_id_value) => :value)

    insertcols!(df_id_ext, :authority_id => auth_id)

    Wds.filltable!(table, df_id_ext)

end

"""
    insert_url_external(table, df_p)

"""
function insert_url_external!(table, data_file)
    @assert @isdefined(Wds) "'Wds' ist nicht definiert"

    df_url = CSV.read(data_file, DataFrame, delim=";", types = Dict(:domherr_id => String))
    Wds.clean_up!(df_url)

    # we need this offset because we use we need table authority twice
    # for this item_type
    id_offset = 1000;

    transform!(df_url, :url_type => ByRow(x -> x + id_offset) => :authority_id);

    # assign ids
    sql = "SELECT id, id_in_source  FROM item WHERE item_type_id = $(item_type_id)"
    df_idx = Wds.sql_df(sql);

    df_url = leftjoin(df_url, df_idx, on = :domherr_id => :id_in_source, matchmissing = :notequal);

    dropmissing!(df_url, [:id, :url_value])

    columns = [
        :id => :item_id,
        :authority_id => :authority_id,
        :url_value => :value,
        :note => :note,
    ]

    Wds.filltable!(table, select(df_url, columns))
end



"""
    insert_name_variant!(table, df_p::AbstractDataFrame, col::Symbol)

insert name variants into `familyname_variant` and `givenname_variant`
"""
function insert_name_variant!(table, df_p::AbstractDataFrame, col::Symbol)
    @assert @isdefined(Wds) "'Wds' ist nicht definiert"

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_p = innerjoin(df_idx, df_p, on = :id_in_source => :ID_Domherr);

    df_nv = dropmissing(df_p, col)

    df_exp = Wds.expand_column(df_nv, col, delim = ",");

    columns = [
        :id => :person_id,
        col => :name,
    ]

    return Wds.filltable!(table, select(df_exp, columns))

end

function insert_name_lookup!(table, df_p)
    @assert @isdefined(Wds) "'Wds' ist nicht definiert"

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_p = innerjoin(df_idx, df_p, on = :id_in_source => :ID_Domherr);

    columns = [
        :id => :id,
        :Vorname => :givenname,
        :Praefix => :prefix_name,
        :Familienname => :familyname,
        :Familienname_Variante => :familyname_variant,
        :Vorname_Variante => :givenname_variant,
    ]

    df_nl = Wds.create_name_lookup(select(df_p, columns))

    return Wds.filltable!(table, df_nl)

end
